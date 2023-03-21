
#include "cg_fragment_compiler.h"
#include "folding_profiler.h"
#include "functions_cg.h"

#include <yt/yt/library/query/base/query_preparer.h>
#include <yt/yt/library/query/base/functions.h>
#include <yt/yt/library/query/base/private.h>

#include <yt/yt/library/query/engine_api/column_evaluator.h>
#include <yt/yt/library/query/engine_api/config.h>

#include <yt/yt/client/query_client/query_statistics.h>

#include <yt/yt/core/misc/sync_cache.h>

namespace NYT::NQueryClient {

using namespace NTableClient;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = QueryClientLogger;

////////////////////////////////////////////////////////////////////////////////

TColumnEvaluator::TColumnEvaluator(
    std::vector<TColumn> columns,
    std::vector<bool> isAggregate)
    : Columns_(std::move(columns))
    , IsAggregate_(std::move(isAggregate))
{ }

TColumnEvaluatorPtr TColumnEvaluator::Create(
    const TTableSchemaPtr& schema,
    const TConstTypeInferrerMapPtr& typeInferrers,
    const TConstFunctionProfilerMapPtr& profilers)
{
    std::vector<TColumn> columns(schema->GetColumnCount());
    std::vector<bool> isAggregate(schema->GetColumnCount());

    for (int index = 0; index < schema->GetColumnCount(); ++index) {
        auto& column = columns[index];
        if (schema->Columns()[index].Expression()) {
            THashSet<TString> references;

            column.Expression = PrepareExpression(
                *schema->Columns()[index].Expression(),
                *schema,
                typeInferrers,
                &references);

            column.Evaluator = Profile(
                column.Expression,
                schema,
                nullptr,
                &column.Variables,
                profilers)();

            for (const auto& reference : references) {
                column.ReferenceIds.push_back(schema->GetColumnIndexOrThrow(reference));
            }
            std::sort(column.ReferenceIds.begin(), column.ReferenceIds.end());
        }

        if (schema->Columns()[index].Aggregate()) {
            const auto& aggregateName = *schema->Columns()[index].Aggregate();
            auto type = schema->Columns()[index].GetWireType();
            column.Aggregate = CodegenAggregate(
                BuiltinAggregateProfilers->GetAggregate(aggregateName)->Profile(type, type, type, aggregateName),
                type, type);
            isAggregate[index] = true;
        }
    }

    return New<TColumnEvaluator>(std::move(columns), std::move(isAggregate));
}

void TColumnEvaluator::EvaluateKey(TMutableRow fullRow, const TRowBufferPtr& buffer, int index) const
{
    YT_VERIFY(index < static_cast<int>(fullRow.GetCount()));
    YT_VERIFY(index < std::ssize(Columns_));

    const auto& column = Columns_[index];
    const auto& evaluator = column.Evaluator;
    YT_VERIFY(evaluator);

    // Zero row to avoid garbage after evaluator.
    fullRow[index] = MakeUnversionedSentinelValue(EValueType::Null);

    evaluator(
        column.Variables.GetLiteralValues(),
        column.Variables.GetOpaqueData(),
        &fullRow[index],
        fullRow.Begin(),
        buffer.Get());

    fullRow[index].Id = index;
}

void TColumnEvaluator::EvaluateKeys(TMutableRow fullRow, const TRowBufferPtr& buffer) const
{
    for (int index = 0; index < std::ssize(Columns_); ++index) {
        if (Columns_[index].Evaluator) {
            EvaluateKey(fullRow, buffer, index);
        }
    }
}

void TColumnEvaluator::EvaluateKeys(
    TMutableVersionedRow fullRow,
    const TRowBufferPtr& buffer) const
{
    auto row = buffer->CaptureRow(MakeRange(fullRow.BeginKeys(), fullRow.GetKeyCount()), false);
    EvaluateKeys(row, buffer);

    for (int index = 0; index < fullRow.GetKeyCount(); ++index) {
        if (Columns_[index].Evaluator) {
            fullRow.BeginKeys()[index] = row[index];
        }
    }
}

const std::vector<int>& TColumnEvaluator::GetReferenceIds(int index) const
{
    return Columns_[index].ReferenceIds;
}

TConstExpressionPtr TColumnEvaluator::GetExpression(int index) const
{
    return Columns_[index].Expression;
}

void TColumnEvaluator::InitAggregate(
    int index,
    TUnversionedValue* state,
    const TRowBufferPtr& buffer) const
{
    Columns_[index].Aggregate.Init(buffer.Get(), state);
    state->Id = index;
}

void TColumnEvaluator::UpdateAggregate(
    int index,
    TUnversionedValue* state,
    const TUnversionedValue& update,
    const TRowBufferPtr& buffer) const
{
    Columns_[index].Aggregate.Update(buffer.Get(), state, &update);
    state->Id = index;
}

void TColumnEvaluator::MergeAggregate(
    int index,
    TUnversionedValue* state,
    const TUnversionedValue& mergeeState,
    const TRowBufferPtr& buffer) const
{
    Columns_[index].Aggregate.Merge(buffer.Get(), state, &mergeeState);
    state->Id = index;
}

void TColumnEvaluator::FinalizeAggregate(
    int index,
    TUnversionedValue* result,
    const TUnversionedValue& state,
    const TRowBufferPtr& buffer) const
{
    Columns_[index].Aggregate.Finalize(buffer.Get(), result, &state);
    result->Id = index;
}

////////////////////////////////////////////////////////////////////////////////

class TCachedColumnEvaluator
    : public TSyncCacheValueBase<llvm::FoldingSetNodeID, TCachedColumnEvaluator>
{
public:
    TCachedColumnEvaluator(
        const llvm::FoldingSetNodeID& id,
        TColumnEvaluatorPtr evaluator)
        : TSyncCacheValueBase(id)
        , Evaluator_(std::move(evaluator))
    { }

    const TColumnEvaluatorPtr& GetColumnEvaluator()
    {
        return Evaluator_;
    }

private:
    const TColumnEvaluatorPtr Evaluator_;
};

// TODO(lukyan): Use async cache?
class TColumnEvaluatorCache
    : public TSyncSlruCacheBase<llvm::FoldingSetNodeID, TCachedColumnEvaluator>
    , public IColumnEvaluatorCache
{
public:
    TColumnEvaluatorCache(
        TColumnEvaluatorCacheConfigPtr config,
        const TConstTypeInferrerMapPtr& typeInferrers,
        const TConstFunctionProfilerMapPtr& profilers)
        : TSyncSlruCacheBase(config->CGCache)
        , TypeInferers_(typeInferrers)
        , Profilers_(profilers)
    { }

    TColumnEvaluatorPtr Find(const TTableSchemaPtr& schema) override
    {
        llvm::FoldingSetNodeID id;
        Profile(schema, &id);

        auto cachedEvaluator = TSyncSlruCacheBase::Find(id);
        if (!cachedEvaluator) {
            YT_LOG_DEBUG("Codegen cache miss: generating column evaluator (Schema: %v)",
                *schema);

            auto evaluator = TColumnEvaluator::Create(
                schema,
                TypeInferers_,
                Profilers_);
            cachedEvaluator = New<TCachedColumnEvaluator>(id, evaluator);

            TryInsert(cachedEvaluator, &cachedEvaluator);
        }

        return cachedEvaluator->GetColumnEvaluator();
    }

    void Configure(const TColumnEvaluatorCacheDynamicConfigPtr& config) override
    {
        TSyncSlruCacheBase::Reconfigure(config->CGCache);
    }

private:
    const TConstTypeInferrerMapPtr TypeInferers_;
    const TConstFunctionProfilerMapPtr Profilers_;
};

IColumnEvaluatorCachePtr CreateColumnEvaluatorCache(
    TColumnEvaluatorCacheConfigPtr config,
    TConstTypeInferrerMapPtr typeInferrers,
    TConstFunctionProfilerMapPtr profilers)
{
    return New<TColumnEvaluatorCache>(
        std::move(config),
        std::move(typeInferrers),
        std::move(profilers));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient
