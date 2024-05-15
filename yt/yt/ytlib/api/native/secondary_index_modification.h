#pragma once

#include <yt/yt/library/query/base/query_preparer.h>

#include <yt/yt/client/api/dynamic_table_transaction.h>

#include <library/cpp/yt/logging/logger.h>

namespace NYT::NApi::NNative {

////////////////////////////////////////////////////////////////////////////////

class TSecondaryIndexModifier
{
public:
    TSecondaryIndexModifier(
        NTableClient::TTableSchemaPtr tableSchema,
        NTableClient::TNameTablePtr nameTable,
        TSharedRange<TRowModification> modifications,
        const NTabletClient::TTableMountInfoPtr& tableMountInfo,
        std::vector<NTabletClient::TTableMountInfoPtr> indexMountInfos,
        const NQueryClient::IExpressionEvaluatorCachePtr& expressionEvaluatorCache,
        NLogging::TLogger logger);

    std::vector<NTableClient::TUnversionedRow> GetLookupKeys();

    const std::vector<int>& GetPositionToTableIdMapping() const;

    void SetInitialAndResultingRows(TSharedRange<NTableClient::TUnversionedRow> lookedUpRows);

    TSharedRange<TRowModification> ProduceModificationsForIndex(int index);

private:
    using TInitialRowMap = THashMap<NTableClient::TKey, NTableClient::TUnversionedRow>;
    using TResultingRowMap = THashMap<NTableClient::TKey, NTableClient::TMutableUnversionedRow>;

    struct TIndexDescriptor
    {
        NTabletClient::ESecondaryIndexKind Kind;
        std::optional<int> UnfoldedColumnPosition;
        std::unique_ptr<NQueryClient::TParsedSource> Predicate;
    };

    const NTableClient::TTableSchemaPtr TableSchema_;
    const TSharedRange<TRowModification> Modifications_;
    const NQueryClient::IExpressionEvaluatorCachePtr ExpressionEvaluatorCache_;
    const NTableClient::TRowBufferPtr RowBuffer_;

    const NLogging::TLogger Logger;

    NTableClient::TNameTablePtr NameTable_;
    std::vector<NTabletClient::TTableMountInfoPtr> IndexInfos_;
    std::vector<TIndexDescriptor> IndexDescriptors_;
    std::vector<int> UnfoldedColumnIndices_;

    NTableClient::TNameTableToSchemaIdMapping ResultingRowMapping_;
    std::vector<int> PositionToIdMapping_;
    NTableClient::TTableSchemaPtr ResultingSchema_;

    TInitialRowMap InitialRowMap_;
    TResultingRowMap ResultingRowMap_;

    TSharedRange<TRowModification> ProduceFullSyncModifications(
        const NTableClient::TNameTableToSchemaIdMapping& indexIdMapping,
        const NTableClient::TNameTableToSchemaIdMapping& keyIndexIdMapping,
        const NTableClient::TTableSchema& indexSchema,
        std::function<bool(NTableClient::TUnversionedRow)> predicate,
        const std::optional<NTableClient::TUnversionedValue>& empty);

    TSharedRange<TRowModification> ProduceUnfoldingModifications(
        const NTableClient::TNameTableToSchemaIdMapping& indexIdMapping,
        const NTableClient::TNameTableToSchemaIdMapping& keyIndexIdMapping,
        const NTableClient::TTableSchema& indexSchema,
        std::function<bool(NTableClient::TUnversionedRow)> predicate,
        const std::optional<NTableClient::TUnversionedValue>& empty,
        int unfoldedKeyPosition);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NNative
