#include "ql_helpers.h"
#include "udf/short_invalid_ir.h"
#include "udf/long_invalid_ir.h"

#include <library/cpp/resource/resource.h>

#include <yt/yt/library/query/base/callbacks.h>
#include <yt/yt/library/query/base/query.h>
#include <yt/yt/library/query/base/query_preparer.h>
#include <yt/yt/library/query/base/functions.h>

#include <yt/yt/library/query/engine_api/column_evaluator.h>
#include <yt/yt/library/query/engine_api/config.h>
#include <yt/yt/library/query/engine_api/coordinator.h>
#include <yt/yt/library/query/engine_api/evaluator.h>

#include <yt/yt/library/query/engine/folding_profiler.h>
#include <yt/yt/library/query/engine/functions_cg.h>

#include <yt/yt/library/query/proto/query.pb.h>

#include <yt/yt/client/table_client/name_table.h>
#include <yt/yt/client/table_client/schema.h>
#include <yt/yt/client/table_client/unversioned_reader.h>
#include <yt/yt/client/table_client/unversioned_writer.h>
#include <yt/yt/client/table_client/row_batch.h>
#include <yt/yt/client/table_client/pipe.h>

#include <yt/yt/core/concurrency/action_queue.h>

#include <yt/yt/core/misc/collection_helpers.h>
#include <yt/yt/core/misc/range_formatters.h>

#include <yt/yt/core/ytree/convert.h>

#include <util/system/sanitizers.h>

#include <util/string/split.h>
#include <util/string/strip.h>
#include <util/string/subst.h>

#include <tuple>

// Tests:
// TQueryPrepareTest
// TJobQueryPrepareTest
// TQueryCoordinateTest
// TQueryEvaluateTest

namespace NYT::NQueryClient {
namespace {

////////////////////////////////////////////////////////////////////////////////

using namespace NApi;
using namespace NConcurrency;
using namespace NYPath;
using namespace NYTree;
using namespace NYson;

using NChunkClient::NProto::TDataStatistics;

void SetObjectId(TDataSplit* /*dataSplit*/, NObjectClient::TObjectId /*objectId*/)
{ }

////////////////////////////////////////////////////////////////////////////////

class TQueryPrepareTest
    : public ::testing::Test
{
protected:
    void SetUp() override
    {
        ActionQueue_ = New<TActionQueue>("PrepareTest");
    }

    void TearDown() override
    {
        ActionQueue_->Shutdown();
    }

    template <class TMatcher>
    void ExpectPrepareThrowsWithDiagnostics(
        const TString& query,
        TMatcher matcher,
        NYson::TYsonStringBuf placeholderValues = {})
    {
        EXPECT_THROW_THAT(
            BIND([&] () {
                PreparePlanFragment(&PrepareMock_, query, DefaultFetchFunctions, placeholderValues);
            })
            .AsyncVia(ActionQueue_->GetInvoker())
            .Run()
            .Get()
            .ThrowOnError(),
            matcher);
    }

    StrictMock<TPrepareCallbacksMock> PrepareMock_;
    TActionQueuePtr ActionQueue_;

};

TEST_F(TQueryPrepareTest, BadSyntax)
{
    ExpectPrepareThrowsWithDiagnostics(
        "bazzinga mu ha ha ha",
        HasSubstr("syntax error"));
}

TEST_F(TQueryPrepareTest, BadWhere)
{
    EXPECT_CALL(PrepareMock_, GetInitialSplit("//t"))
        .WillOnce(Return(MakeFuture(MakeSimpleSplit("//t"))));

    ExpectPrepareThrowsWithDiagnostics(
        "* from [//t] where (a = 1, b = 2)",
        HasSubstr("Expecting scalar expression"));
}

TEST_F(TQueryPrepareTest, BadTableName)
{
    EXPECT_CALL(PrepareMock_, GetInitialSplit("//bad/table"))
        .WillOnce(Invoke(&RaiseTableNotFound));

    ExpectPrepareThrowsWithDiagnostics(
        "a, b from [//bad/table]",
        HasSubstr("Could not find table //bad/table"));
}

TEST_F(TQueryPrepareTest, BadColumnNameInProject)
{
    EXPECT_CALL(PrepareMock_, GetInitialSplit("//t"))
        .WillOnce(Return(MakeFuture(MakeSimpleSplit("//t"))));

    ExpectPrepareThrowsWithDiagnostics(
        "foo from [//t]",
        HasSubstr("Undefined reference \"foo\""));
}

TEST_F(TQueryPrepareTest, BadColumnNameInFilter)
{
    EXPECT_CALL(PrepareMock_, GetInitialSplit("//t"))
        .WillOnce(Return(MakeFuture(MakeSimpleSplit("//t"))));

    ExpectPrepareThrowsWithDiagnostics(
        "k from [//t] where bar = 1",
        HasSubstr("Undefined reference \"bar\""));
}

TEST_F(TQueryPrepareTest, BadTypecheck)
{
    EXPECT_CALL(PrepareMock_, GetInitialSplit("//t"))
        .WillOnce(Return(MakeFuture(MakeSimpleSplit("//t"))));

    ExpectPrepareThrowsWithDiagnostics(
        "k from [//t] where a > \"xyz\"",
        ContainsRegex("Type mismatch in expression"));
}

#if !defined(_asan_enabled_) && !defined(_msan_enabled_)

TEST_F(TQueryPrepareTest, TooBigQuery)
{
    TString query = "k from [//t] where a ";
    for (int i = 0; i < 50; ++i) {
        query += "+ " + ToString(i);
    }
    query += " > 0";

    EXPECT_CALL(PrepareMock_, GetInitialSplit("//t"))
        .WillOnce(Return(MakeFuture(MakeSimpleSplit("//t"))));

    ExpectPrepareThrowsWithDiagnostics(
        query,
        ContainsRegex("Maximum expression depth exceeded"));
}

TEST_F(TQueryPrepareTest, TooBigQuery2)
{
    TString query =
    R"(
        * from [//t]
        where
        (a = 3735 and s = 'd0b160b8-1d27-40ad-8cad-1b69c2187195') or
        (a = 4193 and s = 'd2c05a2c-fdee-4417-b2dd-cf58a51a6db2') or
        (a = 4365 and s = '07d1c433-3a42-473a-9d8f-45f21001dbe5') or
        (a = 4363 and s = 'f0168ae8-8f75-4dee-b113-510c43f2df30') or
        (a = 4346 and s = '4f1cf6d9-999b-4d9e-95c0-55b45ec96573') or
        (a = 4334 and s = '9db5716a-89ef-4a0b-bfe3-f0785f31f5f9') or
        (a = 4340 and s = '8b2b0701-8bd2-44ca-b90b-a9186acadcaf') or
        (a = 4329 and s = 'f0dbede6-ca7f-4e31-9e5e-2de9eb0dd672') or
        (a = 4211 and s = '4b10bebc-8633-4a2c-bb23-05abac483c54') or
        (a = 4351 and s = '64517493-57d5-4f1b-9fdb-393105b3fce7') or
        (a = 4321 and s = '4054e543-6119-4146-8922-3c026109eada') or
        (a = 4204 and s = '9d024b07-85c0-4939-bcea-22948812835f') or
        (a = 4315 and s = '9977f359-c8a8-499e-9af9-15326a90fcc8') or
        (a = 3954 and s = 'c08cb9ef-7797-4ab9-aa53-25fc43f1a7f6') or
        (a = 4338 and s = '06e00ee8-ec64-4aee-9ce2-986559edcc08') or
        (a = 4214 and s = 'b465b4ab-e4e7-46c1-aaac-22817ca6df82') or
        (a = 4344 and s = '6850e940-8afb-4b7e-b428-92996e0d7cc3') or
        (a = 4240 and s = 'd18fe358-6fb4-473e-bf26-496c860cfde1') or
        (a = 4337 and s = 'ddd125eb-2c07-4a8b-b8d2-295356df2253') or
        (a = 4348 and s = '8d791558-ec95-4b2d-9c17-f72e530c30ec') or
        (a = 4339 and s = '73ec1c2a-9258-4ffc-b875-c0f7ac58e1ba') or
        (a = 4343 and s = '79e4a031-958c-4905-9b8d-dd6acd18b253') or
        (a = 4328 and s = '8e785bb6-ca13-47fc-b2d7-de6fa86e842f') or
        (a = 4331 and s = '2665d0e0-49b2-416f-9220-c6c67ccec868') or
        (a = 4239 and s = 'b18a4d21-4c4a-43bc-a7d5-15007c43fd18') or
        (a = 4238 and s = '04720557-55ca-4e0c-be87-332eb43e6274') or
        (a = 4255 and s = 'c731fc35-b7b7-44ff-9075-3c2c2ec500ce') or
        (a = 4327 and s = 'ad9a16a0-129f-48db-9105-06919a24eb3f') or
        (a = 4335 and s = '94ee4985-c05d-4772-991c-781e1602ba0e') or
        (a = 4342 and s = '38db2da1-ea4d-40ba-85a3-beac8f7fc055') or
        (a = 4191 and s = 'a5f3264f-73fe-43bc-972d-97c51710f395') or
        (a = 4341 and s = '32f885b2-63dd-40b2-b8b5-9d2ed7221235') or
        (a = 4347 and s = '8ac67959-3e04-420f-bda5-a4bf5b45ea03') or
        (a = 4353 and s = 'ec994ea3-5cf8-4951-a409-8a04ad1cffaf') or
        (a = 4354 and s = '644f26c6-f4ee-4cac-8885-807be11941c3') or
        (a = 4212 and s = '322c92c1-4d3d-463d-8001-97557e9e93f9') or
        (a = 4325 and s = '932c592b-6ec4-4ec8-a4bd-79c4900996ab') or

        (a = 3735 and s = 'd0b160b8-1d27-40ad-8cad-1b69c2187195') or
        (a = 4193 and s = 'd2c05a2c-fdee-4417-b2dd-cf58a51a6db2') or
        (a = 4365 and s = '07d1c433-3a42-473a-9d8f-45f21001dbe5') or
        (a = 4363 and s = 'f0168ae8-8f75-4dee-b113-510c43f2df30') or
        (a = 4346 and s = '4f1cf6d9-999b-4d9e-95c0-55b45ec96573') or
        (a = 4334 and s = '9db5716a-89ef-4a0b-bfe3-f0785f31f5f9') or
        (a = 4340 and s = '8b2b0701-8bd2-44ca-b90b-a9186acadcaf') or
        (a = 4329 and s = 'f0dbede6-ca7f-4e31-9e5e-2de9eb0dd672') or
        (a = 4211 and s = '4b10bebc-8633-4a2c-bb23-05abac483c54') or
        (a = 4351 and s = '64517493-57d5-4f1b-9fdb-393105b3fce7') or
        (a = 4321 and s = '4054e543-6119-4146-8922-3c026109eada') or
        (a = 4204 and s = '9d024b07-85c0-4939-bcea-22948812835f') or
        (a = 4315 and s = '9977f359-c8a8-499e-9af9-15326a90fcc8') or
        (a = 3954 and s = 'c08cb9ef-7797-4ab9-aa53-25fc43f1a7f6') or
        (a = 4338 and s = '06e00ee8-ec64-4aee-9ce2-986559edcc08') or
        (a = 4214 and s = 'b465b4ab-e4e7-46c1-aaac-22817ca6df82') or
        (a = 4344 and s = '6850e940-8afb-4b7e-b428-92996e0d7cc3') or
        (a = 4240 and s = 'd18fe358-6fb4-473e-bf26-496c860cfde1') or
        (a = 4337 and s = 'ddd125eb-2c07-4a8b-b8d2-295356df2253') or
        (a = 4348 and s = '8d791558-ec95-4b2d-9c17-f72e530c30ec') or
        (a = 4339 and s = '73ec1c2a-9258-4ffc-b875-c0f7ac58e1ba') or
        (a = 4343 and s = '79e4a031-958c-4905-9b8d-dd6acd18b253') or
        (a = 4328 and s = '8e785bb6-ca13-47fc-b2d7-de6fa86e842f') or
        (a = 4331 and s = '2665d0e0-49b2-416f-9220-c6c67ccec868') or
        (a = 4239 and s = 'b18a4d21-4c4a-43bc-a7d5-15007c43fd18') or
        (a = 4238 and s = '04720557-55ca-4e0c-be87-332eb43e6274') or
        (a = 4255 and s = 'c731fc35-b7b7-44ff-9075-3c2c2ec500ce') or
        (a = 4327 and s = 'ad9a16a0-129f-48db-9105-06919a24eb3f') or
        (a = 4335 and s = '94ee4985-c05d-4772-991c-781e1602ba0e') or
        (a = 4342 and s = '38db2da1-ea4d-40ba-85a3-beac8f7fc055') or
        (a = 4191 and s = 'a5f3264f-73fe-43bc-972d-97c51710f395') or
        (a = 4341 and s = '32f885b2-63dd-40b2-b8b5-9d2ed7221235') or
        (a = 4347 and s = '8ac67959-3e04-420f-bda5-a4bf5b45ea03') or
        (a = 4353 and s = 'ec994ea3-5cf8-4951-a409-8a04ad1cffaf') or
        (a = 4354 and s = '644f26c6-f4ee-4cac-8885-807be11941c3') or
        (a = 4212 and s = '322c92c1-4d3d-463d-8001-97557e9e93f9') or
        (a = 4325 and s = '932c592b-6ec4-4ec8-a4bd-79c4900996ab')
    )";

    EXPECT_CALL(PrepareMock_, GetInitialSplit("//t"))
        .WillOnce(Return(MakeFuture(MakeSimpleSplit("//t"))));

    ExpectPrepareThrowsWithDiagnostics(
        query,
        ContainsRegex("Maximum expression depth exceeded"));
}

#endif // !defined(_asan_enabled_) && !defined(_msan_enabled_)

TEST_F(TQueryPrepareTest, BigQuery)
{
    TString query = "k from [//t] where a in (0";
    for (int i = 1; i < 1000; ++i) {
        query += ", " + ToString(i);
    }
    query += ")";

    EXPECT_CALL(PrepareMock_, GetInitialSplit("//t"))
        .WillOnce(Return(MakeFuture(MakeSimpleSplit("//t"))));

    PreparePlanFragment(&PrepareMock_, query);
}

TEST_F(TQueryPrepareTest, ResultSchemaCollision)
{
    ExpectPrepareThrowsWithDiagnostics(
        "a as x, b as x FROM [//t] WHERE k > 3",
        ContainsRegex("Alias \"x\" has been already used"));
}

TEST_F(TQueryPrepareTest, MisuseAggregateFunction)
{
    EXPECT_CALL(PrepareMock_, GetInitialSplit("//t"))
        .WillOnce(Return(MakeFuture(MakeSimpleSplit("//t"))));

    ExpectPrepareThrowsWithDiagnostics(
        "sum(sum(a)) from [//t] group by k",
        ContainsRegex("Misuse of aggregate .*"));

    EXPECT_CALL(PrepareMock_, GetInitialSplit("//t"))
        .WillOnce(Return(MakeFuture(MakeSimpleSplit("//t"))));

    ExpectPrepareThrowsWithDiagnostics(
        "sum(a) from [//t]",
        ContainsRegex("Misuse of aggregate .*"));
}

TEST_F(TQueryPrepareTest, FailedTypeInference)
{
    EXPECT_CALL(PrepareMock_, GetInitialSplit("//t"))
        .WillOnce(Return(MakeFuture(MakeSimpleSplit("//t"))));

    ExpectPrepareThrowsWithDiagnostics(
        "null from [//t]",
        ContainsRegex("Type inference failed"));
}

TEST_F(TQueryPrepareTest, JoinColumnCollision)
{
    EXPECT_CALL(PrepareMock_, GetInitialSplit("//t"))
        .WillOnce(Return(MakeFuture(MakeSimpleSplit("//t"))));

    EXPECT_CALL(PrepareMock_, GetInitialSplit("//s"))
        .WillOnce(Return(MakeFuture(MakeSimpleSplit("//s"))));

    ExpectPrepareThrowsWithDiagnostics(
        "a, b from [//t] join [//s] using b",
        ContainsRegex("Ambiguous resolution"));

    EXPECT_CALL(PrepareMock_, GetInitialSplit("//t"))
        .WillOnce(Return(MakeFuture(MakeSimpleSplit("//t"))));

    EXPECT_CALL(PrepareMock_, GetInitialSplit("//s"))
        .WillOnce(Return(MakeFuture(MakeSimpleSplit("//s"))));

    ExpectPrepareThrowsWithDiagnostics(
        "* from [//t] join [//s] using b",
        ContainsRegex("Ambiguous resolution"));
}

TEST_F(TQueryPrepareTest, SelectColumns)
{
    {
        TDataSplit dataSplit;

        SetObjectId(&dataSplit, MakeId(EObjectType::Table, 0x42, 0, 0xdeadbabe));

        auto schema = New<TTableSchema>(std::vector{
            TColumnSchema("h", EValueType::Int64)
                .SetSortOrder(ESortOrder::Ascending)
                .SetExpression(TString("a")),
            TColumnSchema("a", EValueType::Int64)
                .SetSortOrder(ESortOrder::Ascending),
            TColumnSchema("b", EValueType::Int64)
                .SetSortOrder(ESortOrder::Ascending),
            TColumnSchema("c", EValueType::Int64),
            TColumnSchema("d", EValueType::Int64)
        });

        dataSplit.TableSchema = schema;

        EXPECT_CALL(PrepareMock_, GetInitialSplit("//t"))
            .WillRepeatedly(Return(MakeFuture(dataSplit)));
    }


    {
        TString queryString = "* from [//t]";

        auto query = PreparePlanFragment(&PrepareMock_, queryString)->Query;

        auto schema = query->GetReadSchema();

        EXPECT_EQ(schema->GetColumnCount(), 5);
        EXPECT_EQ(schema->Columns()[0].Name(), "h");
        EXPECT_EQ(schema->Columns()[1].Name(), "a");
        EXPECT_EQ(schema->Columns()[2].Name(), "b");
        EXPECT_EQ(schema->Columns()[3].Name(), "c");
        EXPECT_EQ(schema->Columns()[4].Name(), "d");
    }

    {
        TString queryString = "d, c, a from [//t]";

        auto query = PreparePlanFragment(&PrepareMock_, queryString)->Query;

        auto schema = query->GetReadSchema();

        EXPECT_EQ(schema->GetColumnCount(), 3);
        EXPECT_EQ(schema->Columns()[0].Name(), "a");
        EXPECT_EQ(schema->Columns()[1].Name(), "c");
        EXPECT_EQ(schema->Columns()[2].Name(), "d");
    }
}

TEST_F(TQueryPrepareTest, SortMergeJoin)
{
    {
        TDataSplit dataSplit;

        SetObjectId(&dataSplit, MakeId(EObjectType::Table, 0x42, 0, 0xdeadbabe));

        auto tableSchema = New<TTableSchema>(std::vector{
            TColumnSchema("hash", EValueType::Int64)
                .SetSortOrder(ESortOrder::Ascending)
                .SetExpression(TString("int64(farm_hash(cid))")),
            TColumnSchema("cid", EValueType::Int64)
                .SetSortOrder(ESortOrder::Ascending),
            TColumnSchema("pid", EValueType::Int64)
                .SetSortOrder(ESortOrder::Ascending),
            TColumnSchema("id", EValueType::Int64),
            TColumnSchema("__shard__", EValueType::Int64),
            TColumnSchema("PhraseID", EValueType::Int64),
            TColumnSchema("price", EValueType::Int64),
        });

        dataSplit.TableSchema = tableSchema;

        EXPECT_CALL(PrepareMock_, GetInitialSplit("//bids"))
            .WillRepeatedly(Return(MakeFuture(dataSplit)));
    }

    {
        TDataSplit dataSplit;

        SetObjectId(&dataSplit, MakeId(EObjectType::Table, 0x42, 0, 0xdeadbabe));

        auto schema = New<TTableSchema>(std::vector{
            TColumnSchema("ExportIDHash", EValueType::Int64)
                .SetSortOrder(ESortOrder::Ascending)
                .SetExpression(TString("int64(farm_hash(ExportID))")),
            TColumnSchema("ExportID", EValueType::Int64)
                .SetSortOrder(ESortOrder::Ascending),
            TColumnSchema("GroupExportID", EValueType::Int64)
                .SetSortOrder(ESortOrder::Ascending),
            TColumnSchema("PhraseID", EValueType::Uint64)
                .SetSortOrder(ESortOrder::Ascending),
            TColumnSchema("UpdateTime", EValueType::Int64)
                .SetSortOrder(ESortOrder::Ascending),
            TColumnSchema("Shows", EValueType::Int64),
            TColumnSchema("Clicks", EValueType::Int64),
        });

        dataSplit.TableSchema = schema;

        EXPECT_CALL(PrepareMock_, GetInitialSplit("//DirectPhraseStat"))
            .WillRepeatedly(Return(MakeFuture(dataSplit)));
    }

    {
        TDataSplit dataSplit;

        SetObjectId(&dataSplit, MakeId(EObjectType::Table, 0x42, 0, 0xdeadbabe));

        auto schema = New<TTableSchema>(std::vector{
            TColumnSchema("hash", EValueType::Int64)
                .SetSortOrder(ESortOrder::Ascending)
                .SetExpression(TString("int64(farm_hash(pid))")),
            TColumnSchema("pid", EValueType::Int64)
                .SetSortOrder(ESortOrder::Ascending),
            TColumnSchema("__shard__", EValueType::Int64)
                .SetSortOrder(ESortOrder::Ascending),
            TColumnSchema("status", EValueType::Int64),
        });

        dataSplit.TableSchema = schema;

        EXPECT_CALL(PrepareMock_, GetInitialSplit("//phrases"))
            .WillRepeatedly(Return(MakeFuture(dataSplit)));
    }

    {
        TDataSplit dataSplit;

        SetObjectId(&dataSplit, MakeId(EObjectType::Table, 0x42, 0, 0xdeadbabe));

        auto schema = New<TTableSchema>(std::vector{
            TColumnSchema("hash", EValueType::Int64)
                .SetSortOrder(ESortOrder::Ascending)
                .SetExpression(TString("int64(farm_hash(cid))")),
            TColumnSchema("cid", EValueType::Int64)
                .SetSortOrder(ESortOrder::Ascending),
            TColumnSchema("__shard__", EValueType::Int64)
                .SetSortOrder(ESortOrder::Ascending),
            TColumnSchema("value", EValueType::Int64),
        });

        dataSplit.TableSchema = schema;

        EXPECT_CALL(PrepareMock_, GetInitialSplit("//campaigns"))
            .WillRepeatedly(Return(MakeFuture(dataSplit)));
    }

    {
        TString queryString = "* from [//bids] D\n"
            "left join [//campaigns] C on D.cid = C.cid\n"
            "left join [//DirectPhraseStat] S on (D.cid, D.pid, uint64(D.PhraseID)) = (S.ExportID, S.GroupExportID, S.PhraseID)\n"
            "left join [//phrases] P on (D.pid,D.__shard__) = (P.pid,P.__shard__)";

        auto query = PreparePlanFragment(&PrepareMock_, queryString)->Query;

        EXPECT_EQ(query->JoinClauses.size(), 3u);
        const auto& joinClauses = query->JoinClauses;

        EXPECT_EQ(joinClauses[0]->ForeignKeyPrefix, 2u);
        EXPECT_EQ(joinClauses[0]->CommonKeyPrefix, 2u);

        EXPECT_EQ(joinClauses[1]->ForeignKeyPrefix, 4u);
        EXPECT_EQ(joinClauses[1]->CommonKeyPrefix, 2u);

        EXPECT_EQ(joinClauses[2]->ForeignKeyPrefix, 3u);
        EXPECT_EQ(joinClauses[2]->CommonKeyPrefix, 0u);
    }

    {
        TString queryString = "* from [//bids] D\n"
            "left join [//campaigns] C on (D.cid,D.__shard__) = (C.cid,C.__shard__)\n"
            "left join [//DirectPhraseStat] S on (D.cid, D.pid, uint64(D.PhraseID)) = (S.ExportID, S.GroupExportID, S.PhraseID)\n"
            "left join [//phrases] P on (D.pid,D.__shard__) = (P.pid,P.__shard__)";

        auto query = PreparePlanFragment(&PrepareMock_, queryString)->Query;

        EXPECT_EQ(query->JoinClauses.size(), 3u);
        const auto& joinClauses = query->JoinClauses;

        EXPECT_EQ(joinClauses[0]->ForeignKeyPrefix, 3u);
        EXPECT_EQ(joinClauses[0]->CommonKeyPrefix, 2u);

        EXPECT_EQ(joinClauses[1]->ForeignKeyPrefix, 4u);
        EXPECT_EQ(joinClauses[1]->CommonKeyPrefix, 2u);

        EXPECT_EQ(joinClauses[2]->ForeignKeyPrefix, 3u);
        EXPECT_EQ(joinClauses[2]->CommonKeyPrefix, 0u);
    }

    {
        TString queryString = "* from [//bids] D\n"
            "left join [//DirectPhraseStat] S on (D.cid, D.pid, uint64(D.PhraseID)) = (S.ExportID, S.GroupExportID, S.PhraseID)\n"
            "left join [//campaigns] C on (D.cid,D.__shard__) = (C.cid,C.__shard__)\n"
            "left join [//phrases] P on (D.pid,D.__shard__) = (P.pid,P.__shard__)";

        auto query = PreparePlanFragment(&PrepareMock_, queryString)->Query;

        EXPECT_EQ(query->JoinClauses.size(), 3u);
        const auto& joinClauses = query->JoinClauses;

        EXPECT_EQ(joinClauses[0]->ForeignKeyPrefix, 4u);
        EXPECT_EQ(joinClauses[0]->CommonKeyPrefix, 3u);

        EXPECT_EQ(joinClauses[1]->ForeignKeyPrefix, 3u);
        EXPECT_EQ(joinClauses[1]->CommonKeyPrefix, 2u);

        EXPECT_EQ(joinClauses[2]->ForeignKeyPrefix, 3u);
        EXPECT_EQ(joinClauses[2]->CommonKeyPrefix, 0u);
    }
}

TEST_F(TQueryPrepareTest, SplitWherePredicateWithJoin)
{
    {
        TDataSplit dataSplit;

        SetObjectId(&dataSplit, MakeId(EObjectType::Table, 0x42, 0, 0xdeadbabe));

        auto schema = New<TTableSchema>(std::vector{
            TColumnSchema("kind", EValueType::String, ESortOrder::Ascending).SetRequired(true),
            TColumnSchema("type", EValueType::String).SetRequired(false),
            TColumnSchema("ride_date", EValueType::String).SetRequired(true),
            TColumnSchema("ride_time", EValueType::String).SetRequired(true),
            TColumnSchema("log_time", EValueType::String).SetRequired(true),
            TColumnSchema("rover", EValueType::String).SetRequired(true),
            TColumnSchema("timestamp", EValueType::Int64).SetRequired(true),
            TColumnSchema("_key", EValueType::String).SetRequired(false),
            TColumnSchema("attributes", EValueType::Any).SetRequired(false),
            TColumnSchema("comment", EValueType::String).SetRequired(false),
            TColumnSchema("created_at", EValueType::Uint64).SetRequired(false),
            TColumnSchema("duration", EValueType::Int64).SetRequired(false),
            TColumnSchema("geo", EValueType::Any).SetRequired(false),
            TColumnSchema("ignore", EValueType::Boolean).SetRequired(false),
            TColumnSchema("investigation_status", EValueType::String).SetRequired(false),
            TColumnSchema("place", EValueType::Any).SetRequired(false),
            TColumnSchema("severity", EValueType::String).SetRequired(false),
            TColumnSchema("status", EValueType::String).SetRequired(false),
            TColumnSchema("tags", EValueType::Any).SetRequired(false),
            TColumnSchema("tickets", EValueType::Any).SetRequired(false),
            TColumnSchema("updated_at", EValueType::Uint64).SetRequired(false)
        });

        dataSplit.TableSchema = schema;

        EXPECT_CALL(PrepareMock_, GetInitialSplit("//a"))
            .WillRepeatedly(Return(MakeFuture(dataSplit)));
    }

    {
        TDataSplit dataSplit;

        SetObjectId(&dataSplit, MakeId(EObjectType::Table, 0x42, 0, 0xdeadbabe));

        auto schema = New<TTableSchema>(std::vector{
            TColumnSchema("ride_date", EValueType::String, ESortOrder::Ascending).SetRequired(true),
            TColumnSchema("ride_time", EValueType::String, ESortOrder::Ascending).SetRequired(true),
            TColumnSchema("log_time", EValueType::String, ESortOrder::Ascending).SetRequired(true),
            TColumnSchema("rover", EValueType::String, ESortOrder::Ascending).SetRequired(true),
            TColumnSchema("git_branch", EValueType::String).SetRequired(false),
            TColumnSchema("profile", EValueType::String).SetRequired(false),
            TColumnSchema("track", EValueType::String).SetRequired(false),
            TColumnSchema("flag_hardtest", EValueType::Boolean).SetRequired(false),
            TColumnSchema("ride_tags", EValueType::Any).SetRequired(false)
        });

        dataSplit.TableSchema = schema;

        EXPECT_CALL(PrepareMock_, GetInitialSplit("//b"))
            .WillRepeatedly(Return(MakeFuture(dataSplit)));
    }

    llvm::FoldingSetNodeID id1;
    {
        TString queryString =
        R"(
            *
            FROM [//a] e
            LEFT JOIN [//b] l ON (e.ride_date, e.ride_time, e.log_time, e.rover) = (l.ride_date, l.ride_time, l.log_time, l.rover)
            WHERE
            if(NOT is_null(e.tags), list_contains(e.tags, "0"), false) AND (l.profile IN ("")) AND (l.track IN ("")) AND NOT if(NOT is_null(e.tags), list_contains(e.tags, "1"), false)
            ORDER BY e._key DESC OFFSET 0 LIMIT 200
        )";

        auto query = PreparePlanFragment(&PrepareMock_, queryString)->Query;

        TCGVariables variables;
        Profile(query, &id1, &variables, [] (TQueryPtr, TConstJoinClausePtr) -> TJoinSubqueryEvaluator {
            return {};
        });
    }

    llvm::FoldingSetNodeID id2;
    {
        TString queryString =
        R"(
            *
            FROM [//a] e
            LEFT JOIN [//b] l ON (e.ride_date, e.ride_time, e.log_time, e.rover) = (l.ride_date, l.ride_time, l.log_time, l.rover)
            WHERE
            (l.profile IN ("")) AND (l.track IN ("")) AND if(NOT is_null(e.tags), list_contains(e.tags, "0"), false) AND NOT if(NOT is_null(e.tags), list_contains(e.tags, "1"), false)
            ORDER BY e._key DESC OFFSET 0 LIMIT 200
        )";

        auto query = PreparePlanFragment(&PrepareMock_, queryString)->Query;

        TCGVariables variables;
        Profile(query, &id2, &variables, [] (TQueryPtr, TConstJoinClausePtr) -> TJoinSubqueryEvaluator {
            return {};
        });
    }

    EXPECT_EQ(id1, id2);
}

TEST_F(TQueryPrepareTest, DisjointGroupBy)
{
    {
        TDataSplit dataSplit;

        SetObjectId(&dataSplit, MakeId(EObjectType::Table, 0x42, 0, 0xdeadbabe));

        auto schema = New<TTableSchema>(std::vector{
            TColumnSchema("a", EValueType::Int64, ESortOrder::Ascending),
            TColumnSchema("b", EValueType::Int64),
            TColumnSchema("c", EValueType::Int64)
        });

        dataSplit.TableSchema = schema;

        EXPECT_CALL(PrepareMock_, GetInitialSplit("//t"))
            .WillRepeatedly(Return(MakeFuture(dataSplit)));
    }

    {
        TDataSplit dataSplit;

        SetObjectId(&dataSplit, MakeId(EObjectType::Table, 0x42, 0, 0xdeadbabe));

        auto schema = New<TTableSchema>(std::vector{
            TColumnSchema("a", EValueType::Int64, ESortOrder::Ascending),
            TColumnSchema("b", EValueType::Int64, ESortOrder::Ascending),
            TColumnSchema("c", EValueType::Int64)
        });

        dataSplit.TableSchema = schema;

        EXPECT_CALL(PrepareMock_, GetInitialSplit("//s"))
            .WillRepeatedly(Return(MakeFuture(dataSplit)));
    }

    llvm::FoldingSetNodeID id1;
    {
        TString queryString =
        R"(
            *
            FROM [//t]
            GROUP by a
        )";

        auto query = PreparePlanFragment(&PrepareMock_, queryString)->Query;

        TCGVariables variables;
        Profile(query, &id1, &variables, [] (TQueryPtr, TConstJoinClausePtr) -> TJoinSubqueryEvaluator {
            return {};
        });
    }

    llvm::FoldingSetNodeID id2;
    {
        TString queryString =
        R"(
            *
            FROM [//s]
            GROUP by a
        )";

        auto query = PreparePlanFragment(&PrepareMock_, queryString)->Query;

        TCGVariables variables;
        Profile(query, &id2, &variables, [] (TQueryPtr, TConstJoinClausePtr) -> TJoinSubqueryEvaluator {
            return {};
        });
    }

    EXPECT_NE(id1, id2);
}

TEST_F(TQueryPrepareTest, GroupByPrimaryKey)
{
    {
        TDataSplit dataSplit;

        SetObjectId(&dataSplit, MakeId(EObjectType::Table, 0x42, 0, 0xdeadbabe));

        auto schema = New<TTableSchema>(std::vector{
            TColumnSchema("hash", EValueType::Int64)
                .SetSortOrder(ESortOrder::Ascending)
                .SetExpression(TString("int64(farm_hash(a))")),
            TColumnSchema("a", EValueType::Int64)
                .SetSortOrder(ESortOrder::Ascending),
            TColumnSchema("b", EValueType::Int64)
                .SetSortOrder(ESortOrder::Ascending),
            TColumnSchema("v", EValueType::Int64),
        });

        dataSplit.TableSchema = schema;

        EXPECT_CALL(PrepareMock_, GetInitialSplit("//t"))
            .WillRepeatedly(Return(MakeFuture(dataSplit)));
    }

    {
        TString queryString = "* from [//t] group by hash, a, b";
        auto query = PreparePlanFragment(&PrepareMock_, queryString)->Query;
        EXPECT_TRUE(query->UseDisjointGroupBy);
    }

    {
        TString queryString = "* from [//t] group by a, b";
        auto query = PreparePlanFragment(&PrepareMock_, queryString)->Query;
        EXPECT_TRUE(query->UseDisjointGroupBy);
    }

    {
        TString queryString = "* from [//t] group by a, v";
        auto query = PreparePlanFragment(&PrepareMock_, queryString)->Query;
        EXPECT_EQ(query->GroupClause->CommonPrefixWithPrimaryKey, 1u);
        EXPECT_FALSE(query->UseDisjointGroupBy);
    }
}

TEST_F(TQueryPrepareTest, OrderByPrimaryKeyPrefix)
{
    {
        TDataSplit dataSplit;

        SetObjectId(&dataSplit, MakeId(EObjectType::Table, 0x42, 0, 0xdeadbabe));

        auto schema = New<TTableSchema>(std::vector{
            TColumnSchema("hash", EValueType::Int64)
                .SetSortOrder(ESortOrder::Ascending)
                .SetExpression(TString("int64(farm_hash(a))")),
            TColumnSchema("a", EValueType::Int64)
                .SetSortOrder(ESortOrder::Ascending),
            TColumnSchema("b", EValueType::Int64)
                .SetSortOrder(ESortOrder::Ascending),
            TColumnSchema("v", EValueType::Int64),
        });

        dataSplit.TableSchema = schema;

        EXPECT_CALL(PrepareMock_, GetInitialSplit("//t"))
            .WillRepeatedly(Return(MakeFuture(dataSplit)));
    }

    {
        TString queryString = "* from [//t] order by hash, a limit 10";
        auto query = PreparePlanFragment(&PrepareMock_, queryString)->Query;
        EXPECT_FALSE(query->OrderClause);
    }

    {
        TString queryString = "* from [//t] order by hash, a, b limit 10";
        auto query = PreparePlanFragment(&PrepareMock_, queryString)->Query;
        EXPECT_FALSE(query->OrderClause);
    }

    {
        TString queryString = "* from [//t] order by hash, a offset 5 limit 5";
        auto query = PreparePlanFragment(&PrepareMock_, queryString)->Query;
        EXPECT_FALSE(query->OrderClause);
    }

    {
        TString queryString = "* from [//t] order by a, b limit 10";
        auto query = PreparePlanFragment(&PrepareMock_, queryString)->Query;
        EXPECT_TRUE(query->OrderClause);
    }
}

TEST_F(TQueryPrepareTest, InvalidUdfImpl)
{
    TTypeInferrerMapPtr TypeInferrers_ = New<TTypeInferrerMap>();
    TFunctionProfilerMapPtr FunctionProfilers_ = New<TFunctionProfilerMap>();
    TAggregateProfilerMapPtr AggregateProfilers_ = New<TAggregateProfilerMap>();

    MergeFrom(TypeInferrers_.Get(), *BuiltinTypeInferrersMap);
    MergeFrom(FunctionProfilers_.Get(), *BuiltinFunctionProfilers);
    MergeFrom(AggregateProfilers_.Get(), *BuiltinAggregateProfilers);

    auto builder = CreateFunctionRegistryBuilder(
        TypeInferrers_.Get(),
        FunctionProfilers_.Get(),
        AggregateProfilers_.Get());

    {
        TypeInferrers_->emplace("short_invalid_ir", New<TFunctionTypeInferrer>(
            std::unordered_map<TTypeArgument, TUnionType>{},
            std::vector<TType>{EValueType::Int64},
            EValueType::Null,
            EValueType::Int64));

        FunctionProfilers_->emplace("short_invalid_ir", New<TExternalFunctionCodegen>(
            "short_invalid_ir",
            "short_invalid_ir",
            TSharedRef(short_invalid_ir_bc, short_invalid_ir_bc_len, nullptr),
            GetCallingConvention(ECallingConvention::Simple),
            TSharedRef(),
            false));
    }

    {
        TypeInferrers_->emplace("long_invalid_ir", New<TFunctionTypeInferrer>(
            std::unordered_map<TTypeArgument, TUnionType>{},
            std::vector<TType>{EValueType::Int64},
            EValueType::Null,
            EValueType::Int64));

        FunctionProfilers_->emplace("long_invalid_ir", New<TExternalFunctionCodegen>(
            "long_invalid_ir",
            "long_invalid_ir",
            TSharedRef(long_invalid_ir_bc, long_invalid_ir_bc_len, nullptr),
            GetCallingConvention(ECallingConvention::Simple),
            TSharedRef(),
            false));
    }

    auto bcImplementations = "test_udfs";

    builder->RegisterFunction(
        "abs_udf_arity",
        "abs_udf",
        std::unordered_map<TTypeArgument, TUnionType>(),
        std::vector<TType>{EValueType::Int64, EValueType::Int64},
        EValueType::Null,
        EValueType::Int64,
        bcImplementations,
        ECallingConvention::Simple);

    builder->RegisterFunction(
        "abs_udf_double",
        "abs_udf",
        std::unordered_map<TTypeArgument, TUnionType>(),
        std::vector<TType>{EValueType::Double},
        EValueType::Null,
        EValueType::Int64,
        bcImplementations,
        ECallingConvention::Simple);

    auto schema = New<TTableSchema>(std::vector<TColumnSchema>{
        {"a", EValueType::Int64},
        {"b", EValueType::Int64}
    });

    { // ShortInvalidUdfImpl
        auto expr = PrepareExpression("short_invalid_ir(a)", *schema, TypeInferrers_);

        TCGVariables variables;

        EXPECT_THROW_THAT({
            auto codegen = Profile(expr, schema, nullptr, &variables, FunctionProfilers_);
            auto callback = codegen();
        }, HasSubstr("LLVM bitcode"));
    }

    { // LongInvalidUdfImpl
        auto expr = PrepareExpression("long_invalid_ir(a)", *schema, TypeInferrers_);

        TCGVariables variables;

        EXPECT_THROW_THAT({
            auto codegen = Profile(expr, schema, nullptr, &variables, FunctionProfilers_);
            auto callback = codegen();
        }, HasSubstr("LLVM bitcode"));
    }

    { // InvalidUdfArity
        auto expr = PrepareExpression("abs_udf_arity(a, b)", *schema, TypeInferrers_);

        TCGVariables variables;

        EXPECT_THROW_THAT({
            auto codegen = Profile(expr, schema, nullptr, &variables, FunctionProfilers_);
            auto callback = codegen();
        }, HasSubstr("LLVM bitcode"));
    }

    { // InvalidUdfType
        EXPECT_THROW_THAT({
            PrepareExpression("abs_udf_double(a)", *schema, TypeInferrers_);
        }, HasSubstr("Wrong type for argument"));
    }
}

TEST_F(TQueryPrepareTest, WronglyTypedAggregate)
{
    auto split = MakeSplit({
        {"a", EValueType::String}
    });

    EXPECT_CALL(PrepareMock_, GetInitialSplit("//t"))
        .WillRepeatedly(Return(MakeFuture(split)));

    EXPECT_THROW_THAT({
        PreparePlanFragment(&PrepareMock_, "avg(a) from [//t] group by 1");
    }, HasSubstr("Type mismatch in function \"avg\""));
}

TEST_F(TQueryPrepareTest, OrderByWithoutLimit)
{
    auto split = MakeSplit({
        {"a", EValueType::String}
    });

    EXPECT_CALL(PrepareMock_, GetInitialSplit("//t"))
        .WillRepeatedly(Return(MakeFuture(split)));

    EXPECT_THROW_THAT({
        PreparePlanFragment(&PrepareMock_, "* from [//t] order by a");
    }, HasSubstr("ORDER BY used without LIMIT"));
}

TEST_F(TQueryPrepareTest, OffsetLimit)
{
    auto split = MakeSplit({
        {"a", EValueType::String}
    });

    EXPECT_CALL(PrepareMock_, GetInitialSplit("//t"))
        .WillRepeatedly(Return(MakeFuture(split)));

    EXPECT_THROW_THAT({
        PreparePlanFragment(&PrepareMock_, "* from [//t] offset 5");
    }, HasSubstr("OFFSET used without LIMIT"));
}

////////////////////////////////////////////////////////////////////////////////

class TJobQueryPrepareTest
    : public ::testing::Test
{ };

TEST_F(TJobQueryPrepareTest, TruePredicate)
{
    ParseSource("* where true", EParseMode::JobQuery);
}

TEST_F(TJobQueryPrepareTest, FalsePredicate)
{
    ParseSource("* where false", EParseMode::JobQuery);
}

////////////////////////////////////////////////////////////////////////////////

typedef std::vector<TDataSplit> TDataSplits;

class TQueryCoordinateTest
    : public ::testing::Test
{
protected:
    void SetUp() override
    {
        EXPECT_CALL(PrepareMock_, GetInitialSplit("//t"))
            .WillOnce(Return(MakeFuture(MakeSimpleSplit("//t"))));

        auto config = New<TColumnEvaluatorCacheConfig>();
        ColumnEvaluatorCache_ = CreateColumnEvaluatorCache(config);

        MergeFrom(RangeExtractorMap.Get(), *BuiltinRangeExtractorMap);
    }

    void Coordinate(const TString& source, const TDataSplits& dataSplits, size_t subqueriesCount)
    {
        auto fragment = PreparePlanFragment(
            &PrepareMock_,
            source);

        auto buffer = New<TRowBuffer>();
        TRowRanges sources;
        for (const auto& split : dataSplits) {
            sources.emplace_back(
                buffer->CaptureRow(split.LowerBound),
                buffer->CaptureRow(split.UpperBound));
        }

        auto rowBuffer = New<TRowBuffer>();

        TQueryOptions options;
        options.RangeExpansionLimit = 1000;
        options.VerboseLogging = true;

        auto prunedRanges = GetPrunedRanges(
            fragment->Query,
            MakeId(EObjectType::Table, 0x42, 0, 0xdeadbabe),
            MakeSharedRange(std::move(sources), buffer),
            rowBuffer,
            ColumnEvaluatorCache_,
            RangeExtractorMap,
            options);

        EXPECT_EQ(prunedRanges.size(), subqueriesCount);
    }

    StrictMock<TPrepareCallbacksMock> PrepareMock_;
    IColumnEvaluatorCachePtr ColumnEvaluatorCache_;

    TRangeExtractorMapPtr RangeExtractorMap = New<TRangeExtractorMap>();
};

TEST_F(TQueryCoordinateTest, EmptySplit)
{
    TDataSplits emptySplits;

    EXPECT_NO_THROW({
        Coordinate("k from [//t]", emptySplits, 0);
    });
}

TEST_F(TQueryCoordinateTest, SingleSplit)
{
    TDataSplits singleSplit;
    singleSplit.emplace_back(MakeSimpleSplit("//t", 1));

    EXPECT_NO_THROW({
        Coordinate("k from [//t]", singleSplit, 1);
    });
}

TEST_F(TQueryCoordinateTest, UsesKeyToPruneSplits)
{
    TDataSplits splits;

    splits.emplace_back(MakeSimpleSplit("//t", 1));
    splits.back().LowerBound = YsonToKey("0;0;0");
    splits.back().UpperBound = YsonToKey("1;0;0");

    splits.emplace_back(MakeSimpleSplit("//t", 2));
    splits.back().LowerBound = YsonToKey("1;0;0");
    splits.back().UpperBound = YsonToKey("2;0;0");

    splits.emplace_back(MakeSimpleSplit("//t", 3));
    splits.back().LowerBound = YsonToKey("2;0;0");
    splits.back().UpperBound = YsonToKey("3;0;0");

    EXPECT_NO_THROW({
        Coordinate("a from [//t] where k = 1 and l = 2 and m = 3", splits, 1);
    });
}

TEST_F(TQueryCoordinateTest, SimpleIn)
{
    TDataSplits singleSplit;
    singleSplit.emplace_back(MakeSimpleSplit("//t", 1));

    EXPECT_NO_THROW({
        Coordinate("k from [//t] where k in (1u, 2.0, 3)", singleSplit, 3);
    });
}

////////////////////////////////////////////////////////////////////////////////

class TReaderMock
    : public ISchemafulUnversionedReader
{
public:
    MOCK_METHOD(IUnversionedRowBatchPtr, Read, (const TRowBatchReadOptions& options), (override));
    MOCK_METHOD(TFuture<void>, GetReadyEvent, (), (const, override));
    MOCK_METHOD(bool, IsFetchingCompleted, (), (const, override));
    MOCK_METHOD(std::vector<NChunkClient::TChunkId>, GetFailedChunkIds, (), (const, override));
    MOCK_METHOD(TDataStatistics, GetDataStatistics, (), (const, override));
    MOCK_METHOD(NChunkClient::TCodecStatistics, GetDecompressionStatistics, (), (const, override));
};

class TWriterMock
    : public IUnversionedRowsetWriter
{
public:
    MOCK_METHOD(TFuture<void>, Close, (), (override));
    MOCK_METHOD(bool, Write, (TRange<TUnversionedRow>), (override));
    MOCK_METHOD(TFuture<void>, GetReadyEvent, (), (override));
};

TOwningRow YsonToRow(
    const TString& yson,
    const TDataSplit& dataSplit,
    bool treatMissingAsNull = true)
{
    auto tableSchema = dataSplit.TableSchema;
    return NTableClient::YsonToSchemafulRow(yson, *tableSchema, treatMissingAsNull);
}

TQueryStatistics DoExecuteQuery(
    IEvaluatorPtr evaluator,
    const std::vector<TString>& source,
    TFunctionProfilerMapPtr functionProfilers,
    TAggregateProfilerMapPtr aggregateProfilers,
    TConstQueryPtr query,
    IUnversionedRowsetWriterPtr writer,
    const TQueryBaseOptions& options,
    TJoinSubqueryProfiler joinProfiler = nullptr)
{
    std::vector<TOwningRow> owningSourceRows;
    for (const auto& row : source) {
        owningSourceRows.push_back(NTableClient::YsonToSchemafulRow(row, *query->GetReadSchema(), true));
    }

    std::vector<TRow> sourceRows;
    for (const auto& row : owningSourceRows) {
        sourceRows.push_back(row.Get());
    }

    auto rowsBegin = sourceRows.begin();
    auto rowsEnd = sourceRows.end();

    // Use small batch size for tests.
    const size_t maxBatchSize = 5;

    ssize_t batchSize = maxBatchSize;

    if (query->IsOrdered() && query->Offset + query->Limit < batchSize) {
        batchSize = query->Offset + query->Limit;
    }

    bool isFirstRead = true;
    auto readRows = [&] (const TRowBatchReadOptions& options) {
        // Free memory to test correct capturing of data.
        auto readCount = std::distance(sourceRows.begin(), rowsBegin);
        for (ssize_t index = 0; index < readCount; ++index) {
            sourceRows[index] = TRow();
            owningSourceRows[index] = TOwningRow();
        }

        if (isFirstRead && query->IsOrdered()) {
            EXPECT_EQ(options.MaxRowsPerRead, std::min(RowsetProcessingSize, query->Offset + query->Limit));
            isFirstRead = false;
        }

        auto size = std::min<size_t>(options.MaxRowsPerRead, std::distance(rowsBegin, rowsEnd));
        std::vector<TRow> rows(rowsBegin, rowsBegin + size);
        rowsBegin += size;
        batchSize = std::min<size_t>(batchSize * 2, maxBatchSize);
        return rows.empty()
            ? nullptr
            : CreateBatchFromUnversionedRows(MakeSharedRange(std::move(rows)));
    };

    auto readerMock = New<NiceMock<TReaderMock>>();
    EXPECT_CALL(*readerMock, Read(_))
        .WillRepeatedly(Invoke(readRows));
    ON_CALL(*readerMock, GetReadyEvent())
        .WillByDefault(Return(VoidFuture));

    return evaluator->Run(
        query,
        readerMock,
        writer,
        joinProfiler,
        functionProfilers,
        aggregateProfilers,
        GetDefaultMemoryChunkProvider(),
        options);
}

std::vector<TRow> OrderRowsBy(TRange<TRow> rows, const std::vector<TString>& columns, const TTableSchema& tableSchema)
{
    std::vector<int> indexes;
    for (const auto& column : columns) {
        indexes.push_back(tableSchema.GetColumnIndexOrThrow(column));
    }

    std::vector<TRow> result(rows.begin(), rows.end());
    std::sort(result.begin(), result.end(), [&] (TRow lhs, TRow rhs) {
        for (auto index : indexes) {
            if (lhs[index] == rhs[index]) {
                continue;
            } else {
                return lhs[index] < rhs[index];
            }
        }
        return false;
    });
    return result;
}

typedef std::function<void(TRange<TRow>, const TTableSchema&)> TResultMatcher;

TResultMatcher ResultMatcher(std::vector<TOwningRow> expectedResult, TTableSchemaPtr expectedSchema = nullptr)
{
    return [
            expectedResult = std::move(expectedResult),
            expectedSchema = std::move(expectedSchema)
        ] (TRange<TRow> result, const TTableSchema& tableSchema) {
            if (expectedSchema) {
                EXPECT_EQ(*expectedSchema, tableSchema);
                if (*expectedSchema != tableSchema) {
                    return;
                }
            }
            EXPECT_EQ(expectedResult.size(), result.Size());
            if (expectedResult.size() != result.Size()) {
                return;
            }

            for (int i = 0; i < std::ssize(expectedResult); ++i) {
                auto expectedRow = expectedResult[i];
                auto row = result[i];
                EXPECT_EQ(expectedRow.GetCount(), static_cast<int>(row.GetCount()));
                if (expectedRow.GetCount() != static_cast<int>(row.GetCount())) {
                    continue;
                }
                for (int j = 0; j < expectedRow.GetCount(); ++j) {
                    const auto& expectedValue = expectedRow[j];
                    const auto& value = row[j];
                    EXPECT_EQ(expectedValue.Type, value.Type);
                    if (expectedValue.Type != value.Type) {
                        continue;
                    }
                    if (expectedValue.Type == EValueType::Any || expectedValue.Type == EValueType::Composite) {
                        // Slow path.
                        auto expectedYson = TYsonString(expectedValue.AsString());
                        auto expectedStableYson = ConvertToYsonString(ConvertToNode(expectedYson));
                        auto yson = TYsonString(value.AsString());
                        auto stableYson = ConvertToYsonString(ConvertToNode(yson));
                        EXPECT_EQ(expectedStableYson, stableYson);
                    } else {
                        // Fast path.
                        EXPECT_EQ(expectedValue, value);
                    }
                }
            }
        };
}

TResultMatcher OrderedResultMatcher(
    std::vector<TOwningRow> expectedResult,
    std::vector<TString> columns)
{
    return [
            expectedResult = std::move(expectedResult),
            columns = std::move(columns)
        ] (TRange<TRow> result, const TTableSchema& tableSchema) {
            EXPECT_EQ(expectedResult.size(), result.Size());

            auto sortedResult = OrderRowsBy(result, columns, tableSchema);

            for (int i = 0; i < std::ssize(expectedResult); ++i) {
                EXPECT_EQ(sortedResult[i], expectedResult[i]);
            }
        };
}

class TQueryEvaluateTest
    : public ::testing::Test
{
protected:
    void SetUp() override
    {
        ActionQueue_ = New<TActionQueue>("Test");

        auto bcImplementations = "test_udfs";

        MergeFrom(TypeInferrers_.Get(), *BuiltinTypeInferrersMap);
        MergeFrom(FunctionProfilers_.Get(), *BuiltinFunctionProfilers);
        MergeFrom(AggregateProfilers_.Get(), *BuiltinAggregateProfilers);

        auto builder = CreateFunctionRegistryBuilder(
            TypeInferrers_.Get(),
            FunctionProfilers_.Get(),
            AggregateProfilers_.Get());

        builder->RegisterFunction(
            "abs_udf",
            std::vector<TType>{EValueType::Int64},
            EValueType::Int64,
            bcImplementations,
            ECallingConvention::Simple);
        builder->RegisterFunction(
            "exp_udf",
            std::vector<TType>{EValueType::Int64, EValueType::Int64},
            EValueType::Int64,
            bcImplementations,
            ECallingConvention::Simple);
        builder->RegisterFunction(
            "strtol_udf",
            std::vector<TType>{EValueType::String},
            EValueType::Uint64,
            bcImplementations,
            ECallingConvention::Simple);
        builder->RegisterFunction(
            "tolower_udf",
            std::vector<TType>{EValueType::String},
            EValueType::String,
            bcImplementations,
            ECallingConvention::Simple);
        builder->RegisterFunction(
            "is_null_udf",
            std::vector<TType>{EValueType::String},
            EValueType::Boolean,
            bcImplementations,
            ECallingConvention::UnversionedValue);
        builder->RegisterFunction(
            "sum_udf",
            std::unordered_map<TTypeArgument, TUnionType>(),
            std::vector<TType>{EValueType::Int64},
            EValueType::Int64,
            EValueType::Int64,
            bcImplementations);
        builder->RegisterFunction(
            "seventyfive",
            std::vector<TType>{},
            EValueType::Uint64,
            bcImplementations,
            ECallingConvention::Simple);
        builder->RegisterFunction(
            "throw_if_negative_udf",
            std::vector<TType>{EValueType::Int64},
            EValueType::Int64,
            bcImplementations,
            ECallingConvention::Simple);
    }

    void TearDown() override
    {
        ActionQueue_->Shutdown();
    }

    std::pair<TQueryPtr, TQueryStatistics> EvaluateWithQueryStatistics(
        const TString& query,
        const std::map<TString, TDataSplit>& dataSplits,
        const std::vector<std::vector<TString>>& owningSources,
        const TResultMatcher& resultMatcher,
        i64 inputRowLimit = std::numeric_limits<i64>::max(),
        i64 outputRowLimit = std::numeric_limits<i64>::max(),
        NYson::TYsonStringBuf placeholderValues = {})
    {
        return BIND(&TQueryEvaluateTest::DoEvaluate, this)
            .AsyncVia(ActionQueue_->GetInvoker())
            .Run(
                query,
                dataSplits,
                owningSources,
                resultMatcher,
                inputRowLimit,
                outputRowLimit,
                false,
                placeholderValues)
            .Get()
            .ValueOrThrow();
    }

    TQueryPtr Evaluate(
        const TString& query,
        const std::map<TString, TDataSplit>& dataSplits,
        const std::vector<std::vector<TString>>& owningSources,
        const TResultMatcher& resultMatcher,
        i64 inputRowLimit = std::numeric_limits<i64>::max(),
        i64 outputRowLimit = std::numeric_limits<i64>::max(),
        NYson::TYsonStringBuf placeholderValues = {})
    {
        return EvaluateWithQueryStatistics(
            query,
            dataSplits,
            owningSources,
            resultMatcher,
            inputRowLimit,
            outputRowLimit,
            placeholderValues).first;
    }

    std::pair<TQueryPtr, TQueryStatistics> EvaluateWithQueryStatistics(
        const TString& query,
        const TDataSplit& dataSplit,
        const std::vector<TString>& owningSourceRows,
        const TResultMatcher& resultMatcher,
        i64 inputRowLimit = std::numeric_limits<i64>::max(),
        i64 outputRowLimit = std::numeric_limits<i64>::max(),
        NYson::TYsonStringBuf placeholderValues = {})
    {
        std::vector<std::vector<TString>> owningSources = {
            owningSourceRows
        };
        std::map<TString, TDataSplit> dataSplits = {
            {"//t", dataSplit}
        };

        return EvaluateWithQueryStatistics(
            query,
            dataSplits,
            owningSources,
            resultMatcher,
            inputRowLimit,
            outputRowLimit,
            placeholderValues);
    }

    TQueryPtr Evaluate(
        const TString& query,
        const TDataSplit& dataSplit,
        const std::vector<TString>& owningSourceRows,
        const TResultMatcher& resultMatcher,
        i64 inputRowLimit = std::numeric_limits<i64>::max(),
        i64 outputRowLimit = std::numeric_limits<i64>::max(),
        NYson::TYsonStringBuf placeholderValues = {})
    {
        return EvaluateWithQueryStatistics(
            query,
            dataSplit,
            owningSourceRows,
            resultMatcher,
            inputRowLimit,
            outputRowLimit,
            placeholderValues).first;
    }

    TQueryPtr EvaluateExpectingError(
        const TString& query,
        const TDataSplit& dataSplit,
        const std::vector<TString>& owningSourceRows,
        i64 inputRowLimit = std::numeric_limits<i64>::max(),
        i64 outputRowLimit = std::numeric_limits<i64>::max(),
        NYson::TYsonStringBuf placeholderValues = {})
    {
        std::vector<std::vector<TString>> owningSources = {
            owningSourceRows
        };
        std::map<TString, TDataSplit> dataSplits = {
            {"//t", dataSplit}
        };

        auto resultMatcher = [] (TRange<TRow>, const TTableSchema&) { };

        return BIND(&TQueryEvaluateTest::DoEvaluate, this)
            .AsyncVia(ActionQueue_->GetInvoker())
            .Run(
                query,
                dataSplits,
                owningSources,
                resultMatcher,
                inputRowLimit,
                outputRowLimit,
                true,
                placeholderValues)
            .Get()
            .ValueOrThrow().first;
    }


    TQueryPtr Prepare(
        const TString& query,
        const std::map<TString, TDataSplit>& dataSplits,
        NYson::TYsonStringBuf placeholderValues)
    {
        for (const auto& dataSplit : dataSplits) {
            EXPECT_CALL(PrepareMock_, GetInitialSplit(dataSplit.first))
                .WillOnce(Return(MakeFuture(dataSplit.second)));
        }

        auto fetchFunctions = [&] (const std::vector<TString>& /*names*/, const TTypeInferrerMapPtr& typeInferrers) {
            MergeFrom(typeInferrers.Get(), *TypeInferrers_);
        };

        auto fragment = PreparePlanFragment(
            &PrepareMock_,
            query,
            fetchFunctions,
            placeholderValues);

        return fragment->Query;
    }

    std::pair<TQueryPtr, TQueryStatistics> DoEvaluate(
        const TString& query,
        const std::map<TString, TDataSplit>& dataSplits,
        const std::vector<std::vector<TString>>& owningSources,
        const TResultMatcher& resultMatcher,
        i64 inputRowLimit,
        i64 outputRowLimit,
        bool failure,
        NYson::TYsonStringBuf placeholderValues)
    {
        auto primaryQuery = Prepare(query, dataSplits, placeholderValues);

        TQueryBaseOptions options;
        options.InputRowLimit = inputRowLimit;
        options.OutputRowLimit = outputRowLimit;

        size_t sourceIndex = 1;

        auto profileCallback = [&] (TQueryPtr subquery, TConstJoinClausePtr joinClause) mutable {
            auto rows = owningSources[sourceIndex++];

            return [&, rows, subquery, joinClause] (
                std::vector<TRow> keys,
                TRowBufferPtr permanentBuffer) mutable
            {
                TDataSource dataSource;
                TQueryPtr preparedSubquery;
                std::tie(preparedSubquery, dataSource) = GetForeignQuery(
                    subquery,
                    joinClause,
                    std::move(keys),
                    permanentBuffer);

                auto pipe = New<NTableClient::TSchemafulPipe>(GetDefaultMemoryChunkProvider());

                DoExecuteQuery(
                    Evaluator_,
                    rows,
                    FunctionProfilers_,
                    AggregateProfilers_,
                    preparedSubquery,
                    pipe->GetWriter(),
                    options);

                return pipe->GetReader();
            };
        };

        auto prepareAndExecute = [&] () {
            IUnversionedRowsetWriterPtr writer;
            TFuture<IUnversionedRowsetPtr> asyncResultRowset;

            std::tie(writer, asyncResultRowset) = CreateSchemafulRowsetWriter(primaryQuery->GetTableSchema());

            auto queryStatistics = DoExecuteQuery(
                Evaluator_,
                owningSources.front(),
                FunctionProfilers_,
                AggregateProfilers_,
                primaryQuery,
                writer,
                options,
                profileCallback);

            auto resultRowset = WaitFor(asyncResultRowset)
                .ValueOrThrow();

            resultMatcher(resultRowset->GetRows(), *primaryQuery->GetTableSchema());

            return std::make_pair(primaryQuery, queryStatistics);
        };

        if (failure) {
            EXPECT_THROW(prepareAndExecute(), TErrorException);
            return {nullptr, TQueryStatistics{}};
        } else {
            return prepareAndExecute();
        }
    }

    const IEvaluatorPtr Evaluator_ = CreateEvaluator(New<TExecutorConfig>());

    StrictMock<TPrepareCallbacksMock> PrepareMock_;
    TActionQueuePtr ActionQueue_;

    const TTypeInferrerMapPtr TypeInferrers_ = New<TTypeInferrerMap>();
    const TFunctionProfilerMapPtr FunctionProfilers_ = New<TFunctionProfilerMap>();
    const TAggregateProfilerMapPtr AggregateProfilers_ = New<TAggregateProfilerMap>();

};

std::vector<TOwningRow> YsonToRows(TRange<TString> rowsData, const TDataSplit& split)
{
    std::vector<TOwningRow> result;

    for (auto row : rowsData) {
        result.push_back(YsonToRow(row, split, true));
    }

    return result;
}

TEST_F(TQueryEvaluateTest, Simple)
{
    auto split = MakeSplit({
        {"a", EValueType::Int64},
        {"b", EValueType::Int64}
    });

    std::vector<TString> source = {
        "a=4;b=5",
        "a=10;b=11"
    };

    auto result = YsonToRows({
        "a=4;b=5",
        "a=10;b=11"
    }, split);

    Evaluate("a, b FROM [//t]", split, source, ResultMatcher(result));
}

TEST_F(TQueryEvaluateTest, SimpleOffsetLimit)
{
    auto split = MakeSplit({
        {"a", EValueType::Int64}
    });

    std::vector<TString> source = {
        "a=0",
        "a=1",
        "a=2",
        "a=3",
        "a=4",
        "a=5",
        "a=6",
        "a=7",
        "a=8"
    };

    {
        auto result = YsonToRows({
            "a=0",
            "a=1",
            "a=2",
            "a=3",
            "a=4"
        }, split);

        Evaluate("a FROM [//t] limit 5", split, source, ResultMatcher(result));
    }

    {
        auto result = YsonToRows({
            "a=5"
        }, split);

        Evaluate("a FROM [//t] offset 5 limit 1", split, source, ResultMatcher(result));
    }
}

TEST_F(TQueryEvaluateTest, SimpleAlias)
{
    auto split = MakeSplit({
        {"a", EValueType::Int64},
        {"b", EValueType::Int64}
    });

    std::vector<TString> source = {
        "a=4;b=5",
        "a=10;b=11"
    };

    auto result = YsonToRows({
        "a=16;b=5",
        "a=100;b=11"
    }, split);

    Evaluate("a * a as a, b FROM [//t]", split, source, ResultMatcher(result));
}

TEST_F(TQueryEvaluateTest, SelectAll)
{
    auto split = MakeSplit({
        {"a", EValueType::Int64},
        {"b", EValueType::Int64}
    });

    std::vector<TString> source = {
        "a=4;b=5",
        "a=10;b=11"
    };

    auto result = YsonToRows({
        "a=4;b=5",
        "a=10;b=11"
    }, split);

    Evaluate("* FROM [//t]", split, source, ResultMatcher(result));
}

TEST_F(TQueryEvaluateTest, FilterNulls1)
{
    auto split = MakeSplit({
        {"a", EValueType::Int64},
        {"b", EValueType::Int64}
    });

    std::vector<TString> source = {
        "a=4;b=5",
        "a=6",
        "a=10;b=11"
    };

    auto result = YsonToRows({
        "a=4;b=5",
        "a=10;b=11"
    }, split);

    Evaluate("a, b FROM [//t] where b > 0", split, source, ResultMatcher(result));
}

TEST_F(TQueryEvaluateTest, FilterNulls2)
{
    auto split = MakeSplit({
        {"a", EValueType::Int64},
        {"b", EValueType::Int64}
    });

    std::vector<TString> source = {
        "a=4;b=5",
        "a=6",
        "a=10;b=11"
    };

    auto result = YsonToRows({
        "a=4;b=5",
        "a=6",
        "a=10;b=11"
    }, split);

    Evaluate("a, b FROM [//t] where b > 0 or is_null(b)", split, source, ResultMatcher(result));
}

TEST_F(TQueryEvaluateTest, SimpleCmpInt)
{
    auto split = MakeSplit({
        {"a", EValueType::Int64},
        {"b", EValueType::Int64}
    });

    std::vector<TString> source = {
        "a=4;b=5",
        "a=6;b=6"
    };

    auto resultSplit = MakeSplit({
        {"r1", EValueType::Boolean},
        {"r2", EValueType::Boolean},
        {"r3", EValueType::Boolean},
        {"r4", EValueType::Boolean},
        {"r5", EValueType::Boolean}
    });

    auto result = YsonToRows({
        "r1=%true;r2=%false;r3=%true;r4=%false;r5=%false",
        "r1=%false;r2=%false;r3=%true;r4=%true;r5=%true"
    }, resultSplit);

    Evaluate("a < b as r1, a > b as r2, a <= b as r3, a >= b as r4, a = b as r5 FROM [//t]", split, source, ResultMatcher(result));
}

TEST_F(TQueryEvaluateTest, SimpleCmpString)
{
    auto split = MakeSplit({
        {"a", EValueType::String},
        {"b", EValueType::String}
    });

    std::vector<TString> source = {
        "a=\"a\";b=\"aa\"",
        "a=\"aa\";b=\"aa\""
    };

    auto resultSplit = MakeSplit({
        {"r1", EValueType::Boolean},
        {"r2", EValueType::Boolean},
        {"r3", EValueType::Boolean},
        {"r4", EValueType::Boolean},
        {"r5", EValueType::Boolean}
    });

    auto result = YsonToRows({
        "r1=%true;r2=%false;r3=%true;r4=%false;r5=%false",
        "r1=%false;r2=%false;r3=%true;r4=%true;r5=%true"
    }, resultSplit);

    Evaluate("a < b as r1, a > b as r2, a <= b as r3, a >= b as r4, a = b as r5 FROM [//t]", split, source, ResultMatcher(result));
}

TEST_F(TQueryEvaluateTest, SimpleBetweenAnd)
{
    auto split = MakeSplit({
        {"a", EValueType::Int64},
        {"b", EValueType::Int64}
    });

    std::vector<TString> source = {
        "a=4;b=5",
        "a=10;b=11",
        "a=15;b=11"
    };

    auto result = YsonToRows({
        "a=10;b=11"
    }, split);

    Evaluate("a, b FROM [//t] where a between 9 and 11", split, source, ResultMatcher(result));
}

TEST_F(TQueryEvaluateTest, MultipleBetweenAnd)
{
    auto split = MakeSplit({
        {"a", EValueType::Int64},
        {"b", EValueType::Int64}
    });

    std::vector<TString> source = {
        "a=1;b=10",
        "a=1;b=30",
        "a=2;b=20",
        "a=2;b=30",
        "a=2;b=40",
        "a=2;b=50",
        "a=3;b=30",
        "a=3;b=50",
        "a=3;b=60",
        "a=4;b=5",
        "a=5;b=5",
        "a=6;b=5",
        "a=10;b=11",
        "a=15;b=11"
    };

    auto result = YsonToRows({
        "a=1;b=10",
        "a=2;b=30",
        "a=2;b=40",
        "a=3;b=50",
        "a=3;b=60",
        "a=4;b=5",
        "a=5;b=5"
    }, split);

    Evaluate(R"(
        a, b
    from [//t]
    where
        (a, b) between (
            (1) and (1, 20),
            (2, 30) and (2, 40),
            (3, 50) and (3),
            4 and 5
        )
    )", split, source, ResultMatcher(result));
}

TEST_F(TQueryEvaluateTest, MultipleBetweenAnd2)
{
    auto split = MakeSplit({
        {"a", EValueType::Int64},
        {"b", EValueType::Int64}
    });

    std::vector<TString> source;
    for (size_t i = 0; i < 100; ++i) {
        source.push_back(TString() + "a=" + ToString(i / 10) + ";b=" + ToString(i % 10));
    }

    auto data = source.data();

    std::vector<TString> resultData;
    resultData.insert(resultData.end(), data + 10, data + 13);
    resultData.insert(resultData.end(), data + 23, data + 25);
    resultData.insert(resultData.end(), data + 35, data + 40);
    resultData.insert(resultData.end(), data + 40, data + 60);

    std::vector<TOwningRow> result;
    for (auto row : resultData) {
        result.push_back(YsonToRow(row, split, true));
    }

    Evaluate(R"(
        a, b
    from [//t]
    where
        (a, b) between (
            (1) and (1, 2),
            (2, 3) and (2, 4),
            (3, 5) and (3),
            4 and 5
        )
    )", split, source, ResultMatcher(result));
}

TEST_F(TQueryEvaluateTest, SimpleIn)
{
    auto split = MakeSplit({
        {"a", EValueType::Int64},
        {"b", EValueType::Int64}
    });

    std::vector<TString> source = {
        "a=4;b=5",
        "a=-10;b=11",
        "a=15;b=11"
    };

    auto result = YsonToRows({
        "a=4;b=5",
        "a=-10;b=11"
    }, split);

    Evaluate("a, b FROM [//t] where a in (4.0, -10)", split, source, ResultMatcher(result));
}

TEST_F(TQueryEvaluateTest, BigIn)
{
    auto split = MakeSplit({
        {"a", EValueType::Int64}
    });

    TStringBuilder inBuilder;
    inBuilder.AppendString("a in (");

    for (int i = 0; i < 50; ++ i) {
        if (i != 0) {
            inBuilder.AppendString(", ");
        }
        inBuilder.AppendFormat("%v", i * 2);
    }

    inBuilder.AppendString(")");

    std::vector<TString> source = {
        "a=4",
        "a=10",
        "a=15",
        "a=17",
        "a=18",
        "a=22",
        "a=31",
    };

    auto result = YsonToRows({
        "a=4",
        "a=10",
        "a=18",
        "a=22",
    }, split);

    Evaluate("a FROM [//t] where " + inBuilder.Flush(), split, source, ResultMatcher(result));
}

TEST_F(TQueryEvaluateTest, SimpleInWithNull)
{
    auto split = MakeSplit({
        {"a", EValueType::Int64},
        {"b", EValueType::Int64}
    });

    std::vector<TString> source = {
        "b=1",
        "a=2",
        "a=2;b=1",
        ""
    };

    auto result = YsonToRows({
        "b=1",
        "a=2",
    }, split);

    Evaluate("a, b FROM [//t] where (a, b) in ((null, 1), (2, null))", split, source, ResultMatcher(result));
}

TEST_F(TQueryEvaluateTest, SimpleTransform)
{
    auto split = MakeSplit({
        {"a", EValueType::Int64}
    });

    std::vector<TString> source = {
        "a=4",
        "a=-10",
        "a=15"
    };

    auto resultSplit = MakeSplit({
        {"x", EValueType::Int64}
    });

    auto result = YsonToRows({
        "x=13",
        "x=17",
        "",
    }, resultSplit);

    Evaluate("transform(a, (4.0, -10), (13, 17)) as x FROM [//t]", split, source, ResultMatcher(result));
}

TEST_F(TQueryEvaluateTest, BigTransform)
{
    auto split = MakeSplit({
        {"a", EValueType::Int64}
    });

    TStringBuilder transformBuilder;
    transformBuilder.AppendString("transform(a, (");

    for (int i = -50; i < 50; ++ i) {
        if (i != -50) {
            transformBuilder.AppendString(", ");
        }
        transformBuilder.AppendFormat("%v", i);
    }

    transformBuilder.AppendString("), (");

    for (int i = -50; i < 50; ++ i) {
        if (i != -50) {
            transformBuilder.AppendString(", ");
        }
        transformBuilder.AppendFormat("%v", -i);
    }

    transformBuilder.AppendString("))");

    std::vector<TString> source = {
        "a=4",
        "a=-10",
        "a=7",
        "a=60",
    };

    auto resultSplit = MakeSplit({
        {"x", EValueType::Int64}
    });

    auto result = YsonToRows({
        "x=-4",
        "x=10",
        "x=-7",
        "",
    }, resultSplit);

    Evaluate(transformBuilder.Flush() + " as x FROM [//t]", split, source, ResultMatcher(result));
}

TEST_F(TQueryEvaluateTest, SimpleTransform2)
{
    auto split = MakeSplit({
        {"a", EValueType::Int64},
        {"b", EValueType::String}
    });

    std::vector<TString> source = {
        "a=4;b=p",
        "a=-10;b=q",
        "a=-10;b=s",
        "a=15"
    };

    auto resultSplit = MakeSplit({
        {"x", EValueType::Int64}
    });

    auto result = YsonToRows({
        "x=13",
        "",
        "x=17",
        "",
    }, resultSplit);

    Evaluate(
        "transform((a, b), ((4.0, 'p'), (-10, 's')), (13, 17)) as x FROM [//t]",
        split,
        source,
        ResultMatcher(result));
}

TEST_F(TQueryEvaluateTest, SimpleTransformWithDefault)
{
    auto split = MakeSplit({
        {"a", EValueType::Int64},
        {"b", EValueType::String}
    });

    std::vector<TString> source = {
        "a=4;b=p",
        "a=-10;b=q",
        "a=-10;b=s",
        "a=15"
    };

    auto resultSplit = MakeSplit({
        {"x", EValueType::Int64}
    });

    auto result = YsonToRows({
        "x=13",
        "x=-9",
        "x=17",
        "x=16",
    }, resultSplit);

    Evaluate(
        "transform((a, b), ((4.0, 'p'), (-10, 's')), (13, 17), a + 1) as x FROM [//t]",
        split,
        source,
        ResultMatcher(result));
}

TEST_F(TQueryEvaluateTest, SimpleWithNull)
{
    auto split = MakeSplit({
        {"a", EValueType::Int64},
        {"b", EValueType::Int64},
        {"c", EValueType::Int64}
    });

    std::vector<TString> source = {
        "a=4;b=5",
        "a=10;b=11;c=9",
        "a=16"
    };

    auto result = YsonToRows({
        "a=4;b=5",
        "a=10;b=11;c=9",
        "a=16"
    }, split);

    Evaluate("a, b, c FROM [//t] where a > 3", split, source, ResultMatcher(result));
}

TEST_F(TQueryEvaluateTest, SimpleWithNull2)
{
    auto split = MakeSplit({
        {"a", EValueType::Int64},
        {"b", EValueType::Int64},
        {"c", EValueType::Int64}
    });

    std::vector<TString> source = {
        "a=1;b=2;c=3",
        "a=4",
        "a=5;b=5",
        "a=7;c=8",
        "a=10;b=1",
        "a=10;c=1"
    };

    auto resultSplit = MakeSplit({
        {"a", EValueType::Int64},
        {"x", EValueType::Int64}
    });

    auto result = YsonToRows({
        "a=1;x=5",
        "a=4;",
        "a=5;",
        "a=7;"
    }, resultSplit);

    Evaluate("a, b + c as x FROM [//t] where a < 10", split, source, ResultMatcher(result));
}

TEST_F(TQueryEvaluateTest, Strings)
{
    auto split = MakeSplit({
        {"s", EValueType::String}
    });

    std::vector<TString> source = {
        ""
    };

    auto resultSplit = MakeSplit({
        {"result", EValueType::String}
    });

    auto result = YsonToRows({
        "result=\"\\x0F\\xC7\\x84~\\0@\\0\\0<\\0\\0@\\x99l`\\x16\""
    }, resultSplit);

    Evaluate("\"\\x0F\\xC7\\x84~\\0@\\0\\0<\\0\\0@\\x99l`\\x16\" as result FROM [//t]", split, source,
        ResultMatcher(result));
}

TEST_F(TQueryEvaluateTest, SimpleStrings)
{
    auto split = MakeSplit({
        {"s", EValueType::String}
    });

    std::vector<TString> source = {
        "s=foo",
        "s=bar",
        "s=baz"
    };

    auto result = YsonToRows({
        "s=foo",
        "s=bar",
        "s=baz"
    }, split);

    Evaluate("s FROM [//t]", split, source, ResultMatcher(result));
}

TEST_F(TQueryEvaluateTest, SimpleStrings2)
{
    auto split = MakeSplit({
        {"s", EValueType::String},
        {"u", EValueType::String}
    });

    std::vector<TString> source = {
        "s=foo; u=x",
        "s=bar; u=y",
        "s=baz; u=x",
        "s=olala; u=z"
    };

    auto result = YsonToRows({
        "s=foo; u=x",
        "s=baz; u=x"
    }, split);

    Evaluate("s, u FROM [//t] where u = \"x\"", split, source, ResultMatcher(result));
}

TEST_F(TQueryEvaluateTest, IsPrefixStrings)
{
    auto split = MakeSplit({
        {"s", EValueType::String}
    });

    std::vector<TString> source = {
        "s=foobar",
        "s=bar",
        "s=baz"
    };

    auto result = YsonToRows({
        "s=foobar"
    }, split);

    Evaluate("s FROM [//t] where is_prefix(\"foo\", s)", split, source, ResultMatcher(result));
}

TEST_F(TQueryEvaluateTest, IsSubstrStrings)
{
    auto split = MakeSplit({
        {"s", EValueType::String}
    });

    std::vector<TString> source = {
        "s=foobar",
        "s=barfoo",
        "s=abc",
        "s=\"baz foo bar\"",
        "s=\"baz fo bar\"",
        "s=xyz",
        "s=baz"
    };

    auto result = YsonToRows({
        "s=foobar",
        "s=barfoo",
        "s=\"baz foo bar\"",
        "s=baz"
    }, split);

    Evaluate("s FROM [//t] where is_substr(\"foo\", s) or is_substr(s, \"XX baz YY\")", split, source, ResultMatcher(result));
}

TEST_F(TQueryEvaluateTest, GroupByBool)
{
    auto split = MakeSplit({
        {"a", EValueType::Int64},
        {"b", EValueType::Int64}
    });

    std::vector<TString> source = {
        "a=1;b=10",
        "a=2;b=20",
        "a=3;b=30",
        "a=4;b=40",
        "a=5;b=50",
        "a=6;b=60",
        "a=7;b=70",
        "a=8;b=80",
        "a=9;b=90"
    };

    auto resultSplit = MakeSplit({
        {"x", EValueType::Boolean},
        {"t", EValueType::Int64}
    });

    auto result = YsonToRows({
        "x=%false;t=200",
        "x=%true;t=240"
    }, resultSplit);

    Evaluate("x, sum(b) as t FROM [//t] where a > 1 group by a % 2 = 1 as x", split, source, ResultMatcher(result));

    SUCCEED();
}

TEST_F(TQueryEvaluateTest, GroupByAny)
{
    auto split = MakeSplit({
        {"a", EValueType::Int64},
        {"b", EValueType::Int64}
    });

    std::vector<TString> source;

    EXPECT_THROW_THAT(
        Evaluate("x, sum(b) as t FROM [//t] group by to_any(a) as x",
            split, source,  [] (TRange<TRow> /*result*/, const TTableSchema& /*tableSchema*/) { }),
        HasSubstr("Type mismatch in expression"));

    SUCCEED();
}

TEST_F(TQueryEvaluateTest, GroupByAlias)
{
    auto split = MakeSplit({
        {"a", EValueType::Int64},
        {"b", EValueType::Int64}
    });

    std::vector<TString> source = {
        "a=1;b=10",
        "a=2;b=20",
        "a=3;b=30",
        "a=4;b=40",
        "a=5;b=50",
        "a=6;b=60",
        "a=7;b=70",
        "a=8;b=80",
        "a=9;b=90"
    };

    auto resultSplit = MakeSplit({
        {"a", EValueType::Int64},
        {"b", EValueType::Int64}
    });

    auto result = YsonToRows({
        "a=1;b=123",
        "a=2;b=156",
        "a=0;b=180"
    }, resultSplit);

    Evaluate("a % 3 as a, sum(a + b) as b FROM [//t] group by a", split, source, ResultMatcher(result));

    SUCCEED();
}

TEST_F(TQueryEvaluateTest, GroupWithTotals)
{
    auto split = MakeSplit({
        {"a", EValueType::Int64},
        {"b", EValueType::Int64}
    });

    std::vector<TString> source = {
        "a=1;b=10",
        "a=2;b=20",
        "a=3;b=30",
        "a=4;b=40",
        "a=5;b=50",
        "a=6;b=60",
        "a=7;b=70",
        "a=8;b=80",
        "a=9;b=90"
    };

    auto resultSplitNoAggregate = MakeSplit({
        {"x", EValueType::Boolean}
    });

    auto resultWithTotalsNoAggregate = YsonToRows({
        "x=%false",
        "x=%true",
        ""
    }, resultSplitNoAggregate);

    Evaluate("x FROM [//t] where a > 1 group by a % 2 = 1 as x with totals", split,
        source, ResultMatcher(resultWithTotalsNoAggregate));

    auto resultSplit = MakeSplit({
        {"x", EValueType::Boolean},
        {"t", EValueType::Int64}
    });

    auto resultWithTotals = YsonToRows({
        "x=%false;t=200",
        "x=%true;t=240",
        "t=440"
    }, resultSplit);

    Evaluate("x, sum(b) as t FROM [//t] where a > 1 group by a % 2 = 1 as x with totals", split,
        source, ResultMatcher(resultWithTotals));

    auto resultWithTotalsAfterHaving = YsonToRows({
        "x=%true;t=240",
        "t=240"
    }, resultSplit);

    Evaluate("x, sum(b) as t FROM [//t] where a > 1 group by a % 2 = 1 as x having t > 200 with totals", split,
        source, ResultMatcher(resultWithTotalsAfterHaving));

    auto resultWithTotalsBeforeHaving = YsonToRows({
        "x=%true;t=240",
        "t=440"
    }, resultSplit);

    Evaluate("x, sum(b) as t FROM [//t] where a > 1 group by a % 2 = 1 as x with totals having t > 200", split,
        source, ResultMatcher(resultWithTotalsBeforeHaving));

    auto resultWithTotalsBeforeHaving2 = YsonToRows({
        "x=%false;t=200",
        "t=440"
    }, resultSplit);

    Evaluate("x, sum(b) as t FROM [//t] where a > 1 group by a % 2 = 1 as x with totals having t < 220", split,
        source, ResultMatcher(resultWithTotalsBeforeHaving2));

    SUCCEED();
}

TEST_F(TQueryEvaluateTest, GroupWithTotalsNulls)
{
    auto split = MakeSplit({
        {"a", EValueType::Int64},
        {"b", EValueType::Int64}
    });

    std::vector<TString> source = {
        "a=1;b=10",
        "b=20",
    };

    auto resultSplit = MakeSplit({
        {"x", EValueType::Int64},
        {"t", EValueType::Int64}
    });

    auto resultWithTotals = YsonToRows({
    }, resultSplit);

    EXPECT_THROW_THAT(
        Evaluate("x, sum(b) as t FROM [//t] group by a % 2 as x with totals", split,
            source, [] (TRange<TRow> /*result*/, const TTableSchema& /*tableSchema*/) { }),
        HasSubstr("Null values are forbidden in group key"));

    SUCCEED();
}

TEST_F(TQueryEvaluateTest, GroupWithTotalsEmpty)
{
    auto split = MakeSplit({
        {"a", EValueType::Int64},
        {"b", EValueType::Int64}
    });

    std::vector<TString> source = {
    };

    auto resultSplit = MakeSplit({
        {"x", EValueType::Int64},
        {"t", EValueType::Int64}
    });

    auto resultWithTotals = YsonToRows({
    }, resultSplit);

    Evaluate("x, sum(b) as t FROM [//t] group by a % 2 as x with totals", split,
        source, ResultMatcher(resultWithTotals));

    SUCCEED();
}

TEST_F(TQueryEvaluateTest, GroupWithLimitFirst)
{
    auto split = MakeSplit({
        {"a", EValueType::Int64, ESortOrder::Ascending},
        {"b", EValueType::Int64}
    });

    std::vector<TString> source;
    for (int i = 0; i < 10; i++) {
        source.push_back(Format("a=%v;b=%v", 1, i));
    }

    auto resultSplit = MakeSplit({
        {"f", EValueType::Int64}
    });

    auto result = YsonToRows({
        "f=0"
    }, resultSplit);

    auto queryStatistics = EvaluateWithQueryStatistics("first(b) as f FROM [//t] group by a limit 1", split, source, ResultMatcher(result)).second;
    EXPECT_EQ(queryStatistics.RowsRead, 3);

    SUCCEED();
}

TEST_F(TQueryEvaluateTest, GroupWithLimitFirstString)
{
    auto split = MakeSplit({
        {"a", EValueType::Int64, ESortOrder::Ascending},
        {"b", EValueType::String},
        {"c", EValueType::Int64}
    });

    std::vector<TString> source;
    for (int i = 0; i < 10; i++) {
        source.push_back(Format("a=%v;b=%Qv;c=%v", i % 3, i, i));
    }

    auto resultSplit = MakeSplit({
        {"f", EValueType::String}
    });

    auto result = YsonToRows({
        "f=\"0\""
    }, resultSplit);

    auto queryStatistics = EvaluateWithQueryStatistics(
        "first(b) as f FROM [//t] group by a limit 1",
        split,
        source,
        ResultMatcher(result)).second;
    EXPECT_EQ(queryStatistics.RowsRead, 3);

    SUCCEED();
}

TEST_F(TQueryEvaluateTest, ComplexWithAliases)
{
    auto split = MakeSplit({
        {"a", EValueType::Int64},
        {"b", EValueType::Int64}
    });

    std::vector<TString> source = {
        "a=1;b=10",
        "a=2;b=20",
        "a=3;b=30",
        "a=4;b=40",
        "a=5;b=50",
        "a=6;b=60",
        "a=7;b=70",
        "a=8;b=80",
        "a=9;b=90"
    };

    auto resultSplit = MakeSplit({
        {"x", EValueType::Int64},
        {"t", EValueType::Int64}
    });

    auto result = YsonToRows({
        "x=0;t=200",
        "x=1;t=241"
    }, resultSplit);

    Evaluate("a % 2 as x, sum(b) + x as t FROM [//t] where a > 1 group by x", split, source, ResultMatcher(result));

    SUCCEED();
}

TEST_F(TQueryEvaluateTest, Complex)
{
    auto split = MakeSplit({
        {"a", EValueType::Int64},
        {"b", EValueType::Int64}
    });

    std::vector<TString> source = {
        "a=1;b=10",
        "a=2;b=20",
        "a=3;b=30",
        "a=4;b=40",
        "a=5;b=50",
        "a=6;b=60",
        "a=7;b=70",
        "a=8;b=80",
        "a=9;b=90"
    };

    auto resultSplit = MakeSplit({
        {"x", EValueType::Int64},
        {"t", EValueType::Int64}
    });

    auto result = YsonToRows({
        "x=0;t=200",
        "x=1;t=241"
    }, resultSplit);

    Evaluate("x, sum(b) + x as t FROM [//t] where a > 1 group by a % 2 as x", split, source, ResultMatcher(result));

    SUCCEED();
}

TEST_F(TQueryEvaluateTest, Complex2)
{
    auto split = MakeSplit({
        {"a", EValueType::Int64},
        {"b", EValueType::Int64}
    });

    std::vector<TString> source = {
        "a=1;b=10",
        "a=2;b=20",
        "a=3;b=30",
        "a=4;b=40",
        "a=5;b=50",
        "a=6;b=60",
        "a=7;b=70",
        "a=8;b=80",
        "a=9;b=90"
    };

    auto resultSplit = MakeSplit({
        {"x", EValueType::Int64},
        {"q", EValueType::Int64},
        {"t", EValueType::Int64}
    });

    auto result = YsonToRows({
        "x=0;q=0;t=200",
        "x=1;q=0;t=241"
    }, resultSplit);

    Evaluate("x, q, sum(b) + x as t FROM [//t] where a > 1 group by a % 2 as x, 0 as q", split, source, ResultMatcher(result));

    SUCCEED();
}

TEST_F(TQueryEvaluateTest, ComplexBigResult)
{
    auto split = MakeSplit({
        {"a", EValueType::Int64},
        {"b", EValueType::Int64}
    });

    std::vector<TString> source;
    for (size_t i = 0; i < 10000; ++i) {
        source.push_back(TString() + "a=" + ToString(i) + ";b=" + ToString(i * 10));
    }

    auto resultSplit = MakeSplit({
        {"x", EValueType::Int64},
        {"t", EValueType::Int64}
    });

    std::vector<TOwningRow> result;

    for (size_t i = 2; i < 10000; ++i) {
        result.push_back(YsonToRow(TString() + "x=" + ToString(i) + ";t=" + ToString(i * 10 + i), resultSplit, false));
    }

    Evaluate("x, sum(b) + x as t FROM [//t] where a > 1 group by a as x", split, source, ResultMatcher(result));
}

TEST_F(TQueryEvaluateTest, ComplexWithNull)
{
    auto split = MakeSplit({
        {"a", EValueType::Int64},
        {"b", EValueType::Int64}
    });

    std::vector<TString> source = {
        "a=1;b=10",
        "a=2;b=20",
        "a=3;b=30",
        "a=4;b=40",
        "a=5;b=50",
        "a=6;b=60",
        "a=7;b=70",
        "a=8;b=80",
        "a=9;b=90",
        "a=10",
        "b=1",
        "b=2",
        "b=3"
    };

    auto resultSplit = MakeSplit({
        {"x", EValueType::Int64},
        {"t", EValueType::Int64},
        {"y", EValueType::Int64}
    });

    auto result = YsonToRows({
        "x=1;t=251;y=250",
        "x=0;t=200;y=200",
        "y=6"
    }, resultSplit);

    Evaluate("x, sum(b) + x as t, sum(b) as y FROM [//t] group by a % 2 as x", split, source, ResultMatcher(result));

    SUCCEED();
}

TEST_F(TQueryEvaluateTest, GroupWithTotalsAndLimit)
{
    auto split = MakeSplit({
        {"a", EValueType::Int64},
        {"b", EValueType::Int64}
    });

    size_t limit = 20;
    std::vector<i64> orderedKeys;
    std::unordered_map<i64, i64> groupedValues;
    i64 totalSum = 0;

    auto groupRow = [&] (i64 key, i64 value) {
        i64 x = key % 127;

        if (!groupedValues.count(x)) {
            if (groupedValues.size() >= limit) {
                return;
            } else {
                orderedKeys.push_back(x);
            }
        }

        groupedValues[x] += value;
        totalSum += value;
    };

    std::vector<TString> source;
    for (int i = 0; i < 1000; ++i) {
        auto key = std::rand() % 10000 + 1000;
        auto value = key * 10;

        source.push_back(Format("a=%v;b=%v", key, value));
        groupRow(key, value);
    }

    for (int i = 0; i < 1000; ++i) {
        auto key = 1000 - i;
        auto value = key * 10;

        source.push_back(Format("a=%v;b=%v", key, value));
        groupRow(key, value);
    }

    auto resultSplit = MakeSplit({
        {"x", EValueType::Int64},
        {"y", EValueType::Int64}
    });

    std::vector<TOwningRow> result;
    for (auto key : orderedKeys) {
        TString resultRow = Format("x=%v;y=%v", key, groupedValues[key]);
        result.push_back(YsonToRow(resultRow, resultSplit, false));
    }
    // TODO(lukyan): Try to make stable order of totals row
    result.push_back(YsonToRow("y=" + ToString(totalSum), resultSplit, true));

    Evaluate("x, sum(b) as y FROM [//t] group by a % 127 as x with totals limit 20",
        split, source, ResultMatcher(result));

    SUCCEED();
}

TEST_F(TQueryEvaluateTest, GroupDisjointTotalsLimit)
{
    auto split = MakeSplit({
        {"a", EValueType::Int64, ESortOrder::Ascending},
        {"b", EValueType::Int64, ESortOrder::Ascending},
        {"v", EValueType::Int64}
    });

    size_t limit = 20;
    std::vector<std::pair<i64, i64>> orderedKeys;
    THashMap<std::pair<i64, i64>, i64> groupedValues;
    i64 totalSum = 0;

    auto groupRow = [&] (i64 a, i64 b, i64 value) {
        auto key = std::make_pair(a, b % 3);

        if (!groupedValues.count(key)) {
            if (groupedValues.size() >= limit) {
                return;
            } else {
                orderedKeys.push_back(key);
            }
        }

        groupedValues[key] += value;
        totalSum += value;
    };

    std::vector<TString> source;
    for (int i = 0; i < 100; ++i) {
        auto a = i / 10;
        auto b = i % 10;
        auto v = i;

        source.push_back(Format("a=%v;b=%v;v=%v", a, b, v));
        groupRow(a, b, v);
    }

    auto resultSplit = MakeSplit({
        {"x", EValueType::Int64},
        {"y", EValueType::Int64},
        {"s", EValueType::Int64},
    });

    std::vector<TOwningRow> result;
    for (auto [a, b] : orderedKeys) {
        TString resultRow = Format("x=%v;y=%v;s=%v", a, b, groupedValues[std::make_pair(a, b)]);
        result.push_back(YsonToRow(resultRow, resultSplit, false));
    }
    // TODO(lukyan): Try to make stable order of totals row
    result.push_back(YsonToRow("s=" + ToString(totalSum), resultSplit, true));

    Evaluate("x, y, sum(v) as s FROM [//t] group by a as x, b % 3 as y with totals limit 20",
        split, source, ResultMatcher(result));

    SUCCEED();
}

TEST_F(TQueryEvaluateTest, JoinGroupWithLimit)
{
    std::map<TString, TDataSplit> splits;
    std::vector<std::vector<TString>> sources(2);

    splits["//left"] = MakeSplit({
        {"a", EValueType::Int64}
    }, 0);

    splits["//right"] = MakeSplit({
        {"a", EValueType::Int64},
        {"b", EValueType::Int64}
    }, 1);

    size_t limit = 20;
    std::vector<i64> orderedKeys;
    std::unordered_map<i64, i64> groupedValues;
    i64 totalSum = 0;

    auto groupRow = [&] (i64 key, i64 value) {
        i64 x = key % 31;

        if (!groupedValues.count(x)) {
            if (groupedValues.size() >= limit) {
                return;
            } else {
                orderedKeys.push_back(x);
            }
        }

        groupedValues[x] += value;
        totalSum += value;
    };

    for (int i = 0; i < 1000; ++i) {
        auto key = i;
        auto value = key * 10;

        bool joined = true;

        if (std::rand() % 2) {
            sources[0].push_back(Format("a=%v", key));
        } else {
            joined = false;
        }

        if (std::rand() % 2) {
            sources[1].push_back(Format("a=%v;b=%v", key, value));
        } else {
            joined = false;
        }

        if (joined) {
            groupRow(key, value);
        }
    }

    auto resultSplit = MakeSplit({
        {"x", EValueType::Int64},
        {"y", EValueType::Int64}
    });

    std::vector<TOwningRow> result;
    for (auto key : orderedKeys) {
        TString resultRow = Format("x=%v;y=%v", key, groupedValues[key]);
        result.push_back(YsonToRow(resultRow, resultSplit, false));
    }
    result.push_back(YsonToRow("y=" + ToString(totalSum), resultSplit, true));

    Evaluate(
        "x, sum(b) as y FROM [//left] join [//right] using a group by a % 31 as x with totals limit 20",
        splits,
        sources,
        ResultMatcher(result));

    SUCCEED();
}

TEST_F(TQueryEvaluateTest, HavingClause1)
{
    auto split = MakeSplit({
        {"a", EValueType::Int64},
        {"b", EValueType::Int64}
    });

    std::vector<TString> source = {
        "a=1;b=10",
        "a=1;b=10",
        "a=2;b=20",
        "a=2;b=20",
    };

    auto resultSplit = MakeSplit({
        {"x", EValueType::Int64},
        {"t", EValueType::Int64},
    });

    auto result = YsonToRows({
        "x=1;t=20",
    }, resultSplit);

    Evaluate("a as x, sum(b) as t FROM [//t] group by a having a = 1", split, source, ResultMatcher(result));

    SUCCEED();
}

TEST_F(TQueryEvaluateTest, HavingClause2)
{
    auto split = MakeSplit({
        {"a", EValueType::Int64},
        {"b", EValueType::Int64}
    });

    std::vector<TString> source = {
        "a=1;b=10",
        "a=1;b=10",
        "a=2;b=20",
        "a=2;b=20",
    };

    auto resultSplit = MakeSplit({
        {"x", EValueType::Int64},
        {"t", EValueType::Int64},
    });

    auto result = YsonToRows({
        "x=1;t=20",
    }, resultSplit);

    Evaluate("a as x, sum(b) as t FROM [//t] group by a having sum(b) = 20", split, source, ResultMatcher(result));

    SUCCEED();
}

TEST_F(TQueryEvaluateTest, HavingClause3)
{
    auto split = MakeSplit({
        {"a", EValueType::Int64},
        {"b", EValueType::Int64}
    });

    std::vector<TString> source = {
        "a=1;b=10",
        "a=1;b=10",
        "a=2;b=20",
        "a=2;b=20",
    };

    auto resultSplit = MakeSplit({
        {"x", EValueType::Int64}
    });

    auto result = YsonToRows({
        "x=1",
    }, resultSplit);

    Evaluate("a as x FROM [//t] group by a having sum(b) = 20", split, source, ResultMatcher(result));

    SUCCEED();
}

TEST_F(TQueryEvaluateTest, IsNull)
{
    auto split = MakeSplit({
        {"a", EValueType::Int64},
        {"b", EValueType::Int64}
    });

    std::vector<TString> source = {
        "a=1;b=10",
        "a=2;b=20",
        "a=9;b=90",
        "a=10",
        "b=1",
        "b=2",
        "b=3"
    };

    auto resultSplit = MakeSplit({
        {"b", EValueType::Int64}
    });

    auto result = YsonToRows({
        "b=1",
        "b=2",
        "b=3"
    }, resultSplit);

    Evaluate("b FROM [//t] where is_null(a)", split, source, ResultMatcher(result));

    SUCCEED();
}

TEST_F(TQueryEvaluateTest, DoubleSum)
{
    auto split = MakeSplit({
        {"a", EValueType::Double}
    });

    std::vector<TString> source = {
        "a=1.",
        "a=1.",
        ""
    };

    auto resultSplit = MakeSplit({
        {"x", EValueType::Double},
        {"t", EValueType::Int64}
    });

    auto result = YsonToRows({
        "x=2.;t=3"
    }, resultSplit);

    Evaluate("sum(a) as x, sum(1) as t FROM [//t] group by 1", split, source, ResultMatcher(result));
}

TEST_F(TQueryEvaluateTest, ComplexStrings)
{
    auto split = MakeSplit({
        {"a", EValueType::Int64},
        {"s", EValueType::String}
    });

    std::vector<TString> source = {
        "a=10;s=x",
        "a=20;s=y",
        "a=30;s=x",
        "a=40;s=x",
        "a=42",
        "a=50;s=x",
        "a=60;s=y",
        "a=70;s=z",
        "a=72",
        "a=80;s=y",
        "a=85",
        "a=90;s=z",
        "a=11;s=\"\"",
    };

    auto resultSplit = MakeSplit({
        {"x", EValueType::String},
        {"t", EValueType::Int64}
    });

    auto result = YsonToRows({
        "x=y;t=160",
        "x=x;t=120",
        "t=199",
        "x=z;t=160",
        "x=\"\";t=11"
    }, resultSplit);

    Evaluate("x, sum(a) as t FROM [//t] where a > 10 group by s as x", split, source, ResultMatcher(result));

    SUCCEED();
}

TEST_F(TQueryEvaluateTest, ComplexStringsLower)
{
    auto split = MakeSplit({
        {"a", EValueType::String},
        {"s", EValueType::String}
    });

    std::vector<TString> source = {
        "a=XyZ;s=one",
        "a=aB1C;s=two",
        "a=cs1dv;s=three",
        "a=HDs;s=four",
        "a=kIu;s=five",
        "a=trg1t;s=six"
    };

    auto resultSplit = MakeSplit({
        {"s", EValueType::String}
    });

    auto result = YsonToRows({
        "s=one",
        "s=two",
        "s=four",
        "s=five"
    }, resultSplit);

    Evaluate("s FROM [//t] where lower(a) in (\"xyz\",\"ab1c\",\"hds\",\"kiu\")", split, source, ResultMatcher(result));

    SUCCEED();
}

TEST_F(TQueryEvaluateTest, If)
{
    auto split = MakeSplit({
        {"a", EValueType::Int64},
        {"b", EValueType::Int64}
    });

    std::vector<TString> source = {
        "a=1;b=10",
        "a=2;b=20",
        "a=3;b=30",
        "a=4;b=40",
        "a=5;b=50",
        "a=6;b=60",
        "a=7;b=70",
        "a=8;b=80",
        "a=9;b=90"
    };

    auto resultSplit = MakeSplit({
        {"x", EValueType::String},
        {"t", EValueType::Double}
    });

    auto result = YsonToRows({
        "x=b;t=251.",
        "x=a;t=201."
    }, resultSplit);

    Evaluate("if(q = 4, \"a\", \"b\") as x, double(sum(b)) + 1.0 as t FROM [//t] group by if(a % 2 = 0, 4, 5) as"
                 " q", split, source, ResultMatcher(result));

    SUCCEED();
}

TEST_F(TQueryEvaluateTest, InputRowLimit)
{
    auto split = MakeSplit({
        {"a", EValueType::Int64},
        {"b", EValueType::Int64}
    });

    std::vector<TString> source = {
        "a=1;b=10",
        "a=2;b=20",
        "a=3;b=30",
        "a=4;b=40",
        "a=5;b=50",
        "a=6;b=60",
        "a=7;b=70",
        "a=8;b=80",
        "a=9;b=90"
    };

    auto result = YsonToRows({
        "a=2;b=20",
        "a=3;b=30"
    }, split);

    Evaluate("a, b FROM [//t] where uint64(a) > 1 and uint64(a) < 9", split, source, ResultMatcher(result), 3);

    SUCCEED();
}

TEST_F(TQueryEvaluateTest, OutputRowLimit)
{
    auto split = MakeSplit({
        {"a", EValueType::Int64},
        {"b", EValueType::Int64}
    });

    std::vector<TString> source = {
        "a=1;b=10",
        "a=2;b=20",
        "a=3;b=30",
        "a=4;b=40",
        "a=5;b=50",
        "a=6;b=60",
        "a=7;b=70",
        "a=8;b=80",
        "a=9;b=90"
    };

    auto result = YsonToRows({
        "a=2;b=20",
        "a=3;b=30",
        "a=4;b=40"
    }, split);

    Evaluate("a, b FROM [//t] where a > 1 and a < 9", split, source, ResultMatcher(result), std::numeric_limits<i64>::max(), 3);

    SUCCEED();
}

TEST_F(TQueryEvaluateTest, OutputRowLimit2)
{
    auto split = MakeSplit({});

    std::vector<TString> source;
    for (size_t i = 0; i < 10000; ++i) {
        source.push_back(TString());
    }

    auto resultSplit = MakeSplit({
        {"x", EValueType::Int64}
    });

    std::vector<TOwningRow> result;
    result.push_back(YsonToRow(TString() + "x=" + ToString(10000), resultSplit, false));

    Evaluate("sum(1) as x FROM [//t] group by 0 as q", split, source, ResultMatcher(result), std::numeric_limits<i64>::max(),
             100);

    SUCCEED();
}

TEST_F(TQueryEvaluateTest, OutputRowLimit3)
{
    auto split = MakeSplit({
        {"a", EValueType::Int64}
    });

    std::vector<TString> source;
    for (size_t i = 0; i < 20; ++i) {
        source.push_back(Format("a=%v", i));
    }

    auto resultSplit = MakeSplit({
        {"a", EValueType::Int64}
    });

    std::vector<TOwningRow> result;
    for (size_t i = 0; i < 10; ++i) {
        result.push_back(YsonToRow(Format("a=%v", i), resultSplit, false));
    }

    Evaluate("a FROM [//t] group by a", split, source, ResultMatcher(result), std::numeric_limits<i64>::max(),
             10);

    SUCCEED();
}

TEST_F(TQueryEvaluateTest, TypeInference)
{
    auto split = MakeSplit({
        {"a", EValueType::Int64},
        {"b", EValueType::Int64}
    });

    std::vector<TString> source = {
        "a=1;b=10",
        "a=2;b=20",
        "a=3;b=30",
        "a=4;b=40",
        "a=5;b=50",
        "a=6;b=60",
        "a=7;b=70",
        "a=8;b=80",
        "a=9;b=90"
    };

    auto resultSplit = MakeSplit({
        {"x", EValueType::String},
        {"t", EValueType::Double}
    });

    auto result = YsonToRows({
        "x=b;t=251.",
        "x=a;t=201."
    }, resultSplit);

    Evaluate("if(int64(q) = 4, \"a\", \"b\") as x, double(sum(uint64(b) * 1)) + 1 as t FROM [//t] group by if"
                 "(a % 2 = 0, double(4), 5) as q", split, source, ResultMatcher(result));

    SUCCEED();
}

TEST_F(TQueryEvaluateTest, JoinEmpty)
{
    std::map<TString, TDataSplit> splits;
    std::vector<std::vector<TString>> sources;

    auto leftSplit = MakeSplit({
        {"a", EValueType::Int64},
        {"b", EValueType::Int64}
    }, 0);

    splits["//left"] = leftSplit;
    sources.push_back({
        "a=1;b=10",
        "a=3;b=30",
        "a=5;b=50",
        "a=7;b=70",
        "a=9;b=90"
    });

    auto rightSplit = MakeSplit({
        {"b", EValueType::Int64},
        {"c", EValueType::Int64}
    }, 1);

    splits["//right"] = rightSplit;
    sources.push_back({
        "c=2;b=20",
        "c=4;b=40",
        "c=6;b=60",
        "c=8;b=80"
    });

    auto resultSplit = MakeSplit({
        {"x", EValueType::Int64},
        {"y", EValueType::Int64},
        {"z", EValueType::Int64}
    });

    auto result = YsonToRows({ }, resultSplit);

    Evaluate("sum(a) as x, sum(b) as y, z FROM [//left] join [//right] using b group by c % 2 as z", splits, sources, ResultMatcher(result));

    SUCCEED();
}

TEST_F(TQueryEvaluateTest, JoinSimple2)
{
    std::map<TString, TDataSplit> splits;
    std::vector<std::vector<TString>> sources;

    auto leftSplit = MakeSplit({
        {"a", EValueType::Int64}
    }, 0);

    splits["//left"] = leftSplit;
    sources.push_back({
        "a=1",
        "a=2"
    });

    auto rightSplit = MakeSplit({
        {"a", EValueType::Int64}
    }, 1);

    splits["//right"] = rightSplit;
    sources.push_back({
        "a=2",
        "a=1"
    });

    auto resultSplit = MakeSplit({
        {"x", EValueType::Int64}
    });

    auto result = YsonToRows({
        "x=1",
        "x=2"
    }, resultSplit);

    Evaluate("a as x FROM [//left] join [//right] using a", splits, sources,
             OrderedResultMatcher(result, {"x"}));

    SUCCEED();
}

TEST_F(TQueryEvaluateTest, JoinSimple3)
{
    std::map<TString, TDataSplit> splits;
    std::vector<std::vector<TString>> sources;

    auto leftSplit = MakeSplit({
        {"a", EValueType::Int64}
    }, 0);

    splits["//left"] = leftSplit;
    sources.push_back({
        "a=1",
        "a=1"
    });

    auto rightSplit = MakeSplit({
        {"a", EValueType::Int64}
    }, 1);

    splits["//right"] = rightSplit;
    sources.push_back({
        "a=2",
        "a=1"
    });

    auto resultSplit = MakeSplit({
        {"x", EValueType::Int64}
    });

    auto result = YsonToRows({
        "x=1",
        "x=1"
    }, resultSplit);

    Evaluate("a as x FROM [//left] join [//right] using a", splits, sources, ResultMatcher(result));

    SUCCEED();
}

TEST_F(TQueryEvaluateTest, JoinSimple4)
{
    std::map<TString, TDataSplit> splits;
    std::vector<std::vector<TString>> sources;

    auto leftSplit = MakeSplit({
        {"a", EValueType::Int64}
    }, 0);

    splits["//left"] = leftSplit;
    sources.push_back({
        "a=1",
        "a=2"
    });

    auto rightSplit = MakeSplit({
        {"a", EValueType::Int64}
    }, 1);

    splits["//right"] = rightSplit;
    sources.push_back({
        "a=1",
        "a=1"
    });

    auto resultSplit = MakeSplit({
        {"x", EValueType::Int64}
    });

    auto result = YsonToRows({
        "x=1",
        "x=1"
    }, resultSplit);

    Evaluate("a as x FROM [//left] join [//right] using a", splits, sources, ResultMatcher(result));

    SUCCEED();
}

TEST_F(TQueryEvaluateTest, JoinSimple5)
{
    std::map<TString, TDataSplit> splits;
    std::vector<std::vector<TString>> sources;

    auto leftSplit = MakeSplit({
        {"a", EValueType::Int64}
    }, 0);

    splits["//left"] = leftSplit;
    sources.push_back({
        "a=1",
        "a=1",
        "a=1"
    });

    auto rightSplit = MakeSplit({
        {"a", EValueType::Int64}
    }, 1);

    splits["//right"] = rightSplit;
    sources.push_back({
        "a=1",
        "a=1",
        "a=1"
    });

    auto resultSplit = MakeSplit({
        {"x", EValueType::Int64}
    });

    auto result = YsonToRows({
        "x=1",
        "x=1",
        "x=1",
        "x=1",
        "x=1",
        "x=1",
        "x=1",
        "x=1",
        "x=1"
    }, resultSplit);

    Evaluate("a as x FROM [//left] join [//right] using a", splits, sources, ResultMatcher(result));

    SUCCEED();
}

TEST_F(TQueryEvaluateTest, JoinRowLimit)
{
    std::map<TString, TDataSplit> splits;
    std::vector<std::vector<TString>> sources;

    auto leftSplit = MakeSplit({
        {"a", EValueType::Int64}
    }, 0);

    splits["//left"] = leftSplit;
    sources.push_back({
        "a=1",
        "a=2",
        "a=3",
        "a=4",
        "a=5"
    });

    auto rightSplit = MakeSplit({
        {"a", EValueType::Int64}
    }, 1);

    splits["//right"] = rightSplit;
    sources.push_back({
        "a=2",
        "a=3",
        "a=4",
        "a=5",
        "a=6"
    });

    auto resultSplit = MakeSplit({
        {"x", EValueType::Int64}
    });

    auto result = YsonToRows({
        "x=2",
        "x=3",
        "x=4",
    }, resultSplit);

    Evaluate(
        "a as x FROM [//left] join [//right] using a",
        splits,
        sources,
        ResultMatcher(result),
        std::numeric_limits<i64>::max(), 4);

    SUCCEED();
}

TEST_F(TQueryEvaluateTest, JoinRowLimit2)
{
    std::map<TString, TDataSplit> splits;
    std::vector<std::vector<TString>> sources;

    auto leftSplit = MakeSplit({
        {"a", EValueType::Int64}
    }, 0);

    splits["//left"] = leftSplit;
    sources.push_back({
        "a=1",
        "a=1"
    });

    auto rightSplit = MakeSplit({
        {"a", EValueType::Int64}
    }, 1);

    splits["//right"] = rightSplit;
    sources.push_back({
        "a=1",
        "a=1",
        "a=1",
    });

    auto resultSplit = MakeSplit({
        {"x", EValueType::Int64}
    });

    auto result = YsonToRows({
        "x=1",
        "x=1",
        "x=1",
        "x=1",
        "x=1"
    }, resultSplit);

    Evaluate(
        "a as x FROM [//left] join [//right] using a",
        splits,
        sources,
        ResultMatcher(result),
        std::numeric_limits<i64>::max(), 5);

    SUCCEED();
}

TEST_F(TQueryEvaluateTest, JoinWithLimit)
{
    std::map<TString, TDataSplit> splits;
    std::vector<std::vector<TString>> sources;

    auto leftSplit = MakeSplit({
        {"a", EValueType::Int64}
    }, 0);

    splits["//left"] = leftSplit;
    sources.push_back({
        "a=1",
        "a=2",
        "a=3",
        "a=4",
        "a=5",
        "a=6",
        "a=7"
    });

    auto rightSplit = MakeSplit({
        {"a", EValueType::Int64}
    }, 1);

    splits["//right"] = rightSplit;
    sources.push_back({
        "a=7",
        "a=5",
        "a=3",
        "a=1"
    });

    auto resultSplit = MakeSplit({
        {"x", EValueType::Int64}
    });

    auto result = YsonToRows({
        "x=1",
        "x=3"
    }, resultSplit);

    Evaluate(
        "a as x FROM [//left] join [//right] using a",
        splits,
        sources,
        OrderedResultMatcher(result, {"x"}),
        std::numeric_limits<i64>::max(), 4);

    result = YsonToRows({
        "x=1",
        "x=3",
        "x=5",
        "x=7"
    }, resultSplit);

    Evaluate(
        "a as x FROM [//left] join [//right] using a limit 4",
        splits,
        sources,
        ResultMatcher(result));

    SUCCEED();
}

TEST_F(TQueryEvaluateTest, JoinWithLimit2)
{
    std::map<TString, TDataSplit> splits;
    std::vector<std::vector<TString>> sources;

    auto leftSplit = MakeSplit({
        {"a", EValueType::Int64, ESortOrder::Ascending},
        {"ut", EValueType::Int64, ESortOrder::Ascending},
        {"b", EValueType::Int64, ESortOrder::Ascending},
        {"v", EValueType::Int64}
    }, 0);

    splits["//left"] = leftSplit;
    sources.push_back({
        "a=1;ut=1;b=30",
        "a=1;ut=2;b=20",
        "a=2;ut=3;b=10",
        "a=2;ut=4;b=30",
        "a=3;ut=5;b=20",
        "a=4;ut=6;b=10"
    });

    auto rightSplit = MakeSplit({
        {"b", EValueType::Int64, ESortOrder::Ascending},
        {"c", EValueType::Int64}
    }, 1);

    splits["//right"] = rightSplit;
    sources.push_back({
        "b=10;c=100",
        "b=20;c=200",
        "b=30;c=300"
    });

    auto resultSplit = MakeSplit({
        {"a.ut", EValueType::Int64},
        {"b.c", EValueType::Int64},
        {"a.b", EValueType::Int64},
        {"b.b", EValueType::Int64}
    });

    auto result = YsonToRows({
        "\"a.ut\"=1;\"b.c\"=300;\"a.b\"=30;\"b.b\"=30",
        "\"a.ut\"=2;\"b.c\"=200;\"a.b\"=20;\"b.b\"=20",
        "\"a.ut\"=3;\"b.c\"=100;\"a.b\"=10;\"b.b\"=10",
        "\"a.ut\"=4;\"b.c\"=300;\"a.b\"=30;\"b.b\"=30",
        "\"a.ut\"=5;\"b.c\"=200;\"a.b\"=20;\"b.b\"=20",
        "\"a.ut\"=6;\"b.c\"=100;\"a.b\"=10;\"b.b\"=10"

    }, resultSplit);

    for (size_t limit = 1; limit <= 6; ++limit) {
        std::vector<TOwningRow> currentResult(result.begin(), result.begin() + limit);
        Evaluate(
            Format("a.ut, b.c, a.b, b.b FROM [//left] a join [//right] b on a.b = b.b limit %v", limit),
            splits,
            sources,
            ResultMatcher(currentResult));
    }

    SUCCEED();
}

TEST_F(TQueryEvaluateTest, JoinWithLimit3)
{
    std::map<TString, TDataSplit> splits;
    std::vector<std::vector<TString>> sources;

    auto leftSplit = MakeSplit({
        {"publisherId", EValueType::String, ESortOrder::Ascending},
        {"itemId", EValueType::Int64}
    }, 0);

    splits["//publishers"] = leftSplit;
    sources.push_back({
        "publisherId=\"5903739ad7d0a6e07ad1fb93\";itemId=3796616032221332447",
        "publisherId=\"5908961de3cda81ba288b664\";itemId=847311080463071787",
        "publisherId=\"5909bd2dd7d0a68351e66077\";itemId=-3463642079005455542",
        "publisherId=\"5912f1e27ddde8c264b56f0c\";itemId=2859920047593648390",
        "publisherId=\"5912f1f88e557d5b22ff7077\";itemId=-5478070133262294529",
        "publisherId=\"591446067ddde805266009b5\";itemId=-5089939500492155348",
        "publisherId=\"591464507ddde805266009b8\";itemId=3846436484159153735",
        "publisherId=\"591468bce3cda8db9996fa89\";itemId=2341245309580180142",
        "publisherId=\"5914c6678e557dcf3bf713cf\";itemId=-6844788529441593571",
        "publisherId=\"5915869a7ddde805266009bb\";itemId=-6883609521883689",
        "publisherId=\"5918c7f8e3cda83873187c37\";itemId=-896633843529240754",
        "publisherId=\"591939f67ddde8632415d4ce\";itemId=-2679935711711852631",
        "publisherId=\"59195b327ddde8632415d4d1\";itemId=8410938732504570842"
    });

    auto rightSplit = MakeSplit({
        {"publisherId", EValueType::String, ESortOrder::Ascending},
        {"timestamp", EValueType::Uint64}
    }, 1);

    splits["//draft"] = rightSplit;
    sources.push_back({
        "publisherId=\"591446067ddde805266009b5\";timestamp=1504706169u",
        "publisherId=\"591468bce3cda8db9996fa89\";timestamp=1504706172u",
        "publisherId=\"5914c6678e557dcf3bf713cf\";timestamp=1504706178u",
        "publisherId=\"5918c7f8e3cda83873187c37\";timestamp=1504706175u",
    });

    auto resultSplit = MakeSplit({
        {"publisherId", EValueType::String}
    });

    auto result = YsonToRows({
        "publisherId=\"5903739ad7d0a6e07ad1fb93\"",
        "publisherId=\"5908961de3cda81ba288b664\"",
        "publisherId=\"5909bd2dd7d0a68351e66077\"",
        "publisherId=\"5912f1e27ddde8c264b56f0c\"",
        "publisherId=\"5912f1f88e557d5b22ff7077\"",
        "publisherId=\"591446067ddde805266009b5\"",
        "publisherId=\"591464507ddde805266009b8\"",
        "publisherId=\"591468bce3cda8db9996fa89\"",
        "publisherId=\"5914c6678e557dcf3bf713cf\"",
        "publisherId=\"5915869a7ddde805266009bb\"",
        "publisherId=\"5918c7f8e3cda83873187c37\"",
        "publisherId=\"591939f67ddde8632415d4ce\"",
        "publisherId=\"59195b327ddde8632415d4d1\""
    }, resultSplit);

    for (size_t limit = 1; limit <= 13; ++limit) {
        std::vector<TOwningRow> currentResult(result.begin(), result.begin() + limit);
        Evaluate(
            Format("[publisherId] FROM [//publishers] LEFT JOIN [//draft] USING [publisherId] LIMIT %v", limit),
            splits,
            sources,
            ResultMatcher(currentResult));
    }

    SUCCEED();
}

TEST_F(TQueryEvaluateTest, JoinNonPrefixColumns)
{
    std::map<TString, TDataSplit> splits;
    std::vector<std::vector<TString>> sources;

    auto leftSplit = MakeSplit({
        TColumnSchema("x", EValueType::String).SetSortOrder(ESortOrder::Ascending),
        TColumnSchema("y", EValueType::String)
    }, 0);

    splits["//left"] = leftSplit;
    sources.push_back({
        "x=a",
        "x=b",
        "x=c"
    });

    auto rightSplit = MakeSplit({
        TColumnSchema("a", EValueType::Int64).SetSortOrder(ESortOrder::Ascending),
        TColumnSchema("x", EValueType::String)
    }, 1);

    splits["//right"] = rightSplit;
    sources.push_back({
        "a=1;x=a",
        "a=2;x=b",
        "a=3;x=c"
    });

    auto resultSplit = MakeSplit({
        {"x", EValueType::String},
        {"a", EValueType::Int64},
        {"y", EValueType::String}
    });

    auto result = YsonToRows({
        "a=1;x=a",
        "a=2;x=b",
        "a=3;x=c"
    }, resultSplit);

    Evaluate("x, a, y FROM [//left] join [//right] using x", splits, sources,
             OrderedResultMatcher(result, {"a"}));

    SUCCEED();
}

TEST_F(TQueryEvaluateTest, JoinManySimple)
{
    std::map<TString, TDataSplit> splits;
    std::vector<std::vector<TString>> sources;

    splits["//a"] = MakeSplit({
        {"a", EValueType::Int64},
        {"c", EValueType::String}
    }, 0);
    sources.push_back({
        "a=2;c=b",
        "a=3;c=c",
        "a=4;c=a"
    });

    splits["//b"] = MakeSplit({
        {"b", EValueType::Int64},
        {"c", EValueType::String},
        {"d", EValueType::String}
    }, 1);
    sources.push_back({
        "b=100;c=a;d=X",
        "b=200;c=b;d=Y",
        "b=300;c=c;d=X",
        "b=400;c=a;d=Y",
        "b=500;c=b;d=X",
        "b=600;c=c;d=Y"
    });

    splits["//c"] = MakeSplit({
        {"d", EValueType::String},
        {"e", EValueType::Int64},
    }, 2);
    sources.push_back({
        "d=X;e=1234",
        "d=Y;e=5678"
    });


    auto resultSplit = MakeSplit({
        {"a", EValueType::Int64},
        {"c", EValueType::String},
        {"b", EValueType::Int64},
        {"d", EValueType::String},
        {"e", EValueType::Int64}
    });

    auto result = YsonToRows({
         "a=2;c=b;b=200;d=Y;e=5678",
         "a=2;c=b;b=500;d=X;e=1234",
         "a=3;c=c;b=300;d=X;e=1234",
         "a=3;c=c;b=600;d=Y;e=5678",
         "a=4;c=a;b=100;d=X;e=1234",
         "a=4;c=a;b=400;d=Y;e=5678"
    }, resultSplit);

    Evaluate(
        "a, c, b, d, e from [//a] join [//b] using c join [//c] using d",
        splits,
        sources,
        OrderedResultMatcher(result, {"a", "b"}));

    SUCCEED();
}

TEST_F(TQueryEvaluateTest, Multijoin)
{
    std::map<TString, TDataSplit> splits;
    std::vector<std::vector<TString>> sources;

    splits["//x"] = MakeSplit({
        {"a", EValueType::Int64}
    }, 0);
    sources.push_back({
        "a=0"
    });

    splits["//y"] = MakeSplit({
        {"a", EValueType::Int64},
        {"b", EValueType::Int64}
    }, 1);
    sources.push_back({
        "a=0;b=1",
        "a=0;b=2"
    });

    splits["//z"] = MakeSplit({
        {"a", EValueType::Int64},
        {"c", EValueType::Int64},
    }, 2);
    sources.push_back({
        "a=0;c=1",
        "a=0;c=2",
        "a=0;c=3"
    });

    splits["//q"] = MakeSplit({
        {"a", EValueType::Int64},
        {"d", EValueType::Int64},
    }, 2);
    sources.push_back({ });

    auto resultSplit = MakeSplit({
        {"a", EValueType::Int64},
        {"b", EValueType::Int64},
        {"c", EValueType::Int64}
    });

    auto result = YsonToRows({
         "a=0;b=1;c=1",
         "a=0;b=2;c=1",
         "a=0;b=1;c=2",
         "a=0;b=2;c=2",
         "a=0;b=1;c=3",
         "a=0;b=2;c=3"
    }, resultSplit);

    Evaluate(
        "a, b, c from [//x] join [//y] using a join [//z] using a left join [//q] using a",
        splits,
        sources,
        OrderedResultMatcher(result, {"c", "b"}));

    SUCCEED();
}

TEST_F(TQueryEvaluateTest, SortMergeJoin)
{
    std::map<TString, TDataSplit> splits;
    std::vector<std::vector<TString>> sources;

    auto leftSplit = MakeSplit({
        {"a", EValueType::Int64, ESortOrder::Ascending},
        {"b", EValueType::Int64}
    }, 0);

    splits["//left"] = leftSplit;
    sources.push_back({
        "a=1;b=10",
        "a=3;b=30",
        "a=5;b=50",
        "a=7;b=70",
        "a=9;b=90"
    });

    auto rightSplit = MakeSplit({
        {"c", EValueType::Int64, ESortOrder::Ascending},
        {"d", EValueType::Int64}
    }, 1);

    splits["//right"] = rightSplit;
    sources.push_back({
        "c=1;d=10",
        "c=2;d=20",
        "c=4;d=40",
        "c=5;d=50",
        "c=7;d=70",
        "c=8;d=80"
    });

    auto resultSplit = MakeSplit({
        {"a", EValueType::Int64},
        {"b", EValueType::Int64},
        {"d", EValueType::Int64}
    });

    auto result = YsonToRows({
        "a=1;b=10;d=10",
        "a=5;b=50;d=50",
        "a=7;b=70;d=70"
    }, resultSplit);

    auto query = Evaluate("a, b, d FROM [//left] join [//right] on a = c", splits, sources, ResultMatcher(result));

    EXPECT_EQ(query->JoinClauses.size(), 1u);
    const auto& joinClauses = query->JoinClauses;

    EXPECT_EQ(joinClauses[0]->ForeignKeyPrefix, 1u);
    EXPECT_EQ(joinClauses[0]->CommonKeyPrefix, 1u);

    SUCCEED();
}

TEST_F(TQueryEvaluateTest, PartialSortMergeJoin)
{
    std::map<TString, TDataSplit> splits;
    std::vector<std::vector<TString>> sources;

    auto leftSplit = MakeSplit({
        {"a", EValueType::Int64, ESortOrder::Ascending},
        {"b", EValueType::Int64},
        {"c", EValueType::Int64},
    }, 0);

    splits["//left"] = leftSplit;
    sources.push_back({
        "a=1;b=2;c=1 ",
        "a=1;b=3;c=2 ",
        "a=1;b=1;c=3 ",
        "a=1;b=4;c=4 ",
        "a=2;b=4;c=5 ",
        "a=2;b=3;c=6 ",
        "a=2;b=1;c=7 ",
        "a=2;b=2;c=8 ",
        "a=3;b=1;c=9 ",
        "a=3;b=4;c=10",
        "a=3;b=3;c=11",
        "a=3;b=2;c=12",
        "a=4;b=8;c=13",
        "a=4;b=7;c=14"
    });

    auto rightSplit = MakeSplit({
        {"d", EValueType::Int64, ESortOrder::Ascending},
        {"e", EValueType::Int64, ESortOrder::Ascending},
        {"f", EValueType::Int64},
    }, 1);

    splits["//right"] = rightSplit;
    sources.push_back({
        "d=1;e=1;f=3 ",
        "d=1;e=2;f=1 ",
        "d=1;e=3;f=2 ",
        "d=1;e=4;f=4 ",
        "d=2;e=1;f=7 ",
        "d=2;e=2;f=8 ",
        "d=2;e=3;f=6 ",
        "d=2;e=4;f=5 ",
        "d=3;e=1;f=9 ",
        "d=3;e=2;f=12",
        "d=3;e=3;f=11",
        "d=3;e=4;f=10",
        "d=4;e=7;f=14",
        "d=4;e=8;f=13",

    });

    auto resultSplit = MakeSplit({
        {"a", EValueType::Int64},
        {"b", EValueType::Int64},
        {"c", EValueType::Int64},
        {"d", EValueType::Int64},
        {"e", EValueType::Int64},
        {"f", EValueType::Int64}
    });

    auto result = YsonToRows({
        "a=1;b=2;c=1 ;d=1;e=2;f=1 ",
        "a=1;b=3;c=2 ;d=1;e=3;f=2 ",
        "a=1;b=1;c=3 ;d=1;e=1;f=3 ",
        "a=1;b=4;c=4 ;d=1;e=4;f=4 ",
        "a=2;b=4;c=5 ;d=2;e=4;f=5 ",
        "a=2;b=3;c=6 ;d=2;e=3;f=6 ",
        "a=2;b=1;c=7 ;d=2;e=1;f=7 ",
        "a=2;b=2;c=8 ;d=2;e=2;f=8 ",
        "a=3;b=1;c=9 ;d=3;e=1;f=9 ",
        "a=3;b=4;c=10;d=3;e=4;f=10",
        "a=3;b=3;c=11;d=3;e=3;f=11",
        "a=3;b=2;c=12;d=3;e=2;f=12",
        "a=4;b=8;c=13;d=4;e=8;f=13",
        "a=4;b=7;c=14;d=4;e=7;f=14",
    }, resultSplit);

    {
        auto query = Evaluate("a, b, c, d, e, f FROM [//left] join [//right] on (a, b) = (d, e)",
            splits,
            sources,
            OrderedResultMatcher(result, {"c"}));

        EXPECT_EQ(query->JoinClauses.size(), 1u);
        const auto& joinClauses = query->JoinClauses;

        EXPECT_EQ(joinClauses[0]->ForeignKeyPrefix, 2u);
        EXPECT_EQ(joinClauses[0]->CommonKeyPrefix, 1u);
    }

    {
        auto rightSplit = MakeSplit({
            {"d", EValueType::Int64, ESortOrder::Ascending},
            {"e", EValueType::Int64},
            {"f", EValueType::Int64},
        }, 1);
        splits["//right"] = rightSplit;
        sources[1] = {
            "d=1;e=4;f=4 ",
            "d=1;e=1;f=3 ",
            "d=1;e=3;f=2 ",
            "d=1;e=2;f=1 ",
            "d=2;e=2;f=8 ",
            "d=2;e=4;f=5 ",
            "d=2;e=1;f=7 ",
            "d=2;e=3;f=6 ",
            "d=3;e=2;f=12",
            "d=3;e=3;f=11",
            "d=3;e=4;f=10",
            "d=3;e=1;f=9 ",
            "d=4;e=7;f=14",
            "d=4;e=8;f=13"
        };

        auto query = Evaluate("a, b, c, d, e, f FROM [//left] join [//right] on (a, b) = (d, e)",
            splits,
            sources,
            OrderedResultMatcher(result, {"c"}));

        EXPECT_EQ(query->JoinClauses.size(), 1u);
        const auto& joinClauses = query->JoinClauses;

        EXPECT_EQ(joinClauses[0]->ForeignKeyPrefix, 1u);
        EXPECT_EQ(joinClauses[0]->CommonKeyPrefix, 1u);
    }

    SUCCEED();
}

TEST_F(TQueryEvaluateTest, Join)
{
    std::map<TString, TDataSplit> splits;
    std::vector<std::vector<TString>> sources;

    auto leftSplit = MakeSplit({
        {"a", EValueType::Int64},
        {"b", EValueType::Int64}
    }, 0);

    splits["//left"] = leftSplit;
    sources.push_back({
        "a=1;b=10",
        "a=2;b=20",
        "a=3;b=30",
        "a=4;b=40",
        "a=5;b=50",
        "a=6;b=60",
        "a=7;b=70",
        "a=8;b=80",
        "a=9;b=90"
    });

    auto rightSplit = MakeSplit({
        {"b", EValueType::Int64},
        {"c", EValueType::Int64}
    }, 1);

    splits["//right"] = rightSplit;
    sources.push_back({
        "c=1;b=10",
        "c=2;b=20",
        "c=3;b=30",
        "c=4;b=40",
        "c=5;b=50",
        "c=6;b=60",
        "c=7;b=70",
        "c=8;b=80",
        "c=9;b=90"
    });

    auto resultSplit = MakeSplit({
        {"x", EValueType::Int64},
        {"z", EValueType::Int64}
    });

    auto result = YsonToRows({
        "x=25;z=1",
        "x=20;z=0",
    }, resultSplit);

    Evaluate("sum(a) as x, z FROM [//left] join [//right] using b group by c % 2 as z", splits, sources, ResultMatcher(result));
    Evaluate("sum(a) as x, z FROM [//left] join [//right] on b = b group by c % 2 as z", splits, sources, ResultMatcher(result));
    Evaluate("sum(l.a) as x, z FROM [//left] as l join [//right] as r on l.b = r.b group by r.c % 2 as z", splits, sources, ResultMatcher(result));

    SUCCEED();
}

TEST_F(TQueryEvaluateTest, LeftJoin)
{
    std::map<TString, TDataSplit> splits;
    std::vector<std::vector<TString>> sources;

    auto leftSplit = MakeSplit({
        {"a", EValueType::Int64},
        {"b", EValueType::Int64}
    }, 0);

    splits["//left"] = leftSplit;
    sources.push_back({
        "a=1;b=10",
        "a=2;b=20",
        "a=3;b=30",
        "a=4;b=40",
        "a=5;b=50",
        "a=6;b=60",
        "a=7;b=70",
        "a=8;b=80",
        "a=9;b=90"
    });

    auto rightSplit = MakeSplit({
        {"b", EValueType::Int64},
        {"c", EValueType::Int64}
    }, 1);

    splits["//right"] = rightSplit;
    sources.push_back({
        "c=1;b=10",
        "c=3;b=30",
        "c=5;b=50",
        "c=8;b=80",
        "c=9;b=90"
    });

    auto resultSplit = MakeSplit({
        {"a", EValueType::Int64},
        {"b", EValueType::Int64},
        {"c", EValueType::Int64}
    });

    auto result = YsonToRows({
        "a=1;b=10;c=1",
        "a=2;b=20",
        "a=3;b=30;c=3",
        "a=4;b=40",
        "a=5;b=50;c=5",
        "a=6;b=60",
        "a=7;b=70",
        "a=8;b=80;c=8",
        "a=9;b=90;c=9"
    }, resultSplit);

    Evaluate(
        "a, b, c FROM [//left] left join [//right] using b",
        splits,
        sources,
        OrderedResultMatcher(result, {"a"}));

    SUCCEED();
}

TEST_F(TQueryEvaluateTest, LeftJoinWithCondition)
{
    std::map<TString, TDataSplit> splits;
    std::vector<std::vector<TString>> sources;

    auto leftSplit = MakeSplit({
        {"a", EValueType::Int64}
    }, 0);

    splits["//left"] = leftSplit;
    sources.push_back({
        "a=1",
        "a=2",
        "a=3",
        "a=4"
    });

    auto rightSplit = MakeSplit({
        {"a", EValueType::Int64},
        {"b", EValueType::Int64},
        {"c", EValueType::Int64},
    }, 1);

    splits["//right"] = rightSplit;
    sources.push_back({
        "a=1;b=1;c=1",
        "a=1;b=2;c=1",
        "a=1;b=3;c=1",
        "a=2;b=1;c=1",
        "a=2;b=3;c=1",
        "a=3;b=1;c=1"
    });

    auto resultSplit = MakeSplit({
        {"a", EValueType::Int64},
        {"s", EValueType::Int64}
    });

    auto result = YsonToRows({
        "a=1;s=1",
        "a=4"
    }, resultSplit);

    Evaluate(
        "a, sum(c) as s FROM [//left] left join [//right] using a where b = 2 or b = # group by a",
        splits,
        sources,
        OrderedResultMatcher(result, {"a"}));

    auto result2 = YsonToRows({
        "a=1;s=1",
        "a=2",
        "a=3",
        "a=4"
    }, resultSplit);

    Evaluate(
        "a, sum(c) as s FROM [//left] left join [//right] using a and b = 2 group by a",
        splits,
        sources,
        OrderedResultMatcher(result2, {"a"}));

    SUCCEED();
}

TEST_F(TQueryEvaluateTest, ComplexAlias)
{
    auto split = MakeSplit({
        {"a", EValueType::Int64},
        {"s", EValueType::String}
    });

    std::vector<TString> source = {
        "a=10;s=x",
        "a=20;s=y",
        "a=30;s=x",
        "a=40;s=x",
        "a=42",
        "a=50;s=x",
        "a=60;s=y",
        "a=70;s=z",
        "a=72",
        "a=80;s=y",
        "a=85",
        "a=90;s=z"
    };

    auto resultSplit = MakeSplit({
        {"x", EValueType::String},
        {"t", EValueType::Int64}
    });

    auto result = YsonToRows({
        "x=y;t=160",
        "x=x;t=120",
        "t=199",
        "x=z;t=160"
    }, resultSplit);

    Evaluate("x, sum(p.a) as t FROM [//t] as p where p.a > 10 group by p.s as x", split, source, ResultMatcher(result));

    SUCCEED();
}

TEST_F(TQueryEvaluateTest, JoinMany)
{
    std::map<TString, TDataSplit> splits;
    std::vector<std::vector<TString>> sources;

    splits["//primary"] = MakeSplit({
        {"a", EValueType::Int64},
        {"b", EValueType::Int64}
    }, 0);
    sources.push_back({
        "a=1;b=10",
        "a=2;b=20",
        "a=3;b=30",
        "a=4;b=40",
        "a=5;b=50",
        "a=6;b=60",
        "a=7;b=70",
        "a=8;b=80",
        "a=9;b=90"
    });

    splits["//secondary"] = MakeSplit({
        {"b", EValueType::Int64},
        {"c", EValueType::Int64}
    }, 1);
    sources.push_back({
        "c=1;b=10",
        "c=2;b=20",
        "c=3;b=30",
        "c=4;b=40",
        "c=5;b=50",
        "c=6;b=60",
        "c=7;b=70",
        "c=8;b=80",
        "c=9;b=90"
    });

    splits["//tertiary"] = MakeSplit({
        {"c", EValueType::Int64},
        {"d", EValueType::Int64}
    }, 2);
    sources.push_back({
        "c=1;d=10",
        "c=2;d=20",
        "c=3;d=30",
        "c=4;d=40",
        "c=5;d=50",
        "c=6;d=60",
        "c=7;d=70",
        "c=8;d=80",
        "c=9;d=90"
    });


    auto resultSplit = MakeSplit({
        {"x", EValueType::Int64},
        {"y", EValueType::Int64},
        {"z", EValueType::Int64}
    });

    auto result = YsonToRows({
        "x=20;y=200;z=0",
        "x=25;y=250;z=1"
    }, resultSplit);

    Evaluate(
        "sum(a) as x, sum(d) as y, z FROM [//primary] join [//secondary] using b join [//tertiary] using c group by c % 2 as z",
        splits,
        sources,
        OrderedResultMatcher(result, {"x"}));

    SUCCEED();
}

// Tests common subexpressions in join equations.
TEST_F(TQueryEvaluateTest, TwoLeftJoinOneToMany)
{
    std::map<TString, TDataSplit> splits;

    {
        TDataSplit dataSplit;

        SetObjectId(&dataSplit, MakeId(EObjectType::Table, 0x42, 0, 0xdeadbabe));

        auto schema = New<TTableSchema>(std::vector{
            TColumnSchema("cid", EValueType::Int64)
                .SetSortOrder(ESortOrder::Ascending),
            TColumnSchema("pid", EValueType::Int64)
                .SetSortOrder(ESortOrder::Ascending),
            TColumnSchema("value", EValueType::Int64),
        });

        dataSplit.TableSchema = schema;

        splits["//phrases"] = dataSplit;
    }

    std::vector<TString> phrases = {
        "cid=49353617;pid=4098243503"
    };

    {
        TDataSplit dataSplit;

        SetObjectId(&dataSplit, MakeId(EObjectType::Table, 0x42, 1, 0xdeadbabe));

        auto schema = New<TTableSchema>(std::vector{
            TColumnSchema("__hash__", EValueType::Int64)
                .SetSortOrder(ESortOrder::Ascending)
                .SetExpression(TString("int64(farm_hash(pid) % 64)")),
            TColumnSchema("pid", EValueType::Int64)
                .SetSortOrder(ESortOrder::Ascending),
            TColumnSchema("tag_id", EValueType::Int64)
                .SetSortOrder(ESortOrder::Ascending),
            TColumnSchema("value", EValueType::Int64),
        });

        dataSplit.TableSchema = schema;

        splits["//tag_group"] = dataSplit;
    }

    std::vector<TString> tag_group = {
        "__hash__=13;pid=4098243503;tag_id=39139420",
        "__hash__=13;pid=4098243503;tag_id=39139421"
    };

    {
        TDataSplit dataSplit;

        SetObjectId(&dataSplit, MakeId(EObjectType::Table, 0x42, 2, 0xdeadbabe));

        auto schema = New<TTableSchema>(std::vector{
            TColumnSchema("YTHash", EValueType::Int64)
                .SetSortOrder(ESortOrder::Ascending)
                .SetExpression(TString("int64(farm_hash(ExportID))")),
            TColumnSchema("ExportID", EValueType::Int64)
                .SetSortOrder(ESortOrder::Ascending),
            TColumnSchema("GroupExportID", EValueType::Int64)
                .SetSortOrder(ESortOrder::Ascending),
            TColumnSchema("UpdateTime", EValueType::Int64)
                .SetSortOrder(ESortOrder::Ascending),
            TColumnSchema("value", EValueType::Int64),
        });

        dataSplit.TableSchema = schema;

        splits["//DirectPhraseStatV2"] = dataSplit;
    }

    std::vector<TString> DirectPhraseStatV2 = {
        "YTHash=-9217565170028083870;ExportID=49353617;GroupExportID=4098243503;UpdateTime=1579813200",
        "YTHash=-9217565170028083870;ExportID=49353617;GroupExportID=4098243503;UpdateTime=1580072400",
        "YTHash=-9217565170028083870;ExportID=49353617;GroupExportID=4098243503;UpdateTime=1580158800"
    };

    auto resultSplit = MakeSplit({
        {"tag_id", EValueType::Int64},
        {"UpdateTime", EValueType::Int64}
    });

    {
        auto result = YsonToRows({
            "tag_id=39139420;UpdateTime=1579813200",
            "tag_id=39139421;UpdateTime=1579813200",
            "tag_id=39139420;UpdateTime=1580072400",
            "tag_id=39139421;UpdateTime=1580072400",
            "tag_id=39139420;UpdateTime=1580158800",
            "tag_id=39139421;UpdateTime=1580158800",
        }, resultSplit);

        Evaluate(R"(
            TG.tag_id as tag_id, S.UpdateTime as UpdateTime
            FROM [//phrases] AS P
            LEFT JOIN [//tag_group] AS TG ON P.pid = TG.pid
            LEFT JOIN [//DirectPhraseStatV2] AS S ON (P.cid, P.pid) = (S.ExportID, S.GroupExportID)
        )", splits, {phrases, tag_group, DirectPhraseStatV2}, ResultMatcher(result));
    }

    {
        auto result = YsonToRows({
            "tag_id=39139420;UpdateTime=1579813200",
            "tag_id=39139420;UpdateTime=1580072400",
            "tag_id=39139420;UpdateTime=1580158800",
            "tag_id=39139421;UpdateTime=1579813200",
            "tag_id=39139421;UpdateTime=1580072400",
            "tag_id=39139421;UpdateTime=1580158800"
        }, resultSplit);

        Evaluate(R"(
            TG.tag_id, S.UpdateTime
            FROM [//phrases] AS P
            LEFT JOIN [//DirectPhraseStatV2] AS S ON (P.cid, P.pid) = (S.ExportID, S.GroupExportID)
            LEFT JOIN [//tag_group] AS TG ON P.pid = TG.pid
        )", splits, {phrases, DirectPhraseStatV2, tag_group}, ResultMatcher(result));
    }
}

TEST_F(TQueryEvaluateTest, OrderBy)
{
    auto split = MakeSplit({
        {"a", EValueType::Int64},
        {"b", EValueType::Int64}
    });

    std::vector<TString> source;

    for (int i = 0; i < 10000; ++i) {
        auto value = std::rand() % 100000 + 10000;
        source.push_back(TString() + "a=" + ToString(value) + ";b=" + ToString(value * 10));
    }

    for (int i = 0; i < 10000; ++i) {
        auto value = 10000 - i;
        source.push_back(TString() + "a=" + ToString(value) + ";b=" + ToString(value * 10));
    }

    std::vector<TOwningRow> result;
    for (const auto& row : source) {
        result.push_back(YsonToRow(row, split, false));
    }

    std::vector<TOwningRow> limitedResult;

    std::sort(result.begin(), result.end());
    limitedResult.assign(result.begin(), result.begin() + 100);
    Evaluate("* FROM [//t] order by a * a limit 100", split, source, ResultMatcher(limitedResult));

    limitedResult.assign(result.begin() + 100, result.begin() + 200);
    Evaluate("* FROM [//t] order by a * a offset 100 limit 100", split, source, ResultMatcher(limitedResult));

    std::reverse(result.begin(), result.end());
    limitedResult.assign(result.begin(), result.begin() + 100);
    Evaluate("* FROM [//t] order by a * 3 - 1 desc limit 100", split, source, ResultMatcher(limitedResult));


    source.clear();
    for (int i = 0; i < 10; ++i) {
        auto value = 10 - i;
        source.push_back(TString() + "a=" + ToString(i % 3) + ";b=" + ToString(value));
    }

    result.clear();
    for (const auto& row : source) {
        result.push_back(YsonToRow(row, split, false));
    }

    EXPECT_THROW_THAT(
        Evaluate("* FROM [//t] order by 0.0 / double(a) limit 100", split, source, ResultMatcher(result)),
        HasSubstr("Comparison with NaN"));

    SUCCEED();
}

TEST_F(TQueryEvaluateTest, GroupByTotalsOrderBy)
{
    auto split = MakeSplit({
        {"a", EValueType::Int64},
        {"b", EValueType::Int64}
    });

    std::vector<std::pair<i64, i64>> sourceValues;
    for (int i = 0; i < 10000; ++i) {
        auto value = std::rand() % 100000 + 10000;
        sourceValues.emplace_back(value, value * 10);
    }

    for (int i = 0; i < 10000; ++i) {
        auto value = 10000 - i;
        sourceValues.emplace_back(value, value * 10);
    }

    std::vector<std::pair<i64, i64>> groupedValues(200, std::make_pair(0, 0));
    i64 totalSum = 0;
    for (const auto& row : sourceValues) {
        i64 x = row.first % 200;
        groupedValues[x].first = x;
        groupedValues[x].second += row.second;
        totalSum += row.second;
    }

    std::sort(
        groupedValues.begin(),
        groupedValues.end(), [] (const std::pair<i64, i64>& lhs, const std::pair<i64, i64>& rhs) {
            return lhs.second < rhs.second;
        });

    groupedValues.resize(50);

    std::vector<TString> source;
    for (const auto& row : sourceValues) {
        source.push_back(TString() + "a=" + ToString(row.first) + ";b=" + ToString(row.second));
    }

    auto resultSplit = MakeSplit({
        {"x", EValueType::Int64},
        {"y", EValueType::Int64}
    });

    std::vector<TOwningRow> result;
    result.push_back(YsonToRow("y=" + ToString(totalSum), resultSplit, true));

    for (const auto& row : groupedValues) {
        TString resultRow = TString() + "x=" + ToString(row.first) + ";y=" + ToString(row.second);
        result.push_back(YsonToRow(resultRow, resultSplit, false));
    }

    Evaluate("x, sum(b) as y FROM [//t] group by a % 200 as x with totals order by y limit 50",
        split, source, ResultMatcher(result));

    SUCCEED();
}

TEST_F(TQueryEvaluateTest, Udf)
{
    auto split = MakeSplit({
        {"a", EValueType::Int64},
        {"b", EValueType::Int64}
    });

    std::vector<TString> source = {
        "a=1;b=10",
        "a=-2;b=20",
        "a=9;b=90",
        "a=-10"
    };

    auto resultSplit = MakeSplit({
        {"x", EValueType::Int64}
    });

    auto result = YsonToRows({
        "x=1",
        "x=2",
        "x=9",
        "x=10"
    }, resultSplit);

    Evaluate("abs_udf(a) as x FROM [//t]", split, source, ResultMatcher(result));

    SUCCEED();
}

TEST_F(TQueryEvaluateTest, ZeroArgumentUdf)
{
    auto split = MakeSplit({
        {"a", EValueType::Uint64},
    });

    std::vector<TString> source = {
        "a=1u",
        "a=2u",
        "a=75u",
        "a=10u",
        "a=75u",
        "a=10u",
    };

    auto resultSplit = MakeSplit({
        {"a", EValueType::Uint64}
    });

    auto result = YsonToRows({
        "a=75u",
        "a=75u"
    }, resultSplit);

    Evaluate("a FROM [//t] where a = seventyfive()", split, source, ResultMatcher(result));

    SUCCEED();
}

TEST_F(TQueryEvaluateTest, UdfNullPropagation)
{
    auto split = MakeSplit({
        {"a", EValueType::Int64},
        {"b", EValueType::Int64}
    });

    std::vector<TString> source = {
        "a=1;",
        "a=-2;b=-20",
        "a=9;",
        "b=-10"
    };

    auto resultSplit = MakeSplit({
        {"x", EValueType::Int64}
    });

    auto result = YsonToRows({
        "",
        "x=20",
        "",
        "x=10"
    }, resultSplit);

    Evaluate("abs_udf(b) as x FROM [//t]", split, source, ResultMatcher(result));

    SUCCEED();
}

TEST_F(TQueryEvaluateTest, UdfNullPropagation2)
{
    auto split = MakeSplit({
        {"a", EValueType::Int64},
        {"b", EValueType::Int64}
    });

    std::vector<TString> source = {
        "a=1;",
        "a=2;b=10",
        "b=9",
        ""
    };

    auto resultSplit = MakeSplit({
        {"x", EValueType::Int64}
    });

    auto result = YsonToRows({
        "",
        "x=1024",
        "",
        ""
    }, resultSplit);

    Evaluate("exp_udf(a, b) as x FROM [//t]", split, source, ResultMatcher(result));

    SUCCEED();
}

TEST_F(TQueryEvaluateTest, UdfStringArgument)
{
    auto split = MakeSplit({
        {"a", EValueType::String}
    });

    std::vector<TString> source = {
        "a=\"123\"",
        "a=\"50\"",
        "a=\"\"",
        ""
    };

    auto resultSplit = MakeSplit({
        {"x", EValueType::Uint64}
    });

    auto result = YsonToRows({
        "x=123u",
        "x=50u",
        "x=0u",
        ""
    }, resultSplit);

    Evaluate("strtol_udf(a) as x FROM [//t]", split, source, ResultMatcher(result));

    SUCCEED();
}

TEST_F(TQueryEvaluateTest, UdfStringResult)
{
    auto split = MakeSplit({
        {"a", EValueType::String}
    });

    std::vector<TString> source = {
        "a=\"HELLO\"",
        "a=\"HeLlO\"",
        "a=\"\"",
        ""
    };

    auto resultSplit = MakeSplit({
        {"x", EValueType::String}
    });

    auto result = YsonToRows({
        "x=\"hello\"",
        "x=\"hello\"",
        "x=\"\"",
        ""
    }, resultSplit);

    Evaluate("tolower_udf(a) as x FROM [//t]", split, source, ResultMatcher(result));

    SUCCEED();
}

TEST_F(TQueryEvaluateTest, UnversionedValueUdf)
{
    auto split = MakeSplit({
        {"a", EValueType::String}
    });

    std::vector<TString> source = {
        "a=\"Hello\"",
        "a=\"\"",
        ""
    };

    auto resultSplit = MakeSplit({
        {"x", EValueType::Boolean}
    });

    auto result = YsonToRows({
        "x=%false",
        "x=%false",
        "x=%true"
    }, resultSplit);

    Evaluate("is_null_udf(a) as x FROM [//t]", split, source, ResultMatcher(result));

    SUCCEED();
}

TEST_F(TQueryEvaluateTest, YPathTryGetInt64)
{
    auto split = MakeSplit({
        {"yson", EValueType::Any},
        {"ypath", EValueType::String},
    });

    std::vector<TString> source = {
        "yson={b={c=4};d=[1;2]};ypath=\"/b/c\"",
        "yson={b={c=4};d=[1;2]};ypath=\"/d/1\"",
        "",
        "yson={b={c=4};d=[1;2]};ypath=\"/b/d\"",
        "yson={b={c=4};d=[1;2]}",
        "ypath=\"/d/1\"",
    };

    auto resultSplit = MakeSplit({
        {"result", EValueType::Int64}
    });

    auto result = YsonToRows({
        "result=4",
        "result=2",
        "",
        "",
        "",
        "",
    }, resultSplit);

    Evaluate("try_get_int64(yson, ypath) as result FROM [//t]", split, source, ResultMatcher(result));

    SUCCEED();
}

TEST_F(TQueryEvaluateTest, YPathGetInt64)
{
    auto split = MakeSplit({
        {"yson", EValueType::Any},
        {"ypath", EValueType::String},
    });

    std::vector<TString> source = {
        "yson={b={c=4};d=[1;2]};ypath=\"/b/c\"",
        "yson={b={c=4};d=[1;2]};ypath=\"/d/1\"",
        "",
        "yson={b={c=4};d=[1;2]}",
        "ypath=\"/d/1\"",
    };

    auto resultSplit = MakeSplit({
        {"result", EValueType::Int64}
    });

    auto result = YsonToRows({
        "result=4",
        "result=2",
        "",
        "",
        "",
    }, resultSplit);

    Evaluate("get_int64(yson, ypath) as result FROM [//t]", split, source, ResultMatcher(result));

    SUCCEED();
}

TEST_F(TQueryEvaluateTest, YPathGetInt64Fail)
{
    auto split = MakeSplit({
        {"yson", EValueType::Any},
        {"ypath", EValueType::String},
    });

    std::vector<TString> source = {
        "yson={b={c=4};d=[1;2]};ypath=\"/b/d\"",
        "yson={b={c=4};d=[1;2]};ypath=\"/d/2\"",
        "yson={b={c=4};d=[1;2u]};ypath=\"/d/1\"",
        "yson={b={c=4}d=[1;2}};ypath=\"/d/1\"",
        "yson={b={c=4};d=[1;2}};ypath=\"/d1\"",
        "yson={b={c=4};d=[1;2}};ypath=\"//d/1\"",
        "yson={b={c=4};d=[1;2}};ypath=\"/@d/1\"",
    };

    EvaluateExpectingError("try_get_int64(yson, ypath) as result FROM [//t]", split, source);
    EvaluateExpectingError("get_int64(yson, ypath) as result FROM [//t]", split, source);

    SUCCEED();
}

TEST_F(TQueryEvaluateTest, YPathTryGetUint64)
{
    auto split = MakeSplit({
        {"yson", EValueType::Any},
        {"ypath", EValueType::String},
    });

    std::vector<TString> source = {
        "yson={b={c=4u};d=[1;2]};ypath=\"/b/c\"",
        "yson={b={c=4};d=[1;2u]};ypath=\"/d/1\"",
        "",
        "yson={b={c=4};d=[1;2]};ypath=\"/b/d\"",
        "yson={b={c=4};d=[1;2]}",
        "ypath=\"/d/1\"",
    };

    auto resultSplit = MakeSplit({
        {"result", EValueType::Uint64}
    });

    auto result = YsonToRows({
        "result=4u",
        "result=2u",
        "",
        "",
        "",
        "",
    }, resultSplit);

    Evaluate("try_get_uint64(yson, ypath) as result FROM [//t]", split, source, ResultMatcher(result));

    SUCCEED();
}

TEST_F(TQueryEvaluateTest, YPathGetUint64)
{
    auto split = MakeSplit({
        {"yson", EValueType::Any},
        {"ypath", EValueType::String},
    });

    std::vector<TString> source = {
        "yson={b={c=4u};d=[1;2]};ypath=\"/b/c\"",
        "yson={b={c=4};d=[1;2u]};ypath=\"/d/1\"",
        "",
        "yson={b={c=4};d=[1;2]}",
        "ypath=\"/d/1\"",
    };

    auto resultSplit = MakeSplit({
        {"result", EValueType::Uint64}
    });

    auto result = YsonToRows({
        "result=4u",
        "result=2u",
        "",
        "",
        "",
    }, resultSplit);

    Evaluate("get_uint64(yson, ypath) as result FROM [//t]", split, source, ResultMatcher(result));

    SUCCEED();
}

TEST_F(TQueryEvaluateTest, YPathGetUint64Fail)
{
    auto split = MakeSplit({
        {"yson", EValueType::Any},
        {"ypath", EValueType::String},
    });

    std::vector<TString> source = {
        "yson={b={c=4u};d=[1u;2u]};ypath=\"/b/d\"",
        "yson={b={c=4u};d=[1u;2u]};ypath=\"/d/2\"",
        "yson={b={c=4u};d=[1u;2]};ypath=\"/d/1\"",
        "yson={b={c=4u}d=[1u;2u}};ypath=\"/d/1\"",
        "yson={b={c=4u};d=[1u;2u}};ypath=\"/d1\"",
        "yson={b={c=4u};d=[1u;2u}};ypath=\"//d/1\"",
        "yson={b={c=4u};d=[1u;2u}};ypath=\"/@d/1\"",
    };

    EvaluateExpectingError("try_get_uint64(yson, ypath) as result FROM [//t]", split, source);
    EvaluateExpectingError("get_uint64(yson, ypath) as result FROM [//t]", split, source);

    SUCCEED();
}

TEST_F(TQueryEvaluateTest, YPathTryGetDouble)
{
    auto split = MakeSplit({
        {"yson", EValueType::Any},
        {"ypath", EValueType::String},
    });

    std::vector<TString> source = {
        "yson={b={c=4.};d=[1;2]};ypath=\"/b/c\"",
        "yson={b={c=4};d=[1;2.]};ypath=\"/d/1\"",
        "",
        "yson={b={c=4};d=[1;2]};ypath=\"/b/d\"",
        "yson={b={c=4};d=[1;2]}",
        "ypath=\"/d/1\"",
    };

    auto resultSplit = MakeSplit({
        {"result", EValueType::Double}
    });

    auto result = YsonToRows({
        "result=4.",
        "result=2.",
        "",
        "",
        "",
        "",
    }, resultSplit);

    Evaluate("try_get_double(yson, ypath) as result FROM [//t]", split, source, ResultMatcher(result));

    SUCCEED();
}

TEST_F(TQueryEvaluateTest, YPathGetDouble)
{
    auto split = MakeSplit({
        {"yson", EValueType::Any},
        {"ypath", EValueType::String},
    });

    std::vector<TString> source = {
        "yson={b={c=4.};d=[1;2]};ypath=\"/b/c\"",
        "yson={b={c=4};d=[1;2.]};ypath=\"/d/1\"",
        "",
        "yson={b={c=4};d=[1;2]}",
        "ypath=\"/d/1\"",
    };

    auto resultSplit = MakeSplit({
        {"result", EValueType::Double}
    });

    auto result = YsonToRows({
        "result=4.",
        "result=2.",
        "",
        "",
        "",
    }, resultSplit);

    Evaluate("get_double(yson, ypath) as result FROM [//t]", split, source, ResultMatcher(result));

    SUCCEED();
}

TEST_F(TQueryEvaluateTest, YPathGetDoubleFail)
{
    auto split = MakeSplit({
        {"yson", EValueType::Any},
        {"ypath", EValueType::String},
    });

    std::vector<TString> source = {
        "yson={b={c=4};d=[1;2]};ypath=\"/b/d\"",
        "yson={b={c=4};d=[1;2]};ypath=\"/d/2\"",
        "yson={b={c=4};d=[1;2u]};ypath=\"/d/1\"",
        "yson={b={c=4}d=[1;2}};ypath=\"/d/1\"",
        "yson={b={c=4};d=[1;2}};ypath=\"/d1\"",
        "yson={b={c=4};d=[1;2}};ypath=\"//d/1\"",
        "yson={b={c=4};d=[1;2}};ypath=\"/@d/1\"",
    };

    EvaluateExpectingError("try_get_double(yson, ypath) as result FROM [//t]", split, source);
    EvaluateExpectingError("get_double(yson, ypath) as result FROM [//t]", split, source);

    SUCCEED();
}

TEST_F(TQueryEvaluateTest, YPathTryGetBoolean)
{
    auto split = MakeSplit({
        {"yson", EValueType::Any},
        {"ypath", EValueType::String},
    });

    std::vector<TString> source = {
        "yson={b={c=%true};d=[1;2]};ypath=\"/b/c\"",
        "yson={b={c=4};d=[1;%false]};ypath=\"/d/1\"",
        "",
        "yson={b={c=4};d=[1;2]};ypath=\"/b/d\"",
        "yson={b={c=4};d=[1;2]}",
        "ypath=\"/d/1\"",
    };

    auto resultSplit = MakeSplit({
        {"result", EValueType::Boolean}
    });

    auto result = YsonToRows({
        "result=%true",
        "result=%false",
        "",
        "",
        "",
        "",
    }, resultSplit);

    Evaluate("try_get_boolean(yson, ypath) as result FROM [//t]", split, source, ResultMatcher(result));

    SUCCEED();
}

TEST_F(TQueryEvaluateTest, YPathGetBoolean)
{
    auto split = MakeSplit({
        {"yson", EValueType::Any},
        {"ypath", EValueType::String},
    });

    std::vector<TString> source = {
        "yson={b={c=%false};d=[1;2]};ypath=\"/b/c\"",
        "yson={b={c=4};d=[1;%true]};ypath=\"/d/1\"",
        "",
        "yson={b={c=4};d=[1;2]}",
        "ypath=\"/d/1\"",
    };

    auto resultSplit = MakeSplit({
        {"result", EValueType::Boolean}
    });

    auto result = YsonToRows({
        "result=%false",
        "result=%true",
        "",
        "",
        "",
    }, resultSplit);

    Evaluate("get_boolean(yson, ypath) as result FROM [//t]", split, source, ResultMatcher(result));

    SUCCEED();
}

TEST_F(TQueryEvaluateTest, YPathGetBooleanFail)
{
    auto split = MakeSplit({
        {"yson", EValueType::Any},
        {"ypath", EValueType::String},
    });

    std::vector<TString> source = {
        "yson={b={c=4};d=[1;2]};ypath=\"/b/d\"",
        "yson={b={c=4};d=[1;2]};ypath=\"/d/2\"",
        "yson={b={c=4};d=[1;2u]};ypath=\"/d/1\"",
        "yson={b={c=4}d=[1;2}};ypath=\"/d/1\"",
        "yson={b={c=4};d=[1;2}};ypath=\"/d1\"",
        "yson={b={c=4};d=[1;2}};ypath=\"//d/1\"",
        "yson={b={c=4};d=[1;2}};ypath=\"/@d/1\"",
    };

    EvaluateExpectingError("try_get_boolean(yson, ypath) as result FROM [//t]", split, source);
    EvaluateExpectingError("get_boolean(yson, ypath) as result FROM [//t]", split, source);

    SUCCEED();
}

TEST_F(TQueryEvaluateTest, YPathTryGetString)
{
    auto split = MakeSplit({
        {"yson", EValueType::Any},
        {"ypath", EValueType::String},
    });

    std::vector<TString> source = {
        "yson={b={c=\"hello\"};d=[1;2]};ypath=\"/b/c\"",
        "yson={b={c=4};d=[1;\"world\"]};ypath=\"/d/1\"",
        "",
        "yson={b={c=4};d=[1;2]};ypath=\"/b/d\"",
        "yson={b={c=4};d=[1;2]}",
        "ypath=\"/d/1\"",
    };

    auto resultSplit = MakeSplit({
        {"result", EValueType::String}
    });

    auto result = YsonToRows({
        "result=\"hello\"",
        "result=\"world\"",
        "",
        "",
        "",
        "",
    }, resultSplit);

    Evaluate("try_get_string(yson, ypath) as result FROM [//t]", split, source, ResultMatcher(result));

    SUCCEED();
}

TEST_F(TQueryEvaluateTest, YPathGetString)
{
    auto split = MakeSplit({
        {"yson", EValueType::Any},
        {"ypath", EValueType::String},
    });

    std::vector<TString> source = {
        "yson={b={c=\"here\"};d=[1;2]};ypath=\"/b/c\"",
        "yson={b={c=4};d=[1;\"there\"]};ypath=\"/d/1\"",
        "",
        "yson={b={c=4};d=[1;2]}",
        "ypath=\"/d/1\"",
    };

    auto resultSplit = MakeSplit({
        {"result", EValueType::String}
    });

    auto result = YsonToRows({
        "result=\"here\"",
        "result=\"there\"",
        "",
        "",
        "",
    }, resultSplit);

    Evaluate("get_string(yson, ypath) as result FROM [//t]", split, source, ResultMatcher(result));

    SUCCEED();
}

TEST_F(TQueryEvaluateTest, YPathGetStringFail)
{
    auto split = MakeSplit({
        {"yson", EValueType::Any},
        {"ypath", EValueType::String},
    });

    std::vector<TString> source = {
        "",
        "yson={b={c=4};d=[1;2]};ypath=\"/b/d\"",
        "yson={b={c=4};d=[1;2]};ypath=\"/d/2\"",
        "yson={b={c=4};d=[1;2u]};ypath=\"/d/1\"",
        "yson={b={c=4}d=[1;2}};ypath=\"/d/1\"",
        "yson={b={c=4};d=[1;2}};ypath=\"/d1\"",
        "yson={b={c=4};d=[1;2}};ypath=\"//d/1\"",
        "yson={b={c=4};d=[1;2}};ypath=\"/@d/1\"",
    };

    EvaluateExpectingError("try_get_string(yson, ypath) as result FROM [//t]", split, source);
    EvaluateExpectingError("get_string(yson, ypath) as result FROM [//t]", split, source);

    SUCCEED();
}

TEST_F(TQueryEvaluateTest, YPathGetAny)
{
    auto split = MakeSplit({
        {"yson", EValueType::Any},
        {"ypath0", EValueType::String},
        {"ypath1", EValueType::String},
        {"value", EValueType::String},
    });

    std::vector<TString> source = {
        "yson={b={c=\"here\"};d=[1;2]};ypath0=\"/b\";ypath1=\"/c\";value=\"here\"",
        "yson={b={c=4};d=[1;\"there\"]};ypath0=\"/d\";ypath1=\"/1\";value=\"there\"",
        "",
        "yson={b={c=4};d=[1;2]}",
        "ypath0=\"/d/1\"",
    };

    auto resultSplit = MakeSplit({
        {"result", EValueType::Boolean}
    });

    auto result = YsonToRows({
        "result=%true",
        "result=%true",
        "result=%true",
        "result=%true",
        "result=%true",
    }, resultSplit);

    Evaluate("get_any(get_any(yson, ypath0), ypath1) = value as result FROM [//t]",
        split,
        source,
        ResultMatcher(result));

    SUCCEED();
}

TEST_F(TQueryEvaluateTest, CompareAny)
{
    auto split = MakeSplit({
        {"a", EValueType::Any},
        {"b", EValueType::Any}
    });

    std::vector<TString> source = {
        "a=%false;b=%true;",
        "a=%false;b=%false;",
        "a=1;b=2;",
        "a=1;b=1;",
        "a=1u;b=2u;",
        "a=1u;b=1u;",
        "a=1.0;b=2.0;",
        "a=1.0;b=1.0;",
        "a=x;b=y;",
        "a=x;b=x;",
    };

    auto resultSplit = MakeSplit({
        {"r1", EValueType::Boolean},
        {"r2", EValueType::Boolean},
        {"r3", EValueType::Boolean},
        {"r4", EValueType::Boolean},
        {"r5", EValueType::Boolean},
        {"r6", EValueType::Boolean}
    });

    auto result = YsonToRows({
        "r1=%true;r2=%false;r3=%true;r4=%false;r5=%false;r6=%true",
        "r1=%false;r2=%false;r3=%true;r4=%true;r5=%true;r6=%false",
        "r1=%true;r2=%false;r3=%true;r4=%false;r5=%false;r6=%true",
        "r1=%false;r2=%false;r3=%true;r4=%true;r5=%true;r6=%false",
        "r1=%true;r2=%false;r3=%true;r4=%false;r5=%false;r6=%true",
        "r1=%false;r2=%false;r3=%true;r4=%true;r5=%true;r6=%false",
        "r1=%true;r2=%false;r3=%true;r4=%false;r5=%false;r6=%true",
        "r1=%false;r2=%false;r3=%true;r4=%true;r5=%true;r6=%false",
        "r1=%true;r2=%false;r3=%true;r4=%false;r5=%false;r6=%true",
        "r1=%false;r2=%false;r3=%true;r4=%true;r5=%true;r6=%false",
    }, resultSplit);

    Evaluate("a < b as r1, a > b as r2, a <= b as r3, a >= b as r4, a = b as r5, a != b as r6 FROM [//t]",
        split,
        source,
        ResultMatcher(result));

    SUCCEED();
}

TEST_F(TQueryEvaluateTest, CompareAnyMixed)
{
    auto resultSplit = MakeSplit({
        {"r1", EValueType::Boolean},
        {"r2", EValueType::Boolean},
        {"r3", EValueType::Boolean},
        {"r4", EValueType::Boolean},
        {"r5", EValueType::Boolean},
        {"r6", EValueType::Boolean},
        {"r7", EValueType::Boolean}
    });

    auto result = YsonToRows({
        "r1=%true;r2=%false;r3=%true;r4=%false;r5=%false;r6=%true;r7=%true",
        "r1=%false;r2=%false;r3=%true;r4=%true;r5=%true;r6=%false;r7=%true",
    }, resultSplit);

    TString query = "a < b as r1, a > b as r2, a <= b as r3, a >= b as r4, a = b as r5, a != b as r6, a < b = b "
        "> a and a > b = b < a as r7 FROM [//t]";

    Evaluate(query, MakeSplit({
            {"a", EValueType::Any},
            {"b", EValueType::Boolean},
        }), {
            "a=%false;b=%true;",
            "a=%false;b=%false;"},
        ResultMatcher(result));

    Evaluate(query, MakeSplit({
            {"a", EValueType::Any},
            {"b", EValueType::Int64},
        }), {
            "a=1;b=2;",
            "a=1;b=1;"},
        ResultMatcher(result));

    Evaluate(query, MakeSplit({
            {"a", EValueType::Any},
            {"b", EValueType::Uint64},
        }), {
            "a=1u;b=2u;",
            "a=1u;b=1u;"},
        ResultMatcher(result));

    Evaluate(query, MakeSplit({
            {"a", EValueType::Any},
            {"b", EValueType::Double},
        }), {
            "a=1.0;b=2.0;",
            "a=1.0;b=1.0;"},
        ResultMatcher(result));

    Evaluate(query, MakeSplit({
            {"a", EValueType::Any},
            {"b", EValueType::String},
        }), {
            "a=x;b=y;",
            "a=x;b=x;"},
        ResultMatcher(result));

    SUCCEED();
}

TEST_F(TQueryEvaluateTest, ToAnyAndCompare)
{
    auto resultSplit = MakeSplit({
        {"r", EValueType::Boolean}
    });

    auto result = YsonToRows({
        "r=%true",
    }, resultSplit);

    TString query = "to_any(a) = a FROM [//t]";

    Evaluate(query, MakeSplit({
            {"a", EValueType::Boolean}
        }), {
            "a=%false;"},
        ResultMatcher(result));

    Evaluate(query, MakeSplit({
            {"a", EValueType::Int64}
        }), {
            "a=1;"},
        ResultMatcher(result));

    Evaluate(query, MakeSplit({
            {"a", EValueType::Uint64}
        }), {
            "a=1u;"},
        ResultMatcher(result));

    Evaluate(query, MakeSplit({
            {"a", EValueType::Double}
        }), {
            "a=1.0;"},
        ResultMatcher(result));

    Evaluate(query, MakeSplit({
            {"a", EValueType::String}
        }), {
            "a=x;"},
        ResultMatcher(result));

    SUCCEED();
}

TEST_F(TQueryEvaluateTest, VarargUdf)
{
    auto split = MakeSplit({
        {"a", EValueType::Int64}
    });

    std::vector<TString> source = {
        "a=1",
        "a=2"
    };

    auto resultSplit = MakeSplit({
        {"x", EValueType::Int64}
    });

    auto result = YsonToRows({
        "x=1",
        "x=2"
    }, resultSplit);

    Evaluate("a as x FROM [//t] where sum_udf(7, 3, a) in (11u, 12)", split, source, ResultMatcher(result));

    SUCCEED();
}

TEST_F(TQueryEvaluateTest, FarmHash)
{
    auto split = MakeSplit({
        {"a", EValueType::Int64},
        {"b", EValueType::String},
        {"c", EValueType::Boolean}
    });

    std::vector<TString> source = {
        "a=3;b=\"hello\";c=%true",
        "a=54;c=%false"
    };

    auto resultSplit = MakeSplit({
        {"x", EValueType::Uint64}
    });

    auto result = YsonToRows({
        "x=13185060272037541714u",
        "x=1607147011416532415u"
    }, resultSplit);

    Evaluate("farm_hash(a, b, c) as x FROM [//t]", split, source, ResultMatcher(result));

    SUCCEED();
}

TEST_F(TQueryEvaluateTest, BigbHash)
{
    auto split = MakeSplit({
        {"A", EValueType::String},
    });

    std::vector<TString> source = {
        "A=\"y12345\"",
        "A=\"y12345b\"",
        "A=\"p12345\"",
        "A=\"gaid/12345\"",
        "A=\"idfa/12345\"",
        "A=\"12345\"",
        "A=\"\"",
        "A=\"y\"",
    };

    auto resultSplit = MakeSplit({
        {"x", EValueType::Uint64}
    });

    auto result = YsonToRows({
        "x=12345u",
        "x=4325840212205223962u",
        "x=7036960256067388486u",
        "x=17724055447702487579u",
        "x=5977914593781245279u",
        "x=1820233801294503536u",
        "x=0u",
        "x=15359751383596667256u",
    }, resultSplit);

    Evaluate("bigb_hash(A) as x FROM [//t]", split, source, ResultMatcher(result));

    SUCCEED();
}

TEST_F(TQueryEvaluateTest, RegexParseError)
{
    auto split = MakeSplit({
        {"a", EValueType::String},
    });

    std::vector<TString> source = {
        "a=\"hello\"",
        "a=\"hell\"",
        "",
    };

    auto resultSplit = MakeSplit({
        {"x", EValueType::Boolean},
    });

    auto result = YsonToRows({
        "x=%false",
        "x=%true",
        "x=%false",
    }, resultSplit);

    EvaluateExpectingError("regex_full_match(\"hel[a-z)\", a) as x FROM [//t]", split, source);
}

TEST_F(TQueryEvaluateTest, RegexFullMatch)
{
    auto split = MakeSplit({
        {"a", EValueType::String},
    });

    std::vector<TString> source = {
        "a=\"hello\"",
        "a=\"hell\"",
        "",
    };

    auto resultSplit = MakeSplit({
        {"x", EValueType::Boolean},
    });

    auto result = YsonToRows({
        "x=%false",
        "x=%true",
        "x=%false",
    }, resultSplit);

    Evaluate("regex_full_match(\"hel[a-z]\", a) as x FROM [//t]", split, source, ResultMatcher(result));

    SUCCEED();
}

TEST_F(TQueryEvaluateTest, RegexPartialMatch)
{
    auto split = MakeSplit({
        {"a", EValueType::String},
    });

    std::vector<TString> source = {
        "a=\"xx\"",
        "a=\"x43x\"",
        "",
    };

    auto resultSplit = MakeSplit({
        {"x", EValueType::Boolean},
    });

    auto result = YsonToRows({
        "x=%false",
        "x=%true",
        "x=%false",
    }, resultSplit);

    Evaluate("regex_partial_match(\"[0-9]+\", a) as x FROM [//t]", split, source, ResultMatcher(result));

    SUCCEED();
}

TEST_F(TQueryEvaluateTest, RegexReplaceFirst)
{
    auto split = MakeSplit({
        {"a", EValueType::String},
    });

    std::vector<TString> source = {
        "a=\"x43x43x\"",
        "",
    };

    auto resultSplit = MakeSplit({
        {"x", EValueType::String},
    });

    auto result = YsonToRows({
        "x=\"x_x43x\"",
        "",
    }, resultSplit);

    Evaluate("regex_replace_first(\"[0-9]+\", a, \"_\") as x FROM [//t]", split, source, ResultMatcher(result));

    SUCCEED();
}

TEST_F(TQueryEvaluateTest, RegexReplaceAll)
{
    auto split = MakeSplit({
        {"a", EValueType::String},
    });

    std::vector<TString> source = {
        "a=\"x43x43x\"",
        "",
    };

    auto resultSplit = MakeSplit({
        {"x", EValueType::String},
    });

    auto result = YsonToRows({
        "x=\"x_x_x\"",
        "",
    }, resultSplit);

    Evaluate("regex_replace_all(\"[0-9]+\", a, \"_\") as x FROM [//t]", split, source, ResultMatcher(result));

    SUCCEED();
}

TEST_F(TQueryEvaluateTest, RegexExtract)
{
    auto split = MakeSplit({
        {"a", EValueType::String},
    });

    std::vector<TString> source = {
        "a=\"Send root@ya.com an email.\"",
        "",
    };

    auto resultSplit = MakeSplit({
        {"x", EValueType::String},
    });

    auto result = YsonToRows({
        "x=\"root at ya\"",
        "",
    }, resultSplit);

    Evaluate("regex_extract(\"([a-z]*)@(.*).com\", a, \"\\\\1 at \\\\2\") as x FROM [//t]", split, source, ResultMatcher(result));

    SUCCEED();
}

TEST_F(TQueryEvaluateTest, RegexEscape)
{
    auto split = MakeSplit({
        {"a", EValueType::String},
    });

    std::vector<TString> source = {
        "a=\"1.5\"",
        "",
    };

    auto resultSplit = MakeSplit({
        {"x", EValueType::String},
    });

    auto result = YsonToRows({
        "x=\"1\\\\.5\"",
        "",
    }, resultSplit);

    Evaluate("regex_escape(a) as x FROM [//t]", split, source, ResultMatcher(result));

    SUCCEED();
}

TEST_F(TQueryEvaluateTest, AverageAgg)
{
    auto split = MakeSplit({
        {"a", EValueType::Int64}
    });

    std::vector<TString> source = {
        "a=3",
        "a=53",
        "a=8",
        "a=24",
        "a=33"
    };

    auto resultSplit = MakeSplit({
        {"x", EValueType::Double}
    });

    auto result = YsonToRows({
        "x=24.2",
    }, resultSplit);

    Evaluate("avg(a) as x from [//t] group by 1", split, source, ResultMatcher(result));
}

TEST_F(TQueryEvaluateTest, AverageAgg2)
{
    auto split = MakeSplit({
        {"a", EValueType::Int64},
        {"b", EValueType::Int64},
        {"c", EValueType::Int64}
    });

    std::vector<TString> source = {
        "a=3;b=3;c=1",
        "a=53;b=2;c=3",
        "a=8;b=5;c=32",
        "a=24;b=7;c=4",
        "a=33;b=4;c=9",
        "a=33;b=3;c=43",
        "a=23;b=0;c=0",
        "a=33;b=8;c=2"
    };

    auto resultSplit = MakeSplit({
        {"r1", EValueType::Double},
        {"x", EValueType::Int64},
        {"r2", EValueType::Int64},
        {"r3", EValueType::Double},
        {"r4", EValueType::Int64},
    });

    auto result = YsonToRows({
        "r1=17.0;x=1;r2=43;r3=20.0;r4=3",
        "r1=35.5;x=0;r2=9;r3=3.5;r4=23"
    }, resultSplit);

    Evaluate("avg(a) as r1, x, max(c) as r2, avg(c) as r3, min(a) as r4 from [//t] group by b % 2 as x", split, source, ResultMatcher(result));
}

TEST_F(TQueryEvaluateTest, AverageAgg3)
{
    auto split = MakeSplit({
        {"a", EValueType::Int64},
        {"b", EValueType::Int64}
    });

    std::vector<TString> source = {
        "a=3;b=1",
        "b=1",
        "b=0",
        "a=7;b=1",
    };

    auto resultSplit = MakeSplit({
        {"b", EValueType::Int64},
        {"x", EValueType::Double}
    });

    auto result = YsonToRows({
        "b=1;x=5.0",
        "b=0"
    }, resultSplit);

    Evaluate("b, avg(a) as x from [//t] group by b", split, source, ResultMatcher(result));
}

TEST_F(TQueryEvaluateTest, StringAgg)
{
    auto split = MakeSplit({
        {"a", EValueType::String},
    });

    std::vector<TString> source = {
        "a=\"one\"",
        "a=\"two\"",
        "a=\"three\"",
        "a=\"four\"",
        "a=\"fo\"",
    };

    auto resultSplit = MakeSplit({
        {"b", EValueType::String},
    });

    auto result = YsonToRows({
        "b=\"fo\";c=\"two\"",
    }, resultSplit);

    Evaluate("min(a) as b, max(a) as c from [//t] group by 1", split, source, ResultMatcher(result));
}

TEST_F(TQueryEvaluateTest, CardinalityAggregate)
{
    auto split = MakeSplit({
        {"a", EValueType::Int64}
    });

    std::vector<TString> source;
    for (int i = 0; i < 4; i++) {
        for (int j = 0; j < 2000; j++) {
            source.push_back("a=" + ToString(j));
        }
    }

    auto resultSplit = MakeSplit({
        {"upper", EValueType::Boolean},
        {"lower", EValueType::Boolean},
    });

    auto result = YsonToRows({
        "upper=%true;lower=%true"
    }, resultSplit);

    Evaluate("cardinality(a) < 2020 as upper, cardinality(a) > 1980 as lower from [//t] group by 1", split, source, ResultMatcher(result));
}

TEST_F(TQueryEvaluateTest, CardinalityAggregateTotals)
{
    auto split = MakeSplit({
        {"a", EValueType::Int64}
    });

    std::vector<TString> source;
    for (int i = 0; i < 4; i++) {
        for (int j = 0; j < 2000; j++) {
            source.push_back("a=" + ToString(j));
        }
    }

    auto resultSplit = MakeSplit({
        {"upper", EValueType::Boolean},
        {"lower", EValueType::Boolean},
    });

    auto result = YsonToRows({
        "upper=%true;lower=%true",
        "upper=%true;lower=%true"
    }, resultSplit);

    Evaluate(
        "cardinality(a) < 2020 as upper, cardinality(a) > 1980 as lower from [//t] group by 1 with totals",
        split,
        source,
        ResultMatcher(result));
}

TEST_F(TQueryEvaluateTest, CardinalityAggregateTotals2)
{
    auto split = MakeSplit({
        {"a", EValueType::Int64},
        {"b", EValueType::Int64}
    });

    std::vector<TString> source;
    for (int i = 0; i < 12; i++) {
        for (int j = 0; j < 1000 * (i % 3 + 1); j++) {
            source.push_back("a=" + ToString(j) + "; b=" + ToString(i % 3 + 1));
        }
    }

    auto resultSplit = MakeSplit({
        {"result", EValueType::Boolean},
        {"total", EValueType::Boolean},
        {"b", EValueType::Int64},
    });

    auto result = YsonToRows({
        "result=%true;total=%false;b=2",
        "result=%true;total=%true;b=3",
        "result=%true;total=%true"
    }, resultSplit);

    Evaluate(
        "(int64(cardinality(a)) - b * 1000 as x) >= (-b * 10) and x <= (b * 10) as result,"
        "(int64(cardinality(a)) - 3000) between -30 and 30 as total, b "
        "from [//t] group by b having cardinality(a) > 1500 with totals",
        split,
        source,
        ResultMatcher(result));
}

TEST_F(TQueryEvaluateTest, CardinalityAggregateTotals3)
{
    auto split = MakeSplit({
        {"a", EValueType::Int64},
        {"b", EValueType::Int64}
    });

    std::vector<TString> source;
    for (int i = 0; i < 16; i++) {
        for (int j = 0; j < 500; j++) {
            source.push_back("a=" + ToString(j + 1000 * (i % 3)) + "; b=" + ToString(i % 3));
        }
    }

    auto resultSplit = MakeSplit({
        {"result", EValueType::Boolean},
        {"total", EValueType::Boolean},
        {"b", EValueType::Int64},
    });

    auto result = YsonToRows({
        "result=%true;total=%false;b=1",
        "result=%true;total=%false;b=2",
        "result=%false;total=%true"
    }, resultSplit);

    Evaluate(
        "int64(cardinality(a)) between 490 and 510 as result,"
        "int64(cardinality(a)) between 990 and 1010 as total, b "
        "from [//t] group by b having min(a) > 400 with totals",
        split,
        source,
        ResultMatcher(result));

}

TEST_F(TQueryEvaluateTest, Casts)
{
    auto split = MakeSplit({
        {"a", EValueType::Uint64},
        {"b", EValueType::Int64},
        {"c", EValueType::Double}
    });

    std::vector<TString> source = {
        "a=3u;b=34",
        "c=1.23",
        "a=12u",
        "b=0;c=1.0",
        "a=5u",
    };

    auto resultSplit = MakeSplit({
        {"r1", EValueType::Int64},
        {"r2", EValueType::Double},
        {"r3", EValueType::Uint64},
    });

    auto result = YsonToRows({
        "r1=3;r2=34.0",
        "r3=1u",
        "r1=12",
        "r2=0.0;r3=1u",
        "r1=5",
    }, resultSplit);

    Evaluate("int64(a) as r1, double(b) as r2, uint64(c) as r3 from [//t]", split, source, ResultMatcher(result));
}

TEST_F(TQueryEvaluateTest, UdfException)
{
    auto split = MakeSplit({
        {"a", EValueType::Int64},
    });

    std::vector<TString> source = {
        "a=-3",
    };

    auto resultSplit = MakeSplit({
        {"r", EValueType::Int64},
    });

    auto result = YsonToRows({
    }, resultSplit);

    EvaluateExpectingError("throw_if_negative_udf(a) from [//t]", split, source);
}

TEST_F(TQueryEvaluateTest, MakeMapSuccess)
{
    auto split = MakeSplit({
        {"v_any", EValueType::Any},
        {"v_null", EValueType::Any}
    });

    std::vector<TString> source = {
        "v_any={hello=world}"
    };

    auto resultSplit = MakeSplit({
        {"x", EValueType::Any},
    });

    auto result = YsonToRows({
        "x={"
        "  k_int=1;"
        "  k_uint=2u;"
        "  k_bool=%true;"
        "  k_double=3.14;"
        "  k_any={hello=world};"
        "  k_null=#;"
        "}",
    }, resultSplit);

    Evaluate(
        "make_map("
        "  \"k_int\", 1, "
        "  \"k_uint\", 2u, "
        "  \"k_bool\", %true, "
        "  \"k_double\", 3.14, "
        "  \"k_any\", v_any, "
        "  \"k_null\", v_null"
        ") as x FROM [//t]", split, source, ResultMatcher(result));
}

TEST_F(TQueryEvaluateTest, MakeMapFailure)
{
    auto split = MakeSplit({
        {"a", EValueType::Any}
    });

    std::vector<TString> source = {
        "a=1"
    };

    auto resultSplit = MakeSplit({
        {"x", EValueType::Any},
    });

    EvaluateExpectingError("make_map(\"a\") as x FROM [//t]", split, source);
    EvaluateExpectingError("make_map(1, 1) as x FROM [//t]", split, source);
}

TEST_F(TQueryEvaluateTest, DecimalExpr)
{
    auto split = MakeSplit({
        {"a", DecimalLogicalType(5, 2)}
    });
    std::vector<TString> source = {
        "a=\"\\x80\\x00\\x2a\\x3a\""
    };

    auto result = YsonToRows(source, split);

    Evaluate("a FROM [//t]", split, source, ResultMatcher(result, New<TTableSchema>(std::vector<TColumnSchema>{
        {"a", DecimalLogicalType(5, 2)}
    })));
}

TEST_F(TQueryEvaluateTest, TypeV1Propagation)
{
    auto split = MakeSplit({
        {"a", SimpleLogicalType(ESimpleLogicalValueType::Int32)}
    });
    std::vector<TString> source = {
        "a=5"
    };

    auto result = YsonToRows(source, split);

    Evaluate("a FROM [//t]", split, source, ResultMatcher(result, New<TTableSchema>(std::vector<TColumnSchema>{
        {"a", OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Int64))}
    })));
}

TEST_F(TQueryEvaluateTest, ListExpr)
{
    auto split = MakeSplit({
        {"a", ListLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Int32))}
    });
    std::vector<TString> source = {
        "a=[1;2;3]"
    };

    auto result = YsonToRows(source, split);

    Evaluate("a FROM [//t]", split, source, ResultMatcher(result, New<TTableSchema>(std::vector<TColumnSchema>{
        {"a", ListLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Int32))}
    })));
}

TEST_F(TQueryEvaluateTest, ListExprToAny)
{
    auto split = MakeSplit({
        {"a", ListLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Int32))}
    });
    std::vector<TString> source = {
        "a=[1;2;3]"
    };

    auto result = YsonToRows(source, split);

    Evaluate("to_any(a) as b FROM [//t]", split, source, ResultMatcher(result, New<TTableSchema>(std::vector<TColumnSchema>{
        {"b", ESimpleLogicalValueType::Any}
    })));
}

////////////////////////////////////////////////////////////////////////////////

struct TJoinColumns
{
    const char* Self = nullptr;
    const char* Foreign = nullptr;
};

struct TGroupColumns
{
    const char* X = nullptr;
    const char* Y = nullptr;
    const char* Z = nullptr;
};

struct TIntValue
    : public std::optional<int>
{
    using TBase = std::optional<int>;
    using TBase::TBase;

    void operator+=(const TIntValue& other)
    {
        if (!*this) {
            *this = other;
        } else if (other) {
            **this += *other;
        }
    }
};

bool operator<(const TIntValue& l, const TIntValue& r)
{
    return static_cast<const std::optional<int>&>(l) < static_cast<const std::optional<int>&>(r);
}

bool operator==(const TIntValue& l, const TIntValue& r)
{
    return static_cast<const std::optional<int>&>(l) == static_cast<const std::optional<int>&>(r);
}

void FormatValue(TStringBuilderBase* builder, const TIntValue& value, TStringBuf /*spec*/)
{
    if (value) {
        builder->AppendFormat("%v", *value);
    } else {
        builder->AppendChar('#');
    }
}

class TQueryEvaluateComplexTest
    : public TQueryEvaluateTest
    , public ::testing::WithParamInterface<std::tuple<
        const char*, // left or inner
        TJoinColumns, // join equation
        TGroupColumns, // group key
        const char*>> // totals
{
protected:
    struct TPrimaryKey
    {
        TIntValue a, b, c;

        bool operator< (const TPrimaryKey& other) const
        {
            return std::tie(a, b, c) < std::tie(other.a, other.b, other.c);
        }

        bool operator== (const TPrimaryKey& other) const
        {
            return std::tie(a, b, c) == std::tie(other.a, other.b, other.c);
        }
    };

    struct TPrimaryValue
    {
        TIntValue v;
    };

    struct TPrimaryRow
        : public TPrimaryKey
        , public TPrimaryValue
    { };

    struct TSecondaryKey
    {
        TIntValue d, e;

        bool operator< (const TSecondaryKey& other) const
        {
            return std::tie(d, e) < std::tie(other.d, other.e);
        }

        bool operator== (const TSecondaryKey& other) const
        {
            return std::tie(d, e) == std::tie(other.d, other.e);
        }
    };

    struct TSecondaryValue
    {
        TIntValue w;
    };

    struct TSecondaryRow
        : public TSecondaryKey
        , public TSecondaryValue
    { };

    struct TGroupKey
    {
        TIntValue x, y, z;

        bool operator< (const TGroupKey& other) const
        {
            return std::tie(x, y, z) < std::tie(other.x, other.y, other.z);
        }

        bool operator== (const TGroupKey& other) const
        {
            return std::tie(x, y, z) == std::tie(other.x, other.y, other.z);
        }
    };

    struct TAggregates
    {
        TIntValue count, sumv, sumw;

        void operator+= (const TAggregates& other)
        {
            count += other.count;
            sumv += other.sumv;
            sumw += other.sumw;
        }
    };

    struct TResultRow
        : TGroupKey
        , TAggregates
    { };

    void DoTest(
        const std::vector<TPrimaryRow>& primaryTable,
        const std::vector<TSecondaryRow>& secondaryTable,
        size_t offset,
        size_t limit,
        TStringBuf joinType,
        TJoinColumns joinEq,
        TGroupColumns groupEq,
        TStringBuf groupTotals);

};


void TQueryEvaluateComplexTest::DoTest(
    const std::vector<TPrimaryRow>& primaryTable,
    const std::vector<TSecondaryRow>& secondaryTable,
    size_t offset,
    size_t limit,
    TStringBuf joinType,
    TJoinColumns joinEq,
    TGroupColumns groupEq,
    TStringBuf groupTotals)
{
    // Primary columns: (a, b, c) -> v
    // Secondary columns: (d, e) -> w
    // Group columns x, y, z

    TString queryString = Format(
        "x, y, z, sum(1) as count, sum(v) as sumv, sum(w) as sumw "
        "from [//t] %v join [//s] on (%v) = (%v) group by %v as x, %v as y, %v as z %v offset %v limit %v",
        joinType,
        joinEq.Self,
        joinEq.Foreign,
        groupEq.X,
        groupEq.Y,
        groupEq.Z,
        groupTotals,
        offset,
        limit);

    TStringBuf s1, s2;
    StringSplitter(joinEq.Self)
        .Split(',')
        .CollectInto(&s1, &s2);

    s1 = StripString(s1);
    s2 = StripString(s2);

    auto evaluate = [] (const TPrimaryRow& row, TStringBuf expr) -> TIntValue {
        if (expr == "a") {
            return row.a;
        }

        if (expr == "b") {
            return row.b;
        }

        if (expr == "c") {
            return row.c;
        }

        if (expr == "c % 3") {
            return row.c ? TIntValue(*row.c % 3) : std::nullopt;
        }

        if (expr == "(b * 10 + c) % 3") {
            return row.b && row.c ? TIntValue((*row.b * 10 + *row.c) % 3) : std::nullopt;
        }

        if (expr == "0") {
            return 0;
        }

        YT_ABORT();
    };

    std::map<TGroupKey, TAggregates> lookup;
    std::vector<TGroupKey> groupedKeys;

    bool isLeft = joinType == "left";

    TSecondaryRow nullRow;

    for (auto& primaryRow : primaryTable) {
        // Join.
        auto joinKey = TSecondaryKey{
            evaluate(primaryRow, s1),
            evaluate(primaryRow, s2)};

        auto [begin, end] = std::equal_range(
            secondaryTable.data(),
            secondaryTable.data() + secondaryTable.size(),
            joinKey);

        if (begin == end && isLeft) {
            begin = &nullRow;
            end = begin + 1;
        }

        for (auto it = begin; it != end; ++it) {
            auto groupKey = TGroupKey{
                evaluate(primaryRow, groupEq.X),
                evaluate(primaryRow, groupEq.Y),
                evaluate(primaryRow, groupEq.Z)};

            auto groupValue = TAggregates{1, primaryRow.v, it->w};

            bool limitReached = lookup.size() >= offset + limit;

            if (limitReached) {
                auto found = lookup.find(groupKey);

                if (found != lookup.end()) {
                    found->second += groupValue;
                }
            } else {
                auto [pos, inserted] = lookup.emplace(groupKey, groupValue);

                if (!inserted) {
                    pos->second += groupValue;
                } else {
                    groupedKeys.push_back(groupKey);
                }
            }
        }
    }

    std::map<TString, TDataSplit> splits;
    std::vector<std::vector<TString>> sources(2);

    auto primarySplit = MakeSplit({
        {"a", EValueType::Int64, ESortOrder::Ascending},
        {"b", EValueType::Int64, ESortOrder::Ascending},
        {"c", EValueType::Int64, ESortOrder::Ascending},
        {"v", EValueType::Int64}
    }, 0);

    splits["//t"] = primarySplit;

    for (const auto& row : primaryTable) {
        sources[0].push_back(Format("a=%v;b=%v;c=%v;v=%v", row.a, row.b, row.c, row.v));
    }

    auto secondarySplit = MakeSplit({
        {"d", EValueType::Int64, ESortOrder::Ascending},
        {"e", EValueType::Int64, ESortOrder::Ascending},
        {"w", EValueType::Int64}
    }, 0);

    splits["//s"] = secondarySplit;

    for (const auto& row : secondaryTable) {
        sources[1].push_back(Format("d=%v;e=%v;w=%v", row.d, row.e, row.w));
    }

    auto resultSplit = MakeSplit({
        {"x", EValueType::Int64},
        {"y", EValueType::Int64},
        {"z", EValueType::Int64},
        {"count", EValueType::Int64},
        {"sumv", EValueType::Int64},
        {"sumw", EValueType::Int64},
    });

    std::vector<TResultRow> resultData;
    TAggregates totals;

    for (size_t index = offset; index < std::min(offset + limit, groupedKeys.size()); ++index) {
        auto groupKey = groupedKeys[index];
        auto groupValue = lookup[groupKey];

        resultData.push_back(TResultRow{
            {groupKey.x, groupKey.y, groupKey.z},
            {groupValue.count, groupValue.sumv, groupValue.sumw}});

        totals += groupValue;
    }

    std::vector<TOwningRow> result;

    for (const auto& row : resultData) {
        auto resultRow = Format(
            "x=%v;y=%v;z=%v;count=%v;sumv=%v;sumw=%v",
            row.x,
            row.y,
            row.z,
            row.count,
            row.sumv,
            row.sumw);

        result.push_back(YsonToRow(resultRow, resultSplit, true));
    }

    if (groupTotals == "with totals" && !resultData.empty()) {
        auto rowString = Format(
            "count=%v;sumv=%v;sumw=%v",
            totals.count,
            totals.sumv,
            totals.sumw);
        result.push_back(YsonToRow(rowString, resultSplit, true));
    }

    auto resultMatcher = [&] (TRange<TRow> actualResult, const TTableSchema& /*tableSchema*/) {
        EXPECT_EQ(result.size(), actualResult.Size());

        bool print = false;

        for (int i = 0; i < std::ssize(result); ++i) {
            print |= actualResult[i] != result[i];
            EXPECT_EQ(actualResult[i], result[i]);
        }
        if (print) {
            Cout << "expectedResult:" << Endl;
            for (int i = 0; i < std::ssize(result); ++i) {
                Cout << ToString(result[i]) << Endl;
            }

            Cout << "actualResult:" << Endl;
            for (int i = 0; i < std::ssize(actualResult); ++i) {
                Cout << ToString(actualResult[i]) << Endl;
            }

            Cout << "primary:" << Endl;
            for (const auto& row : primaryTable) {
                Cout << Format("{%v, %v, %v, %v},", row.a, row.b, row.c, row.v) << Endl;
            }

            Cout << "secondary:" << Endl;
            for (const auto& row : secondaryTable) {
                Cout << Format("{%v, %v, %v},", row.d, row.e, row.w) << Endl;
            }
        }
    };

    auto query = Evaluate(
        queryString,
        splits,
        sources,
        resultMatcher);

    EXPECT_TRUE(query->IsOrdered());
}

TEST_P(TQueryEvaluateComplexTest, All)
{
    int M = 7;

    const auto& param = GetParam();

    TStringBuf joinType = std::get<0>(param);
    TJoinColumns joinEq = std::get<1>(param);
    TGroupColumns groupEq = std::get<2>(param);
    TStringBuf groupTotals = std::get<3>(param);

    for (size_t repeat = 0; repeat < 10; ++repeat) {
        std::vector<TPrimaryRow> primaryTable;
        std::vector<TSecondaryRow> secondaryTable;

        for (int i = 0; i < M * M * M; ++i) {
            if (rand() % 7 == 0) {
                continue;
            }
            primaryTable.push_back({{i / (M * M), i % (M * M) / M, i % M}, {i}});
        }

        for (int i = 0; i < M * M; ++i) {
            if (rand() % 7 == 0) {
                continue;
            }
            secondaryTable.push_back({{i / M, i % M}, {i}});
        }

        auto shareSize = primaryTable.size() / 3;
        auto offset = rand() % shareSize;
        auto limit = rand() % shareSize + 1;

        DoTest(primaryTable, secondaryTable, offset, limit, joinType, joinEq, groupEq, groupTotals);
    }
}

INSTANTIATE_TEST_SUITE_P(1, TQueryEvaluateComplexTest,
    ::testing::Combine(
        ::testing::ValuesIn({
            "",
            "left"
        }), // join type
        ::testing::ValuesIn({
            TJoinColumns{"a, b", "d, e"},
            TJoinColumns{"b, c", "d, e"},
            TJoinColumns{"a, c", "d, e"},
            TJoinColumns{"c, a", "d, e"},
        }), // join equation
        ::testing::ValuesIn({
            TGroupColumns{"a", "0", "0"},
            TGroupColumns{"a", "b", "0"},
            TGroupColumns{"a", "b", "c"},
            TGroupColumns{"a", "b", "c % 3"},
            TGroupColumns{"a", "(b * 10 + c) % 3", "0"}
        }), // group by
        ::testing::ValuesIn({
            "",
            "with totals"
        })));

////////////////////////////////////////////////////////////////////////////////

TEST_F(TQueryEvaluateTest, QuotedColumnNames)
{
    {
        auto split = MakeSplit({{"column ]]] \n \t \x42 \u2019 ` ", EValueType::Int64}});
        auto source = std::vector<TString>{
            R"("column ]]] \n \t \x42 \u2019 ` "=4)",
            R"("column ]]] \n \t \x42 \u2019 ` "=10)",
        };

        auto result = YsonToRows(source, split);

        Evaluate("`column ]]] \\n \\t \\x42 \\u2019 \\` ` FROM `//t`", split, source, ResultMatcher(result));
    }

    {
        auto split = MakeSplit({{"where", EValueType::Int64}});
        auto source = std::vector<TString>{
            R"("where"=4)",
            R"("where"=10)",
        };

        auto result = YsonToRows(source, split);

        Evaluate("`where` FROM `//t`", split, source, ResultMatcher(result));
    }
}

////////////////////////////////////////////////////////////////////////////////

class TQueryEvaluatePlaceholdersTest
    : public TQueryEvaluateTest
    , public ::testing::WithParamInterface<std::tuple<
        const char*, // query
        const char*, // placeholders
        std::vector<TOwningRow>>> // result
{ };

TEST_P(TQueryEvaluatePlaceholdersTest, Simple)
{
    auto split = MakeSplit({{"a", EValueType::Int64}, {"b", EValueType::Int64}, });
    auto source = std::vector<TString>{
        R"(a=1;b=2)",
        R"(a=3;b=4)",
        R"(a=5;b=6)",
        R"(a=7;b=8)",
    };

    const auto& args = GetParam();
    const auto& query = std::get<0>(args);
    const auto& placeholders = NYson::TYsonStringBuf(std::get<1>(args));
    const auto& result = std::get<2>(args);

    Evaluate(
        query,
        split,
        source,
        ResultMatcher(result),
        std::numeric_limits<i64>::max(),
        std::numeric_limits<i64>::max(),
        placeholders);
}

INSTANTIATE_TEST_SUITE_P(
    QueryEvaluatePlaceholdersTest,
    TQueryEvaluatePlaceholdersTest,
    ::testing::Values(
        std::make_tuple(
            "a from [//t] where (a, b) = {tuple}",
            "{tuple=[3;4]}",
            YsonToRows({"a=3"}, MakeSplit({{"a", EValueType::Int64}}))),
        std::make_tuple(
            "a from [//t] where (a, b) > ({a}, {b})",
            "{a=5;b=5}",
            YsonToRows({"a=5", "a=7"}, MakeSplit({{"a", EValueType::Int64}}))),
        std::make_tuple(
            "a from [//t] where a in {tuple}",
            "{tuple=[1;7]}",
            YsonToRows({"a=1", "a=7"}, MakeSplit({{"a", EValueType::Int64}}))),
        std::make_tuple(
            "concat({prefix}, numeric_to_string(a)) as c from [//t] where a = {a}",
            "{prefix=p;a=1}",
            YsonToRows({R"(c="p1")"}, MakeSplit({{"c", EValueType::String}}))),
        std::make_tuple(
            "concat({prefix}, numeric_to_string(a)) as c from [//t] where a = {a}",
            R"({prefix="{a}";a=1})",
            YsonToRows({R"(c="{a}1")"}, MakeSplit({{"c", EValueType::String}}))),
        std::make_tuple(
            "transform(b, {from}, ({first_to}, {second_to})) as c from [//t] where a = {a}",
            "{from=[2;4];first_to=42;second_to=-5;a=1}",
            YsonToRows({"c=42"}, MakeSplit({{"c", EValueType::Int64}})))));

TEST_F(TQueryEvaluatePlaceholdersTest, Complex)
{
    {
        auto split = MakeSplit({{"a", EValueType::String}, {"b", EValueType::String}, });
        auto source = std::vector<TString>{ R"(a="1";b="2")", };
        auto result = std::vector<TOwningRow>{};
        Evaluate(
            "b from [//t] where a = {a}",
            split,
            source,
            ResultMatcher(result),
            std::numeric_limits<i64>::max(),
            std::numeric_limits<i64>::max(),
            NYson::TYsonStringBuf{"{a=\"42\\\" or \\\"1\\\" = \\\"1\"}"sv});
    }

    {
        auto split = MakeSplit({{"a", EValueType::Int64}, {"b", EValueType::Int64}, });
        auto source = std::vector<TString>{
            R"(a=1;b=2)",
            R"(a=3;b=4)",
            R"(a=5;b=6)",
            R"(a=7;b=8)",
        };

        EXPECT_THROW_THAT(
            Evaluate(
                "b from [//t] where a = {a}",
                split,
                source,
                [] (TRange<TRow> /*result*/, const TTableSchema& /*tableSchema*/) { },
                std::numeric_limits<i64>::max(),
                std::numeric_limits<i64>::max(),
                NYson::TYsonStringBuf{"{a=\"42 or 1 = 1\"}"sv}),
            HasSubstr("Type mismatch in expression"));
    }

    SUCCEED();
}

class TQueryEvaluatePlaceholdersWithIncorrectSyntaxTest
    : public TQueryPrepareTest
    , public ::testing::WithParamInterface<std::tuple<
        const char*, // query
        const char*, // placeholders
        const char*>> // error message
{ };

TEST_P(TQueryEvaluatePlaceholdersWithIncorrectSyntaxTest, Simple)
{
    const auto& args = GetParam();
    const auto& query = std::get<0>(args);
    const auto& placeholders = NYson::TYsonStringBuf(std::get<1>(args));
    const auto& errorMessage = std::get<2>(args);

    ExpectPrepareThrowsWithDiagnostics(
        query,
        HasSubstr(errorMessage),
        placeholders);
}

INSTANTIATE_TEST_SUITE_P(
    QueryEvaluatePlaceholdersWithIncorrectSyntaxTest,
    TQueryEvaluatePlaceholdersWithIncorrectSyntaxTest,
    ::testing::Values(
        std::make_tuple(
            "a from [//t] where a = {a}",
            "{}",
            "Placeholder was not found"),
        std::make_tuple(
            "a from [//t] where a = {a}",
            "{a=}",
            "Error occurred while parsing YSON"),
        std::make_tuple(
            "a from [//t] where a = {a}",
            "{a=<attribute=attribute>42}",
            "Incorrect placeholder map: values should be plain types or lists"),
        std::make_tuple(
            "a from [//t] where a = {a}",
            "{a=[<attribute=attribute>42]}",
            "Attributes inside YSON placeholder are not allowed"),
        std::make_tuple(
            "a from [//t] where a = {a}",
            "{a={b=42}}",
            "Incorrect placeholder map: values should be plain types or lists"),
        std::make_tuple(
            "a from [//t] where a = {a}",
            "{a=[{b=42}]}",
            "Maps inside YSON placeholder are not allowed"),
        std::make_tuple(
            "a from [//t] where a = {a}",
            "{a={b=42}}",
            "Incorrect placeholder map: values should be plain types or lists"),
        std::make_tuple(
            "a from [//t] where a = {a}",
            "[42;]",
            "Incorrect placeholder argument: YSON map expected"),
        std::make_tuple(
            "a from [//t] where a = {a} incorrect query",
            "{a=42}",
            "{a}  >>>>> incorrect <<<<<  query"),
        std::make_tuple(
            "a from [//t] where a = {}",
            "{}",
            "a =  >>>>> { <<<<< }"),
        std::make_tuple(
            "a from [//t] where a = {{a}}",
            "{}",
            "a =  >>>>> { <<<<< {a}}"),
        std::make_tuple(
            "a from {t} where a = {a}",
            "{t=table_name;a=42}",
            "from  >>>>> {t} <<<<<  where"),
        std::make_tuple(
            "a from [//T] where a = {a} {b}",
            "{b=b;a=42}",
            "{a}  >>>>> {b} <<<<<")));

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NQueryClient
