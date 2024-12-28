#include "config.h"
#include "profiler.h"
#include "query_tracker_proxy.h"

#include <yt/yt/client/api/transaction.h>

#include <yt/yt/client/chunk_client/data_statistics.h>

#include <yt/yt/client/query_client/query_builder.h>

#include <yt/yt/client/table_client/record_helpers.h>
#include <yt/yt/client/table_client/wire_protocol.h>

#include <yt/yt/client/transaction_client/timestamp_provider.h>

#include <yt/yt/ytlib/api/native/client.h>

#include <yt/yt/ytlib/cypress_client/cypress_ypath_proxy.h>

#include <yt/yt/ytlib/object_client/object_service_proxy.h>

#include <yt/yt/ytlib/query_tracker_client/records/query.record.h>
#include <yt/yt/ytlib/query_tracker_client/helpers.h>

#include <yt/yt/core/logging/log.h>

#include <yt/yt/core/ytree/convert.h>

#include <library/cpp/iterator/functools.h>

#include <contrib/libs/pfr/include/pfr/tuple_size.hpp>

namespace NYT::NQueryTracker {

using namespace NApi;
using namespace NChunkClient::NProto;
using namespace NConcurrency;
using namespace NCypressClient;
using namespace NLogging;
using namespace NObjectClient;
using namespace NQueryTrackerClient;
using namespace NQueryTrackerClient::NRecords;
using namespace NSecurityClient;
using namespace NTableClient;
using namespace NTransactionClient;
using namespace NYPath;
using namespace NYTree;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

static TLogger Logger("QueryTrackerProxy");

namespace NDetail {

// This ACO is the same as all others, but does not affect ListQueries. This is needed to enable link sharing.
constexpr TStringBuf EveryoneShareAccessControlObject = "everyone-share";

constexpr int MaxAccessControlObjectsPerQuery = 10;

// Path to access control object namespace for QT.
constexpr TStringBuf QueriesAcoNamespacePath = "//sys/access_control_object_namespaces/queries";

const TString FinishedQueriesByAcoAndStartTimeTable = "finished_queries_by_aco_and_start_time";
const TString FinishedQueriesByUserAndStartTimeTable = "finished_queries_by_user_and_start_time";

TQuery PartialRecordToQuery(const auto& partialRecord)
{
    static_assert(pfr::tuple_size<TQuery>::value == 16);
    static_assert(TActiveQueryDescriptor::FieldCount == 20);
    static_assert(TFinishedQueryDescriptor::FieldCount == 14);

    TQuery query;
    // Note that some of the fields are twice optional.
    // First time due to the fact that they are optional in the record,
    // and second time due to the extra optionality of any field in the partial record.
    // TODO(max42): coalesce the optionality of the fields in the partial record.
    query.Id = partialRecord.Key.QueryId;
    query.Engine = partialRecord.Engine;
    query.Query = partialRecord.Query;
    query.Files = partialRecord.Files.value_or(std::nullopt);
    query.StartTime = partialRecord.StartTime;
    query.FinishTime = partialRecord.FinishTime.value_or(std::nullopt);
    query.Settings = partialRecord.Settings.value_or(TYsonString());
    query.User = partialRecord.User;
    query.AccessControlObjects = partialRecord.AccessControlObjects.value_or(TYsonString(TString("[]")));
    query.State = partialRecord.State;
    query.ResultCount = partialRecord.ResultCount.value_or(std::nullopt);
    query.Progress = partialRecord.Progress.value_or(TYsonString());
    query.Error = partialRecord.Error.value_or(std::nullopt);
    query.Annotations = partialRecord.Annotations.value_or(TYsonString());

    IAttributeDictionaryPtr otherAttributes;
    auto fillIfPresent = [&] (const TString& key, const auto& value) {
        if (value) {
            if (!otherAttributes) {
                otherAttributes = CreateEphemeralAttributes();
            }
            otherAttributes->Set(key, *value);
        }
    };

    if constexpr (std::is_same_v<std::decay_t<decltype(partialRecord)>, TActiveQueryPartial>) {
        fillIfPresent("abort_request", partialRecord.AbortRequest.value_or(std::nullopt));
        fillIfPresent("incarnation", partialRecord.Incarnation);
        fillIfPresent("lease_transaction_id", partialRecord.LeaseTransactionId);
        fillIfPresent("assigned_tracker", partialRecord.AssignedTracker);
    }

    query.OtherAttributes = std::move(otherAttributes);

    return query;
}

//! Lookup one of query tracker state tables by query id.
template <class TRecordDescriptor>
TFuture<typename TRecordDescriptor::TRecordPartial> LookupQueryTrackerRecord(
    TQueryId queryId,
    const IClientPtr& client,
    const TString& tablePath,
    const TString& tableKind,
    const std::optional<std::vector<TString>>& lookupKeys,
    TTimestamp timestamp)
{
    auto rowBuffer = New<TRowBuffer>();
    const auto& nameTable = TRecordDescriptor::Get()->GetNameTable();

    TLookupRowsOptions lookupOptions;
    if (lookupKeys) {
        std::vector<int> columnIds;
        for (const auto& key : *lookupKeys) {
            if (auto columnId = nameTable->FindId(key)) {
                columnIds.push_back(*columnId);
            }
        }
        lookupOptions.ColumnFilter = TColumnFilter(columnIds);
    }
    lookupOptions.Timestamp = timestamp;
    lookupOptions.KeepMissingRows = true;
    std::vector keys{
        TActiveQueryKey{.QueryId = queryId}.ToKey(rowBuffer),
    };
    auto asyncLookupResult = client->LookupRows(
        tablePath,
        TRecordDescriptor::Get()->GetNameTable(),
        MakeSharedRange(std::move(keys), rowBuffer),
        lookupOptions);
    auto asyncRecord = asyncLookupResult.Apply(BIND([=] (const TUnversionedLookupRowsResult& result) {
        auto optionalRecords = ToOptionalRecords<typename TRecordDescriptor::TRecordPartial>(result.Rowset);
        YT_VERIFY(optionalRecords.size() == 1);
        if (!optionalRecords[0]) {
            THROW_ERROR_EXCEPTION("Query %v is not found in %Qv query table", queryId, tableKind);
        }
        return *optionalRecords[0];
    }));
    return asyncRecord;
};

THashSet<TString> GetUserSubjects(const std::string& user, const IClientPtr& client)
{
    // Get all subjects for the user.
    TGetNodeOptions options;
    options.ReadFrom = EMasterChannelKind::Cache;
    options.SuccessStalenessBound = TDuration::Minutes(1);
    auto userSubjectsOrError = WaitFor(client->GetNode("//sys/users/" + user + "/@member_of_closure", options));
    if (!userSubjectsOrError.IsOK()) {
        THROW_ERROR_EXCEPTION("Error while fetching user membership for the user %Qv", user)
            << userSubjectsOrError;
    }
    auto userSubjects = ConvertTo<THashSet<TString>>(userSubjectsOrError.Value());
    return userSubjects;
}

ESecurityAction CheckAccessControl(
    const std::string& user,
    const std::optional<TYsonString>& accessControlObjects,
    const TString& queryAuthor,
    const IClientPtr& client,
    EPermission permission)
{
    if (user == queryAuthor) {
        return ESecurityAction::Allow;
    }

    auto userSubjects = GetUserSubjects(user, client);
    if (userSubjects.contains(NSecurityClient::SuperusersGroupName)) {
        return NSecurityClient::ESecurityAction::Allow;
    }

    auto accessControlObjectList = ConvertTo<std::optional<std::vector<TString>>>(accessControlObjects);
    if (!accessControlObjectList) {
        return NSecurityClient::ESecurityAction::Deny;
    }

    TCheckPermissionOptions checkPermissionOptions;
    checkPermissionOptions.ReadFrom = EMasterChannelKind::Cache;
    checkPermissionOptions.SuccessStalenessBound = TDuration::Minutes(1);
    for (const auto& accessControlObject: *accessControlObjectList) {
        auto path = Format(
            "%v/%v/principal",
            QueriesAcoNamespacePath,
            NYPath::ToYPathLiteral(accessControlObject));

        auto securityAction = WaitFor(client->CheckPermission(user, path, permission, checkPermissionOptions))
            .ValueOrThrow()
            .Action;

        if (securityAction == NSecurityClient::ESecurityAction::Allow) {
            return NSecurityClient::ESecurityAction::Allow;
        }
    }

    return NSecurityClient::ESecurityAction::Deny;
}

void ThrowAccessDeniedException(
    TQueryId queryId,
    EPermission permission,
    const std::string& user,
    const std::optional<TYsonString>& accessControlObjects,
    const TString& queryAuthor)
{
    THROW_ERROR_EXCEPTION(NSecurityClient::EErrorCode::AuthorizationError,
        "Access denied to query %v due to missing %Qv permission",
        queryId,
        permission)
        << TErrorAttribute("user", user)
        << TErrorAttribute("access_control_objects", accessControlObjects)
        << TErrorAttribute("query_author", queryAuthor);
}

//! Lookup a query in active_queries and finished_queries tables by query id.
TQuery LookupQuery(
    TQueryId queryId,
    const IClientPtr& client,
    const TString& root,
    const std::optional<std::vector<TString>>& lookupKeys,
    TTimestamp timestamp,
    const TLogger& logger)
{
    auto asyncActiveRecord = LookupQueryTrackerRecord<TActiveQueryDescriptor>(
        queryId,
        client,
        root + "/active_queries",
        "active",
        lookupKeys,
        timestamp);
    auto asyncFinishedRecord = LookupQueryTrackerRecord<TFinishedQueryDescriptor>(
        queryId,
        client,
        root + "/finished_queries",
        "finished",
        lookupKeys,
        timestamp);

    auto error = WaitFor(AnySucceeded(std::vector{asyncActiveRecord.AsVoid(), asyncFinishedRecord.AsVoid()}));
    if (!error.IsOK()) {
        THROW_ERROR_EXCEPTION(NQueryTrackerClient::EErrorCode::QueryNotFound,
            "Query %v is not found neither in active nor in finished query tables",
            queryId)
            << error;
    }
    bool isActive = asyncActiveRecord.IsSet() && asyncActiveRecord.Get().IsOK();
    bool isFinished = asyncFinishedRecord.IsSet() && asyncFinishedRecord.Get().IsOK();
    YT_VERIFY(isActive || isFinished);
    if (isActive && isFinished) {
        const auto& Logger = logger;
        YT_LOG_ALERT(
            "Query is found in both active and finished query tables "
            "(QueryId: %v, Timestamp: %v)",
            queryId,
            timestamp);
    }
    if (isActive) {
        return PartialRecordToQuery(asyncActiveRecord.Get().Value());
    } else {
        return PartialRecordToQuery(asyncFinishedRecord.Get().Value());
    }
}

void ValidateQueryPermissions(
    TQueryId queryId,
    const TString& root,
    TTimestamp timestamp,
    const std::string& user,
    const IClientPtr& client,
    EPermission permission,
    const TLogger& logger)
{
    std::vector<TString> lookupKeys = {"user", "access_control_objects"};
    auto query = LookupQuery(queryId, client, root, lookupKeys, timestamp, logger);
    if (CheckAccessControl(user, query.AccessControlObjects, *query.User, client, permission) == ESecurityAction::Deny) {
        ThrowAccessDeniedException(queryId, permission, user, query.AccessControlObjects, *query.User);
    }
}

std::vector<TString> GetAcosForSubjects(const THashSet<TString>& subjects, bool filterEveryoneShareAco, const IClientPtr& client)
{
    // Get all access control objects.
    TGetNodeOptions options;
    options.Attributes = {
        "principal_acl",
    };
    options.ReadFrom = EMasterChannelKind::Cache;
    options.SuccessStalenessBound = TDuration::Minutes(1);

    auto allAcosOrError = WaitFor(client->GetNode(TString(QueriesAcoNamespacePath), options));

    if (!allAcosOrError.IsOK()) {
        THROW_ERROR_EXCEPTION(
            "Error while fetching all access control objects in the namespace \"queries\". "
            "Please make sure that the namespace exists")
            << allAcosOrError;
    }

    auto allAcos = ConvertToNode(allAcosOrError.Value())->AsMap()->GetChildren();

    std::vector<TString> acosForUser;
    // We expect average user to have access to a small number of access control objects.
    acosForUser.reserve(10);

    for (const auto& aco : allAcos) {
        auto acoName = aco.first;

        if (filterEveryoneShareAco && acoName == EveryoneShareAccessControlObject) {
            continue;
        }

        auto aclRules = ConvertToNode(aco.second->Attributes().GetYson("principal_acl"))->AsList()->GetChildren();
        bool allowUseRuleFound = false;
        bool denyUseRuleFound = false;
        // Check if there are allow or deny "Use" rules matching the subjects.
        for (const auto& aclRule : aclRules) {
            auto aclSubjects = aclRule->AsMap()->GetChildOrThrow("subjects")->AsList()->GetChildren();
            auto aclPermissions = aclRule->AsMap()->GetChildOrThrow("permissions")->AsList()->GetChildren();
            bool usePermissionFound = false;
            for (const auto& aclPermission : aclPermissions) {
                auto aclPermissionName = aclPermission->GetValue<TString>();
                aclPermissionName.to_lower();
                if (aclPermissionName == "use") {
                    usePermissionFound = true;
                    break;
                }
            }
            if (!usePermissionFound) {
                continue;
            }
            for (const auto& aclSubject : aclSubjects) {
                auto aclSubjectName = aclSubject->GetValue<TString>();
                if (subjects.find(aclSubjectName) != subjects.end()) {
                    auto aclAction = aclRule->AsMap()->GetChildOrThrow("action")->GetValue<TString>();
                    aclAction.to_lower();
                    if (aclAction == "allow") {
                        allowUseRuleFound = true;
                    } else if (aclAction == "deny") {
                        denyUseRuleFound = true;
                    }
                }
            }
        }
        if (allowUseRuleFound && !denyUseRuleFound) {
            acosForUser.emplace_back(acoName);
        }
    }
    return acosForUser;
}

void VerifyAllAccessControlObjectsExist(const std::vector<TString>& accessControlObjects, const IClientPtr& client)
{
    TNodeExistsOptions nodeExistsOptions;
    nodeExistsOptions.ReadFrom = EMasterChannelKind::Cache;
    nodeExistsOptions.SuccessStalenessBound = TDuration::Minutes(1);

    std::vector<TFuture<bool>> futures;
    for (const auto& accessControlObject : accessControlObjects) {
        futures.push_back(client->NodeExists(Format("%v/%v", QueriesAcoNamespacePath, ToYPathLiteral(accessControlObject)), nodeExistsOptions)
            .Apply(BIND([accessControlObject] (const TErrorOr<bool>& rspOrError) {
                if (!rspOrError.IsOK()) {
                    THROW_ERROR_EXCEPTION("Failed to check whether access control object %Qv exists", accessControlObject)
                        << rspOrError;
                }

                if (!rspOrError.Value()) {
                    THROW_ERROR_EXCEPTION(NYTree::EErrorCode::ResolveError,
                        "Access control object %Qv does not exist",
                        accessControlObject);
                }

                return true;
            })));
    }

    WaitFor(AllSucceeded(futures))
        .ThrowOnError();
}

IUnversionedRowsetPtr FilterRowsetColumns(IUnversionedRowsetPtr rowset, std::vector<std::string> columns)
{
    const auto& schema = rowset->GetSchema();
    const auto& rows = rowset->GetRows();
    auto rowBuffer = New<TRowBuffer>();
    auto nameTable = TNameTable::FromSchema(*schema);
    std::vector<int> columnIndexes;
    THashSet<int> columnIndexSet;
    for (const auto& column : columns) {
        if (auto id = nameTable->FindId(column); id && columnIndexSet.insert(*id).second) {
            columnIndexes.push_back(*id);
        }
    }
    TColumnFilter columnFilter(columnIndexes);

    std::vector<TUnversionedRow> newRows;
    newRows.reserve(rows.size());
    for (const auto& row : rows) {
        auto newRow = rowBuffer->AllocateUnversioned(columnIndexes.size());
        for (auto [newIndex, index] : Enumerate(columnIndexes)) {
            auto value = row[index];
            value.Id = newIndex;
            newRow[newIndex] = rowBuffer->CaptureValue(value);
        }
        newRows.emplace_back(newRow);
    }

    auto newSchema = schema->Filter(columnFilter);

    return CreateRowset(newSchema, MakeSharedRange(std::move(newRows), std::move(rowBuffer)));
}

std::vector<TQuery> PartialRecordsToQueries(const auto& partialRecords)
{
    std::vector<TQuery> queries;
    queries.reserve(partialRecords.size());
    for (const auto& partialRecord : partialRecords) {
        queries.push_back(PartialRecordToQuery(partialRecord));
    }
    return queries;
}

std::optional<std::vector<TString>> ValidateAccessControlObjects(const std::optional<TString>& accessControlObject, const std::optional<std::vector<TString>>& accessControlObjects)
{
    if (!accessControlObjects && !accessControlObject) {
        return std::nullopt;
    }

    if (accessControlObjects && accessControlObject) {
        THROW_ERROR_EXCEPTION("Only one of \"access_control_objects\", \"access_control_object\" should be specified");
    }

    if (accessControlObject) {
        return std::vector<TString>({ *accessControlObject });
    }

    if (accessControlObjects->size() > MaxAccessControlObjectsPerQuery) {
        THROW_ERROR_EXCEPTION(NQueryTrackerClient::EErrorCode::TooManyAcos,
            "Too many ACOs in query: limit is %v, actual count is %v",
                MaxAccessControlObjectsPerQuery,
                accessControlObjects->size());
    }
    return accessControlObjects;
}

void AddFilterConditions(
    NQueryClient::TQueryBuilder& builder,
    TYsonString& placeholderValues,
    const TListQueriesOptions& options,
    const std::string& user,
    const std::vector<TString>& acosForUser,
    const TString& table,
    bool isSuperuser)
{
    std::optional<i64> fromTime = options.FromTime ? std::make_optional<i64>(options.FromTime->MicroSeconds()) : std::nullopt;
    if (options.CursorDirection == EOperationSortDirection::Future && options.CursorTime) {
        fromTime = i64(options.CursorTime->MicroSeconds()) + 1;
    }

    std::optional<i64> toTime = options.ToTime ? std::make_optional<i64>(options.ToTime->MicroSeconds()) : std::nullopt;
    if (options.CursorDirection == EOperationSortDirection::Past && options.CursorTime) {
        toTime = i64(options.CursorTime->MicroSeconds()) - 1;
    }

    auto placeholdersFluentMap = BuildYsonNodeFluently().BeginMap();
    if (options.UserFilter) {
        builder.AddWhereConjunct("[user] = {UserFilter}");
        placeholdersFluentMap.Item("UserFilter").Value(*options.UserFilter);
    }
    if (options.EngineFilter) {
        builder.AddWhereConjunct("[engine] = {EngineFilter}");
        placeholdersFluentMap.Item("EngineFilter").Value(*options.EngineFilter);
    }
    if (options.StateFilter) {
        builder.AddWhereConjunct("[state] = {StateFilter}");
        placeholdersFluentMap.Item("StateFilter").Value(*options.StateFilter);
    }
    if (options.SubstrFilter) {
        builder.AddWhereConjunct("is_substr({SubstrFilter}, filter_factors)");
        placeholdersFluentMap.Item("SubstrFilter").Value(*options.SubstrFilter);
    }
    if (table == "active_queries") {
        if (fromTime) {
            builder.AddWhereConjunct("[start_time] >= " + ToString(fromTime));
        }
        if (toTime) {
            builder.AddWhereConjunct("[start_time] <= " + ToString(toTime));
        }

        if (!isSuperuser) {
            placeholdersFluentMap.Item("User").Value(user);

            TStringBuilder conditionBuilder;
            TDelimitedStringBuilderWrapper delimitedBuilder(&conditionBuilder, " OR ");

            delimitedBuilder->AppendString("user = {User}");
            for (const auto& aco : acosForUser) {
                delimitedBuilder->AppendString(Format("list_contains(access_control_objects, \"%v\")", aco));
            }

            builder.AddWhereConjunct(conditionBuilder.Flush());
        }
    } else {
        if (fromTime) {
            builder.AddWhereConjunct("-[minus_start_time] >= " + ToString(fromTime));
        }
        if (toTime) {
            builder.AddWhereConjunct("-[minus_start_time] <= " + ToString(toTime));
        }

        if (!isSuperuser) {
            if (table == FinishedQueriesByUserAndStartTimeTable) {
                placeholdersFluentMap.Item("User").Value(user);
                builder.AddWhereConjunct("user = {User}");
            } else if (table == FinishedQueriesByAcoAndStartTimeTable) {
                placeholdersFluentMap.Item("acosForUser").DoListFor(acosForUser, [] (TFluentList fluent, const TString& aco) {
                    fluent.Item().Value(aco);
                });
                builder.AddWhereConjunct("access_control_object IN {acosForUser}");
            }
        }
    }

    placeholderValues = ConvertToYsonString(placeholdersFluentMap.EndMap());
}

template <typename TRecord>
void GetQueriesByStartTime(
    const NApi::IClientPtr client,
    const TString& root,
    const TListQueriesOptions& requestOptions,
    const TTimestamp timestamp,
    const std::string& user,
    const std::vector<TString>& acosForUser,
    const TString& tableName,
    std::vector<std::pair<TTimestamp, TQueryId>>& results,
    bool isSuperuser = false)
{
    NQueryClient::TQueryBuilder builder;
    TYsonString placeholderValues;
    builder.SetSource(root + "/" + tableName);
    AddFilterConditions(builder, placeholderValues, requestOptions, user, acosForUser, tableName, isSuperuser);
    builder.AddSelectExpression("[minus_start_time]");
    builder.AddSelectExpression("[query_id]");
    // We request for one more in order to understand whether the whole result fits into the limit.
    builder.SetLimit(requestOptions.Limit + 1);
    if (requestOptions.CursorDirection == EOperationSortDirection::Past) {
        builder.AddOrderByAscendingExpression("minus_start_time");
    } else if (requestOptions.CursorDirection == EOperationSortDirection::Future) {
        builder.AddOrderByDescendingExpression("minus_start_time");
    }
    if (tableName == FinishedQueriesByAcoAndStartTimeTable) {
        // We need to combine the same query with different acos into one record
        builder.AddGroupByExpression("minus_start_time");
        builder.AddGroupByExpression("query_id");
    }

    TSelectRowsOptions options;
    options.Timestamp = timestamp;
    options.PlaceholderValues = placeholderValues;
    auto query = builder.Build();
    YT_LOG_DEBUG("Selecting finished queries by start time (Query: %v, Table: %v)", query, tableName);
    auto selectResult = WaitFor(client->SelectRows(query, options))
        .ValueOrThrow();

    for (const auto& record : ToRecords<TRecord>(selectResult.Rowset)) {
        results.push_back({-record.Key.MinusStartTime, record.Key.QueryId});
    }
}

void ConvertAcoToOldFormat(TQuery& query)
{
    auto accessControlObjectList = ConvertTo<std::optional<std::vector<TString>>>(query.AccessControlObjects);

    if (!accessControlObjectList || accessControlObjectList->empty()) {
        return;
    }

    if (accessControlObjectList->size() == 1) {
        query.AccessControlObject = (*accessControlObjectList)[0];
    }
}

std::pair<std::vector<TString>, std::vector<TString>> GetAccessControlDiff(const std::vector<TString>& before, const std::optional<std::vector<TString>>& after)
{
    std::vector<TString> toDelete, toInsert;
    if (!after) {
        return {toDelete, toInsert};
    }

    THashSet<TString> beforeSet(before.begin(), before.end());
    THashSet<TString> afterSet(after->begin(), after->end());

    for (const auto& aco : before) {
        if (!afterSet.contains(aco)) {
            toDelete.push_back(aco);
        }
    }
    for (const auto& aco : *after) {
        if (!beforeSet.contains(aco)) {
            toInsert.push_back(aco);
        }
    }

    return {toDelete, toInsert};
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDetail

using namespace NDetail;

////////////////////////////////////////////////////////////////////////////////

TQueryTrackerProxy::TQueryTrackerProxy(
    IClientPtr stateClient,
    TYPath stateRoot,
    TQueryTrackerProxyConfigPtr config
)
    : StateClient_(std::move(stateClient))
    , StateRoot_(std::move(stateRoot))
    , ProxyConfig_(std::move(config))
{ }

void TQueryTrackerProxy::Reconfigure(const TQueryTrackerProxyConfigPtr& config)
{
    ProxyConfig_ = config;
}

void TQueryTrackerProxy::StartQuery(
    const TQueryId queryId,
    const EQueryEngine engine,
    const TString& query,
    const TStartQueryOptions& options,
    const std::string& user)
{
    if (ssize(options.Files) > ProxyConfig_->MaxQueryFileCount) {
        THROW_ERROR_EXCEPTION("Too many files: limit is %v, actual count is %v",
            ProxyConfig_->MaxQueryFileCount,
            options.Files.size());
    }
    for (const auto& file : options.Files) {
        if (ssize(file->Name) > ProxyConfig_->MaxQueryFileNameSizeBytes) {
            THROW_ERROR_EXCEPTION("Too large file %v name: limit is %v, actual size is %v",
                file->Name,
                ProxyConfig_->MaxQueryFileNameSizeBytes,
                file->Name.size());
        }
        if (ssize(file->Content) > ProxyConfig_->MaxQueryFileContentSizeBytes) {
            THROW_ERROR_EXCEPTION("Too large file %v content: limit is %v, actual size is %v",
                file->Name,
                ProxyConfig_->MaxQueryFileContentSizeBytes,
                file->Content.size());
        }
    }

    static const TYsonString EmptyMap = TYsonString(TString("{}"));

    YT_LOG_DEBUG("Starting query (QueryId: %v, Draft: %v)", queryId, options.Draft);

    auto rowBuffer = New<TRowBuffer>();
    auto transaction = WaitFor(StateClient_->StartTransaction(ETransactionType::Tablet, {}))
        .ValueOrThrow();

    auto accessControlObjects = ValidateAccessControlObjects(options.AccessControlObject, options.AccessControlObjects).value_or(std::vector<TString>{});
    VerifyAllAccessControlObjectsExist(accessControlObjects, StateClient_);

    // Draft queries go directly to finished query tables (regular and ordered by start time),
    // non-draft queries go to the active query table.

    if (options.Draft) {
        TString filterFactors;
        auto startTime = TInstant::Now();
        {
            static_assert(TFinishedQueryDescriptor::FieldCount == 14);
            TFinishedQuery newRecord{
                .Key = {.QueryId = queryId},
                .Engine = engine,
                .Query = query,
                .Files = ConvertToYsonString(options.Files),
                .Settings = options.Settings ? ConvertToYsonString(options.Settings) : EmptyMap,
                // TODO(babenko): switch to std::string
                .User = TString(user),
                .AccessControlObjects = ConvertToYsonString(accessControlObjects),
                .StartTime = startTime,
                .State = EQueryState::Draft,
                .Progress = EmptyMap,
                .Annotations = options.Annotations ? ConvertToYsonString(options.Annotations) : EmptyMap,
            };
            filterFactors = GetFilterFactors(newRecord);
            std::vector rows{
                newRecord.ToUnversionedRow(rowBuffer, TFinishedQueryDescriptor::Get()->GetIdMapping()),
            };
            transaction->WriteRows(
                StateRoot_ + "/finished_queries",
                TFinishedQueryDescriptor::Get()->GetNameTable(),
                MakeSharedRange(std::move(rows), rowBuffer));
        }
        {
            static_assert(TFinishedQueryByStartTimeDescriptor::FieldCount == 7);

            TFinishedQueryByStartTime newRecord{
                .Key = {.MinusStartTime = -i64(startTime.MicroSeconds()), .QueryId = queryId},
                .Engine = engine,
                // TODO(babenko): switch to std::string
                .User = TString(user),
                .AccessControlObjects = ConvertToYsonString(accessControlObjects),
                .State = EQueryState::Draft,
                .FilterFactors = filterFactors,
            };
            std::vector rows{
                newRecord.ToUnversionedRow(rowBuffer, TFinishedQueryByStartTimeDescriptor::Get()->GetIdMapping()),
            };
            transaction->WriteRows(
                StateRoot_ + "/finished_queries_by_start_time",
                TFinishedQueryByStartTimeDescriptor::Get()->GetNameTable(),
                MakeSharedRange(std::move(rows), rowBuffer));
        }
        {
            static_assert(TFinishedQueryByUserAndStartTimeDescriptor::FieldCount == 6);

            TFinishedQueryByUserAndStartTime newRecord{
                // TODO(babenko): switch to std::string
                .Key = {.User = TString(user), .MinusStartTime = -i64(startTime.MicroSeconds()), .QueryId = queryId},
                .Engine = engine,
                .State = EQueryState::Draft,
                .FilterFactors = filterFactors,
            };
            std::vector rows{
                newRecord.ToUnversionedRow(rowBuffer, TFinishedQueryByUserAndStartTimeDescriptor::Get()->GetIdMapping()),
            };
            transaction->WriteRows(
                StateRoot_ + "/" + FinishedQueriesByUserAndStartTimeTable,
                TFinishedQueryByUserAndStartTimeDescriptor::Get()->GetNameTable(),
                MakeSharedRange(std::move(rows), rowBuffer));
        }
        {
            static_assert(TFinishedQueryByAcoAndStartTimeDescriptor::FieldCount == 7);

            if (!accessControlObjects.empty()) {
                std::vector<TUnversionedRow> rows;
                rows.reserve(accessControlObjects.size());
                for (const auto& aco : accessControlObjects) {
                    TFinishedQueryByAcoAndStartTime newRecord{
                        .Key = {.AccessControlObject = aco, .MinusStartTime = -i64(startTime.MicroSeconds()), .QueryId = queryId},
                        .Engine = engine,
                        // TODO(babenko): switch to std::string
                        .User = TString(user),
                        .State = EQueryState::Draft,
                        .FilterFactors = filterFactors,
                    };
                    rows.push_back(newRecord.ToUnversionedRow(rowBuffer, TFinishedQueryByAcoAndStartTimeDescriptor::Get()->GetIdMapping()));
                }
                transaction->WriteRows(
                    StateRoot_ + "/" + FinishedQueriesByAcoAndStartTimeTable,
                    TFinishedQueryByAcoAndStartTimeDescriptor::Get()->GetNameTable(),
                    MakeSharedRange(std::move(rows), rowBuffer));
            }
        }
    } else {
        static_assert(TActiveQueryDescriptor::FieldCount == 20);
        TActiveQueryPartial newRecord{
            .Key = {.QueryId = queryId},
            .Engine = engine,
            .Query = query,
            .Files = ConvertToYsonString(options.Files),
            .Settings = options.Settings ? ConvertToYsonString(options.Settings) : EmptyMap,
            // TODO(babenko): switch to std::string
            .User = TString(user),
            .AccessControlObjects = ConvertToYsonString(accessControlObjects),
            .StartTime = TInstant::Now(),
            .State = EQueryState::Pending,
            .Incarnation = -1,
            .Progress = EmptyMap,
            .Annotations = options.Annotations ? ConvertToYsonString(options.Annotations) : EmptyMap,
        };
        newRecord.FilterFactors = GetFilterFactors(newRecord);
        std::vector rows{
            newRecord.ToUnversionedRow(rowBuffer, TActiveQueryDescriptor::Get()->GetIdMapping()),
        };
        transaction->WriteRows(
            StateRoot_ + "/active_queries",
            TActiveQueryDescriptor::Get()->GetNameTable(),
            MakeSharedRange(std::move(rows), rowBuffer));
    }
    WaitFor(transaction->Commit())
        .ThrowOnError();
}

void TQueryTrackerProxy::AbortQuery(
    const TQueryId queryId,
    const TAbortQueryOptions& options,
    const std::string& user)
{
    auto abortError = TError(
        "Query was aborted by user %Qv%v",
        user,
        options.AbortMessage ? Format(" with message %Qv", *options.AbortMessage) : TString());

    TActiveQueryPartial newRecord{
        .Key = {.QueryId = queryId},
        .State = EQueryState::Aborting,
        .FinishTime = TInstant::Now(),
        .AbortRequest = ConvertToYsonString(abortError),
    };
    auto rowBuffer = New<TRowBuffer>();
    std::vector rows{
        newRecord.ToUnversionedRow(rowBuffer, TActiveQueryDescriptor::Get()->GetIdMapping()),
    };
    auto transaction = WaitFor(StateClient_->StartTransaction(ETransactionType::Tablet, {}))
        .ValueOrThrow();

    ValidateQueryPermissions(queryId, StateRoot_, transaction->GetStartTimestamp(), user, StateClient_, EPermission::Administer, Logger);

    YT_LOG_DEBUG("Aborting query (QueryId: %v, AbortMessage: %v)", queryId, options.AbortMessage);

    TActiveQuery record;
    {
        const auto& idMapping = TActiveQueryDescriptor::Get()->GetIdMapping();
        TLookupRowsOptions options;
        options.Timestamp = transaction->GetStartTimestamp();
        options.ColumnFilter = {*idMapping.State, *idMapping.Engine, *idMapping.AssignedTracker, *idMapping.ExecutionStartTime};
        options.KeepMissingRows = true;
        TActiveQueryKey key{.QueryId = queryId};
        std::vector keys{
            key.ToKey(rowBuffer),
        };
        auto asyncLookupResult = StateClient_->LookupRows(
            StateRoot_ + "/active_queries",
            TActiveQueryDescriptor::Get()->GetNameTable(),
            MakeSharedRange(std::move(keys), rowBuffer),
            options);
        auto rowset = WaitFor(asyncLookupResult)
            .ValueOrThrow()
            .Rowset;
        auto optionalRecords = ToOptionalRecords<TActiveQuery>(rowset);
        YT_VERIFY(optionalRecords.size() == 1);
        if (!optionalRecords[0]) {
            THROW_ERROR_EXCEPTION(
                NQueryTrackerClient::EErrorCode::QueryNotFound,
                "Query %v not found or is not running",
                queryId);
        }
        record = *optionalRecords[0];
        if (record.State != EQueryState::Pending && record.State != EQueryState::Running) {
            THROW_ERROR_EXCEPTION("Cannot abort query %v which is in state %Qlv",
                queryId,
                record.State);
        }
    }

    transaction->WriteRows(
        StateRoot_ + "/active_queries",
        TActiveQueryDescriptor::Get()->GetNameTable(),
        MakeSharedRange(std::move(rows), std::move(rowBuffer)));

    auto error = WaitFor(transaction->Commit());
    if (!error.IsOK()) {
        if (error.FindMatching(NTabletClient::EErrorCode::TransactionLockConflict)) {
            // TODO(max42): retry such errors automatically?
            THROW_ERROR_EXCEPTION("Cannot abort query because its state is being changed at the moment; please try again")
                << error;
        }
        THROW_ERROR error;
    }

    {
        // Save profile counter.
        auto now = TInstant::Now();
        auto time = now - record.StartTime;
        if (record.State == EQueryState::Running) {
            time = now - *record.ExecutionStartTime;
        }

        auto& stateTimeGauge = GetOrCreateProfilingCounter<TStateTimeProfilingCounter>(
            QueryTrackerProfiler,
            ProfilingTagsFromActiveQueryRecord(record))->StateTime;
        stateTimeGauge.Update(time);
    }
}

TQueryResult TQueryTrackerProxy::GetQueryResult(
    const TQueryId queryId,
    const i64 resultIndex,
    const std::string& user)
{
    auto timestamp = WaitFor(StateClient_->GetTimestampProvider()->GenerateTimestamps())
        .ValueOrThrow();

    YT_LOG_DEBUG("Getting query result (QueryId: %v, ResultIndex: %v)", queryId, resultIndex);

    TQueryResult queryResult;
    {
        auto rowBuffer = New<TRowBuffer>();
        TLookupRowsOptions options;
        options.KeepMissingRows = true;

        ValidateQueryPermissions(queryId, StateRoot_, timestamp, user, StateClient_, EPermission::Read, Logger);

        TFinishedQueryResultKey key{.QueryId = queryId, .Index = resultIndex};
        std::vector keys{
            key.ToKey(rowBuffer),
        };
        auto asyncLookupResult = StateClient_->LookupRows(
            StateRoot_ + "/finished_query_results",
            TFinishedQueryResultDescriptor::Get()->GetNameTable(),
            MakeSharedRange(std::move(keys), rowBuffer),
            options);
        auto rowset = WaitFor(asyncLookupResult)
            .ValueOrThrow()
            .Rowset;
        auto optionalRecords = ToOptionalRecords<TFinishedQueryResult>(rowset);
        YT_VERIFY(optionalRecords.size() == 1);
        if (!optionalRecords[0]) {
            THROW_ERROR_EXCEPTION(
                NQueryTrackerClient::EErrorCode::QueryResultNotFound,
                "Query %v result %v not found or is expired",
                queryId,
                resultIndex);
        }
        const auto& record = *optionalRecords[0];

        queryResult.Id = queryId;
        queryResult.ResultIndex = resultIndex;
        auto schemaNode = record.Schema ? ConvertToNode(*record.Schema) : nullptr;
        queryResult.Schema = schemaNode && schemaNode->GetType() == ENodeType::List ? ConvertTo<TTableSchemaPtr>(schemaNode) : nullptr;
        queryResult.Error = record.Error;
        queryResult.IsTruncated = record.IsTruncated;
        queryResult.DataStatistics = ConvertTo<TDataStatistics>(record.DataStatistics);
    }

    return queryResult;
}

IUnversionedRowsetPtr TQueryTrackerProxy::ReadQueryResult(
    const TQueryId queryId,
    const i64 resultIndex,
    const TReadQueryResultOptions& options,
    const std::string& user)
{
    YT_LOG_DEBUG(
        "Reading query result (QueryId: %v, ResultIndex: %v, LowerRowIndex: %v, UpperRowIndex: %v)",
        queryId,
        resultIndex,
        options.LowerRowIndex,
        options.UpperRowIndex);

    auto timestamp = WaitFor(StateClient_->GetTimestampProvider()->GenerateTimestamps())
        .ValueOrThrow();

    TString wireRowset;
    TTableSchemaPtr schema;
    {
        auto rowBuffer = New<TRowBuffer>();
        TLookupRowsOptions options;
        options.KeepMissingRows = true;

        ValidateQueryPermissions(queryId, StateRoot_, timestamp, user, StateClient_, EPermission::Read, Logger);

        TFinishedQueryResultKey key{.QueryId = queryId, .Index = resultIndex};
        std::vector keys{
            key.ToKey(rowBuffer),
        };
        auto asyncLookupResult = StateClient_->LookupRows(
            StateRoot_ + "/finished_query_results",
            TFinishedQueryResultDescriptor::Get()->GetNameTable(),
            MakeSharedRange(std::move(keys), rowBuffer),
            options);
        auto rowset = WaitFor(asyncLookupResult)
            .ValueOrThrow()
            .Rowset;
        auto optionalRecords = ToOptionalRecords<TFinishedQueryResult>(rowset);
        YT_VERIFY(optionalRecords.size() == 1);
        if (!optionalRecords[0]) {
            THROW_ERROR_EXCEPTION(
                NQueryTrackerClient::EErrorCode::QueryResultNotFound,
                "Query %v result %v not found or is expired",
                queryId,
                resultIndex);
        }
        const auto& record = *optionalRecords[0];
        if (!record.Error.IsOK()) {
            THROW_ERROR record.Error;
        }
        if (!record.Rowset) {
            // This should not normally happen, but better this than abort.
            THROW_ERROR_EXCEPTION(
                NQueryTrackerClient::EErrorCode::QueryResultNotFound,
                "Query %v result %v rowset is missing",
                queryId,
                resultIndex);
        }
        wireRowset = *record.Rowset;
        schema = ConvertTo<TTableSchemaPtr>(record.Schema);
    }

    auto wireReader = CreateWireProtocolReader(TSharedRef::FromString(std::move(wireRowset)));
    auto schemaData = IWireProtocolReader::GetSchemaData(*schema);
    auto rows = wireReader->ReadSchemafulRowset(schemaData, /*captureValues*/ true);

    {
        auto lowerRowIndex = options.LowerRowIndex.value_or(0);
        lowerRowIndex = std::clamp<i64>(lowerRowIndex, 0, rows.size());
        auto upperRowIndex = options.UpperRowIndex.value_or(rows.size());
        upperRowIndex = std::clamp<i64>(upperRowIndex, lowerRowIndex, rows.size());
        rows = rows.Slice(lowerRowIndex, upperRowIndex);
    }

    auto rowset = CreateRowset(std::move(schema), std::move(rows));
    if (options.Columns) {
        rowset = FilterRowsetColumns(rowset, *options.Columns);
    }
    return rowset;
}

TQuery TQueryTrackerProxy::GetQuery(
    const TQueryId queryId,
    const TGetQueryOptions& options,
    const std::string& user)
{
    auto timestamp = options.Timestamp != NullTimestamp
        ? options.Timestamp
        : WaitFor(StateClient_->GetTimestampProvider()->GenerateTimestamps())
            .ValueOrThrow();

    YT_LOG_DEBUG("Getting query (QueryId: %v, Timestamp: %v, Attributes: %v)", queryId, timestamp, options.Attributes);

    options.Attributes.ValidateKeysOnly();

    ValidateQueryPermissions(queryId, StateRoot_, timestamp, user, StateClient_, EPermission::Use, Logger);

    auto lookupKeys = options.Attributes ? std::make_optional(options.Attributes.Keys) : std::nullopt;

    auto query = LookupQuery(queryId, StateClient_, StateRoot_, lookupKeys, timestamp, Logger);

    if (!lookupKeys || std::find(lookupKeys->begin(), lookupKeys->end(), "access_control_object") != lookupKeys->end()) {
        ConvertAcoToOldFormat(query);
    }

    return query;
}

TListQueriesResult TQueryTrackerProxy::ListQueries(
    const TListQueriesOptions& options,
    const std::string& user)
{
    auto timestamp = WaitFor(StateClient_->GetTimestampProvider()->GenerateTimestamps())
        .ValueOrThrow();

    YT_LOG_DEBUG(
        "Listing queries (Timestamp: %v, State: %v, CursorDirection: %v, FromTime: %v, ToTime: %v, CursorTime: %v,"
        "Substr: %v, User: %v, Engine: %v, Limit: %v, Attributes: %v)",
        timestamp,
        options.StateFilter,
        options.CursorDirection,
        options.FromTime,
        options.ToTime,
        options.CursorTime,
        options.SubstrFilter,
        options.UserFilter,
        options.EngineFilter,
        options.Limit,
        options.Attributes);

    auto attributes = options.Attributes;

    attributes.ValidateKeysOnly();

    if (!attributes.AdmitsKeySlow("start_time")) {
        YT_VERIFY(attributes);
        attributes.Keys.push_back("start_time");
    }

    auto userSubjects = GetUserSubjects(user, StateClient_);
    // TODO(babenko): switch to std::string
    userSubjects.insert(TString(user));

    std::vector<TString> userSubjectsVector(userSubjects.begin(), userSubjects.end());
    YT_LOG_DEBUG(
        "Fetched user subjects (User: %v, Subjects: %v)",
        user,
        userSubjectsVector);

    bool isSuperuser = userSubjects.contains(SuperusersGroupName);
    std::vector<TString> acosForUser;

    if (!isSuperuser) {
        acosForUser = GetAcosForSubjects(userSubjects, /*filterEveryoneShareAco*/ true, StateClient_);
        YT_LOG_DEBUG(
            "Fetched suitable access control objects for user (User: %v, Acos: %v)",
            user,
            acosForUser);
    }

    auto addSelectExpressionsFromAttributes = [&] (NQueryClient::TQueryBuilder& builder, const TNameTablePtr& nameTable) {
        if (attributes) {
            for (const auto& key : attributes.Keys) {
                if (nameTable->FindId(key)) {
                    builder.AddSelectExpression("[" + key + "]");
                }
            }
        } else {
            builder.AddSelectExpression("*");
        }
    };

    auto addSelectExpressionsForMerging = [&] (NQueryClient::TQueryBuilder& builder) {
        if (!attributes.AdmitsKeySlow("query_id")) {
            builder.AddSelectExpression("[query_id]");
        }
    };

    auto selectActiveQueries = [=, this, this_ = MakeStrong(this)] {
        try {
            NQueryClient::TQueryBuilder builder;
            TYsonString placeholderValues;
            builder.SetSource(StateRoot_ + "/active_queries");
            AddFilterConditions(builder, placeholderValues, options, user, acosForUser, TString("active_queries"), isSuperuser);
            addSelectExpressionsFromAttributes(builder, TActiveQueryDescriptor::Get()->GetNameTable());
            addSelectExpressionsForMerging(builder);
            TSelectRowsOptions options;
            options.Timestamp = timestamp;
            options.PlaceholderValues = placeholderValues;
            auto query = builder.Build();
            YT_LOG_DEBUG("Selecting active queries (Query: %v)", query);
            auto selectResult = WaitFor(StateClient_->SelectRows(query, options))
                .ValueOrThrow();
            auto records = ToRecords<TActiveQueryPartial>(selectResult.Rowset);
            return PartialRecordsToQueries(records);
        } catch (const std::exception& ex) {
            THROW_ERROR_EXCEPTION("Error while selecting active queries")
                << ex;
        }
    };

    bool incomplete = false;
    auto selectFinishedQueries = [=, this, this_ = MakeStrong(this), &incomplete] () -> std::vector<TQuery> {
        try {
            TStringBuilder admittedQueryIds;

            {
                std::vector<std::pair<TTimestamp, TQueryId>> results;
                if (isSuperuser && !options.UserFilter) {
                    GetQueriesByStartTime<TFinishedQueryByStartTime>(StateClient_, StateRoot_, options, timestamp, user, acosForUser, "finished_queries_by_start_time", results, isSuperuser);
                } else {
                    GetQueriesByStartTime<TFinishedQueryByUserAndStartTime>(StateClient_, StateRoot_, options, timestamp, user, acosForUser, FinishedQueriesByUserAndStartTimeTable, results, isSuperuser);
                    if (!acosForUser.empty()) {
                        GetQueriesByStartTime<TFinishedQueryByAcoAndStartTime>(StateClient_, StateRoot_, options, timestamp, user, acosForUser, FinishedQueriesByAcoAndStartTimeTable, results, isSuperuser);
                    }
                    if (options.CursorDirection != EOperationSortDirection::None) {
                        auto compare = [&] (const std::pair<TTimestamp, TQueryId>& lhs, const std::pair<TTimestamp, TQueryId>& rhs) {
                            return options.CursorDirection == EOperationSortDirection::Past ? lhs > rhs : lhs < rhs;
                        };
                        std::sort(results.begin(), results.end(), compare);
                    }
                }

                bool isFirst = true;

                // filter duplicates
                THashSet<TQueryId> usedQueries;
                for (const auto& result : results) {
                    if (usedQueries.contains(result.second)) {
                        continue;
                    }
                    usedQueries.insert(result.second);
                    if (usedQueries.size() > options.Limit) {
                        incomplete = true;
                        break;
                    }

                    if (!isFirst) {
                        admittedQueryIds.AppendString(", ");
                    }
                    admittedQueryIds.AppendFormat("\"%v\"", result.second);
                    isFirst = false;
                }

                if (isFirst) {
                    return {};
                }
            }

            {
                NQueryClient::TQueryBuilder builder;
                builder.SetSource(StateRoot_ + "/finished_queries");
                addSelectExpressionsFromAttributes(builder, TFinishedQueryDescriptor::Get()->GetNameTable());
                builder.AddWhereConjunct("[query_id] in (" + admittedQueryIds.Flush() + ")");
                addSelectExpressionsForMerging(builder);
                TSelectRowsOptions options;
                options.Timestamp = timestamp;
                auto query = builder.Build();
                YT_LOG_DEBUG("Selecting admitted finished queries (Query: %v)", query);
                auto selectResult = WaitFor(StateClient_->SelectRows(query, options))
                    .ValueOrThrow();
                auto records = ToRecords<TFinishedQueryPartial>(selectResult.Rowset);
                return PartialRecordsToQueries(records);
            }
        } catch (const std::exception& ex) {
            THROW_ERROR_EXCEPTION("Error while selecting finished queries")
                << ex;
        }
    };

    std::vector<TQuery> queries;
    {
        std::vector<TFuture<std::vector<TQuery>>> futures;
        std::optional<bool> stateFilterDefinesFinishedQuery = options.StateFilter
            ? std::make_optional(IsFinishedState(*options.StateFilter))
            : std::nullopt;
        if (!options.StateFilter || *stateFilterDefinesFinishedQuery) {
            futures.push_back(BIND(selectFinishedQueries).AsyncVia(GetCurrentInvoker()).Run());
        }
        if (!options.StateFilter || !*stateFilterDefinesFinishedQuery) {
            futures.push_back(BIND(selectActiveQueries).AsyncVia(GetCurrentInvoker()).Run());
        }
        auto queryVectors = WaitFor(AllSucceeded(futures))
            .ValueOrThrow();
        for (const auto& queryVector : queryVectors) {
            queries.insert(queries.end(), queryVector.begin(), queryVector.end());
        }
    }

    std::vector<TQuery> result;
    if (options.CursorDirection != EOperationSortDirection::None) {
        auto compare = [&] (const TQuery& lhs, const TQuery& rhs) {
            return options.CursorDirection == EOperationSortDirection::Past
                ? std::tie(lhs.StartTime, lhs.Query) > std::tie(rhs.StartTime, rhs.Query)
                : std::tie(lhs.StartTime, lhs.Query) < std::tie(rhs.StartTime, rhs.Query);
        };
        std::sort(queries.begin(), queries.end(), compare);
        for (auto& query : queries) {
            if (!options.CursorTime ||
                (options.CursorDirection == EOperationSortDirection::Past && query.StartTime < options.CursorTime) ||
                (options.CursorDirection == EOperationSortDirection::Future && query.StartTime > options.CursorTime))
            {
                if (result.size() == options.Limit) {
                    // We are finishing collecting queries prematurely, set incomplete flag and break.
                    incomplete = true;
                    break;
                }
                result.push_back(std::move(query));
            }
        }
    } else {
        if (queries.size() > options.Limit) {
            incomplete = true;
        }
        result = std::vector(queries.begin(), queries.begin() + std::min(options.Limit, queries.size()));
    }

    if (!attributes || std::find(attributes.Keys.begin(), attributes.Keys.end(), "access_control_object") != attributes.Keys.end()) {
        for (auto& query: result) {
            ConvertAcoToOldFormat(query);
        }
    }

    return TListQueriesResult{
        .Queries = std::move(result),
        .Incomplete = incomplete,
        .Timestamp = timestamp,
    };
}

void TQueryTrackerProxy::AlterQuery(
    const TQueryId queryId,
    const TAlterQueryOptions& options,
    const std::string& user)
{
    auto transaction = WaitFor(StateClient_->StartTransaction(ETransactionType::Tablet, {}))
        .ValueOrThrow();

    auto timestamp = transaction->GetStartTimestamp();

    ValidateQueryPermissions(queryId, StateRoot_, timestamp, user, StateClient_, EPermission::Administer, Logger);

    std::vector<TString> lookupKeys = {"state", "start_time", "user"};

    auto query = LookupQuery(queryId, StateClient_, StateRoot_, lookupKeys, timestamp, Logger);

    auto accessControlObjects = ValidateAccessControlObjects(options.AccessControlObject, options.AccessControlObjects);

    YT_LOG_DEBUG(
        "Altering query (QueryId: %v, State: %v, HasAnnotations: %v, HasAccessControlObjects: %v)",
        queryId,
        *query.State,
        static_cast<bool>(options.Annotations),
        static_cast<bool>(accessControlObjects));

    if (!options.Annotations && !accessControlObjects) {
        WaitFor(transaction->Commit())
            .ThrowOnError();
        return;
    }

    if (accessControlObjects) {
        VerifyAllAccessControlObjectsExist(*accessControlObjects, StateClient_);
    }

    if (IsFinishedState(*query.State)) {
        auto rowBuffer = New<TRowBuffer>();
        TString filterFactors;
        {
            TFinishedQueryPartial record{
                .Key = {.QueryId = queryId},
                .AccessControlObjects = query.AccessControlObjects ? query.AccessControlObjects : TYsonString(TString("[]")),
                .Annotations = query.Annotations ? query.Annotations : TYsonString(TString("{}")),
            };
            if (options.Annotations) {
                record.Annotations = ConvertToYsonString(options.Annotations);
                filterFactors = GetFilterFactors(record);
            }
            if (accessControlObjects) {
                record.AccessControlObjects = ConvertToYsonString(accessControlObjects);
            }

            filterFactors = GetFilterFactors(record);

            std::vector rows{
                record.ToUnversionedRow(rowBuffer, TFinishedQueryDescriptor::Get()->GetIdMapping()),
            };
            transaction->WriteRows(
                StateRoot_ + "/finished_queries",
                TFinishedQueryDescriptor::Get()->GetNameTable(),
                MakeSharedRange(std::move(rows), rowBuffer));
        }
        {
            TFinishedQueryByStartTimePartial record{
                .Key = {.MinusStartTime = -i64(query.StartTime->MicroSeconds()), .QueryId = queryId},
                .FilterFactors = filterFactors,
            };
            if (accessControlObjects) {
                record.AccessControlObjects = ConvertToYsonString(accessControlObjects);
            }

            std::vector rows{
                record.ToUnversionedRow(rowBuffer, TFinishedQueryByStartTimeDescriptor::Get()->GetIdMapping()),
            };
            transaction->WriteRows(
                StateRoot_ + "/finished_queries_by_start_time",
                TFinishedQueryByStartTimeDescriptor::Get()->GetNameTable(),
                MakeSharedRange(std::move(rows), rowBuffer));
        }
        {
            if (options.Annotations) {
                if (!query.User) {
                    THROW_ERROR_EXCEPTION("User is lost in query %v", queryId);
                } else if (!query.StartTime) {
                    THROW_ERROR_EXCEPTION("StartTime is lost in query %v", queryId);
                }

                TFinishedQueryByUserAndStartTimePartial record{
                    .Key = {.User = *query.User, .MinusStartTime = -i64(query.StartTime->MicroSeconds()), .QueryId = queryId},
                    .FilterFactors = filterFactors,
                };

                std::vector rows{
                    record.ToUnversionedRow(rowBuffer, TFinishedQueryByUserAndStartTimeDescriptor::Get()->GetIdMapping()),
                };
                transaction->WriteRows(
                    StateRoot_ + "/" + FinishedQueriesByUserAndStartTimeTable,
                    TFinishedQueryByUserAndStartTimeDescriptor::Get()->GetNameTable(),
                    MakeSharedRange(std::move(rows), rowBuffer));
            }
        }
        {
            if (!query.User) {
                THROW_ERROR_EXCEPTION("User is lost in query %v", queryId);
            } else if (!query.StartTime) {
                THROW_ERROR_EXCEPTION("StartTime is lost in query %v", queryId);
            }

            auto previousAccessControlObjects = query.AccessControlObjects && *query.AccessControlObjects ? ConvertTo<std::vector<TString>>(*query.AccessControlObjects) : std::vector<TString>{};
            auto diff = GetAccessControlDiff(previousAccessControlObjects, accessControlObjects);

            auto acoToDelete = diff.first;
            if (!acoToDelete.empty()) {
                std::vector<TUnversionedRow> keysToDelete;
                keysToDelete.reserve(acoToDelete.size());
                for (const auto& aco : acoToDelete) {
                    keysToDelete.push_back(TFinishedQueryByAcoAndStartTimeKey{.AccessControlObject = aco, .MinusStartTime = -i64(query.StartTime->MicroSeconds()), .QueryId = queryId}.ToKey(rowBuffer));
                }
                transaction->DeleteRows(
                    StateRoot_ + "/" + FinishedQueriesByAcoAndStartTimeTable,
                    TFinishedQueryByAcoAndStartTimeDescriptor::Get()->GetNameTable(),
                    MakeSharedRange(std::move(keysToDelete), rowBuffer));
            }

            auto acoToInsert = diff.second;
            if (options.Annotations) {
                auto newAccessControlObjects = accessControlObjects.value_or(previousAccessControlObjects);
                acoToInsert = newAccessControlObjects;
            }
            if (!acoToInsert.empty()) {
                std::vector<TUnversionedRow> rows;
                rows.reserve(acoToInsert.size());
                for (const auto& aco : acoToInsert) {
                    TFinishedQueryByAcoAndStartTimePartial record{
                        .Key = {.AccessControlObject = aco, .MinusStartTime = -i64(query.StartTime->MicroSeconds()), .QueryId = queryId},
                        .Engine = query.Engine,
                        .User = query.User,
                        .State = query.State,
                        .FilterFactors = filterFactors,
                    };
                    rows.push_back(record.ToUnversionedRow(rowBuffer, TFinishedQueryByAcoAndStartTimeDescriptor::Get()->GetIdMapping()));
                }
                transaction->WriteRows(
                    StateRoot_ + "/" + FinishedQueriesByAcoAndStartTimeTable,
                    TFinishedQueryByAcoAndStartTimeDescriptor::Get()->GetNameTable(),
                    MakeSharedRange(std::move(rows), rowBuffer));
            }
        }
    } else {
        auto rowBuffer = New<TRowBuffer>();
        {
            TActiveQueryPartial record{
                .Key = {.QueryId = queryId},
                .AccessControlObjects = query.AccessControlObjects ? query.AccessControlObjects : TYsonString(TString("[]")),
                .Annotations = query.Annotations ? query.Annotations : TYsonString(TString("{}")),
            };
            if (options.Annotations) {
                record.Annotations = ConvertToYsonString(options.Annotations);
            }
            if (accessControlObjects) {
                record.AccessControlObjects = ConvertToYsonString(accessControlObjects);
            }
            record.FilterFactors = GetFilterFactors(record);

            std::vector rows{
                record.ToUnversionedRow(rowBuffer, TActiveQueryDescriptor::Get()->GetIdMapping()),
            };
            transaction->WriteRows(
                StateRoot_ + "/active_queries",
                TActiveQueryDescriptor::Get()->GetNameTable(),
                MakeSharedRange(std::move(rows), rowBuffer));
        }
    }

    WaitFor(transaction->Commit())
        .ThrowOnError();
}

TGetQueryTrackerInfoResult TQueryTrackerProxy::GetQueryTrackerInfo(
    const TGetQueryTrackerInfoOptions& options)
{
    YT_LOG_DEBUG(
        "Getting query tracker information (Attributes: %v)",
        options.Attributes);

    auto attributes = options.Attributes;

    attributes.ValidateKeysOnly();

    TString clusterName = "";

    if (attributes.AdmitsKeySlow("cluster_name")) {
        YT_LOG_DEBUG("Getting cluster name");
        clusterName = StateClient_->GetClusterName().value_or("");
    }

    TNodeExistsOptions nodeExistsOptions;
    nodeExistsOptions.ReadFrom = EMasterChannelKind::Cache;
    nodeExistsOptions.SuccessStalenessBound = TDuration::Minutes(1);

    static const TYsonString EmptyMap = TYsonString(TString("{}"));
    TYsonString supportedFeatures = EmptyMap;
    if (attributes.AdmitsKeySlow("supported_features")) {
        // These features are guaranteed to be deployed before or with this code.
        supportedFeatures = BuildYsonStringFluently()
            .BeginMap()
                .Item("access_control").Value(true)
                .Item("multiple_aco").Value(true)
            .EndMap();
    }

    std::vector<TString> accessControlObjects;
    if (attributes.AdmitsKeySlow("access_control_objects")) {
        YT_LOG_DEBUG("Getting access control objects");
        TListNodeOptions listOptions;
        listOptions.ReadFrom = EMasterChannelKind::Cache;
        listOptions.SuccessStalenessBound = TDuration::Minutes(1);
        auto allAcos = WaitFor(StateClient_->ListNode(TString(QueriesAcoNamespacePath), listOptions))
            .ValueOrThrow();
        accessControlObjects = ConvertTo<std::vector<TString>>(allAcos);
    }

    return TGetQueryTrackerInfoResult{
        .QueryTrackerStage = options.QueryTrackerStage,
        .ClusterName = std::move(clusterName),
        .SupportedFeatures = std::move(supportedFeatures),
        .AccessControlObjects = std::move(accessControlObjects),
    };
}

////////////////////////////////////////////////////////////////////////////////

TQueryTrackerProxyPtr CreateQueryTrackerProxy(
    IClientPtr stateClient,
    TYPath stateRoot,
    TQueryTrackerProxyConfigPtr config)
{
    return New<TQueryTrackerProxy>(
        std::move(stateClient),
        std::move(stateRoot),
        std::move(config));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryTracker
