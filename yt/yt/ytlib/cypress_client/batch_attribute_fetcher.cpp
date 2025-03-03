#include "batch_attribute_fetcher.h"

#include <yt/yt/ytlib/api/native/client.h>
#include <yt/yt/ytlib/api/native/connection.h>
#include <yt/yt/ytlib/api/native/rpc_helpers.h>

#include <yt/yt/ytlib/object_client/object_service_proxy.h>

#include <yt/yt/core/ypath/helpers.h>

#include <yt/yt/core/ytree/ypath_proxy.h>

#include <library/cpp/iterator/functools.h>

namespace NYT::NCypressClient {

using namespace NApi;
using namespace NApi::NNative;
using namespace NObjectClient;
using namespace NYPath;
using namespace NYson;
using namespace NYTree;
using namespace NConcurrency;
using namespace NLogging;

////////////////////////////////////////////////////////////////////////////////

// TODO(max42): make configurable.
static constexpr int MaxUnusedNodeCount = 5000;

// TODO(achulkov2): It would be nifty to extract these names from interned attribute names into some non-server space.
// Builtin attributes are used as extensively all over the client/ytlib space.
static constexpr auto TypeAttributeName = "type";

////////////////////////////////////////////////////////////////////////////////

TBatchAttributeFetcher::TBatchAttributeFetcher(
    const std::vector<TYPath>& paths,
    const std::vector<std::string>& attributeNames,
    const NApi::NNative::IClientPtr& client,
    const IInvokerPtr& invoker,
    const TLogger& logger,
    const NApi::TMasterReadOptions& options)
    : AttributeNames_(attributeNames)
    , Client_(client)
    , Invoker_(invoker)
    , MasterReadOptions_(options)
    , Logger(logger)
{
    // We need the "type" attribute to distinguish links from regular nodes. If it is not requested, we fetch it, but
    // later remove it from the externally visible result.
    if (std::find(AttributeNames_.begin(), AttributeNames_.end(), TypeAttributeName) == AttributeNames_.end()) {
        AddedTypeAttribute_ = true;
        AttributeNames_.push_back(TypeAttributeName);
    }

    int invalidPathCount = 0;

    for (const auto& [index, path] : Enumerate(paths)) {
        auto& entry = Entries_.emplace_back();
        entry.Index = index;
        if (IsPathPointingToAttributes(path)) {
            entry.Error = TError("Requested path should not point to attributes (i.e. contain @)");
            ++invalidPathCount;
        }
        std::tie(entry.DirName, entry.BaseName) = DirNameAndBaseName(path);
    }

    YT_LOG_DEBUG(
        "Batch attribute fetcher initialized (PathCount: %v, InvalidPathCount: %v)",
        paths.size(),
        invalidPathCount);

    std::sort(Entries_.begin(), Entries_.end(), [] (auto lhs, auto rhs) {
        return std::tie(lhs.DirName, lhs.BaseName) < std::tie(rhs.DirName, rhs.BaseName);
    });

    Attributes_.resize(Entries_.size());
    DeduplicationReferenceTableIndices_.resize(Entries_.size(), -1);

    // Deduplicate entries using manual unique-like procedure. For each dropped entry keep index
    // of a reference table to copy attributes from when we finish fetching.
    // [beginIndex, endIndex) is a half-interval of tables with same <DirName, BaseName> tuple
    // uniqueIndex points to the position of first unfilled element (similar to standard unique procedure).
    size_t uniqueIndex = 0;
    for (size_t beginIndex = 0, endIndex = 0; beginIndex != Entries_.size(); beginIndex = endIndex) {
        endIndex = beginIndex + 1;
        while (
            endIndex != Entries_.size() &&
            std::tie(Entries_[beginIndex].DirName, Entries_[beginIndex].BaseName) ==
            std::tie(Entries_[endIndex].DirName, Entries_[endIndex].BaseName))
        {
            DeduplicationReferenceTableIndices_[Entries_[endIndex].Index] = Entries_[beginIndex].Index;
            ++endIndex;
        }
        Entries_[uniqueIndex++] = std::move(Entries_[beginIndex]);
    }
    Entries_.resize(uniqueIndex);

    for (size_t beginIndex = 0, endIndex = 0; beginIndex != Entries_.size(); beginIndex = endIndex) {
        // Extract contiguous run of entries in same directory.
        endIndex = beginIndex + 1;
        while (endIndex != Entries_.size() &&
            Entries_[endIndex].DirName == Entries_[beginIndex].DirName &&
            Entries_[endIndex].Error.IsOK())
        {
            ++endIndex;
        }

        if (!Entries_[beginIndex].Error.IsOK()) {
            continue;
        }

        if (endIndex - beginIndex > 1) {
            // It makes sense to consider this group as a batch entry.
            auto& listEntry = ListEntries_.emplace_back();
            listEntry.RequestedEntryCount = endIndex - beginIndex;
            listEntry.DirName = Entries_[beginIndex].DirName;
            // Link entries and batch entry together.
            for (auto index = beginIndex; index != endIndex; ++index) {
                auto& entry = Entries_[index];
                entry.FetchAsBatch = true;
                listEntry.BaseNameToEntry[entry.BaseName] = &entry;
            }
        }
    }
}

TFuture<void> TBatchAttributeFetcher::Fetch()
{
    if (Entries_.empty()) {
        return VoidFuture;
    }

    return BIND(&TBatchAttributeFetcher::DoFetch, MakeWeak(this))
        .AsyncVia(Invoker_)
        .Run();
}

void TBatchAttributeFetcher::SetupBatchRequest(const TObjectServiceProxy::TReqExecuteBatchPtr& batchReq)
{
    SetBalancingHeader(batchReq, Client_->GetNativeConnection(), MasterReadOptions_);
}

void TBatchAttributeFetcher::SetupYPathRequest(const TYPathRequestPtr& req)
{
    SetCachingHeader(req, Client_->GetNativeConnection(), MasterReadOptions_);
}

void TBatchAttributeFetcher::FetchBatchCounts()
{
    if (ListEntries_.empty()) {
        return;
    }

    YT_LOG_DEBUG("Collecting node counts (DirectoryCount: %v)", ListEntries_.size());

    auto proxy = CreateObjectServiceReadProxy(Client_, MasterReadOptions_.ReadFrom);
    auto batchReq = proxy.ExecuteBatch();
    SetupBatchRequest(batchReq);

    for (auto& listEntry : ListEntries_) {
        auto req = TYPathProxy::Get(listEntry.DirName + "/@count");
        SetupYPathRequest(req);
        req->Tag() = &listEntry;
        batchReq->AddRequest(req);
    }

    auto result = WaitFor(batchReq->Invoke())
        .ValueOrThrow();

    YT_LOG_DEBUG("Node counts collected");

    for (const auto& [tag, countOrError] : result->GetTaggedResponses<TYPathProxy::TRspGet>()) {
        auto* listEntry = std::any_cast<TListEntry*>(tag);
        if (countOrError.IsOK()) {
            listEntry->DirNodeCount = ConvertTo<ui64>(TYsonString(countOrError.Value()->value()));
            // If there are too many "useless" items except requested ones,
            // perform fallback to regular Get requests.
            if (listEntry->DirNodeCount - listEntry->RequestedEntryCount > MaxUnusedNodeCount) {
                YT_LOG_DEBUG(
                    "There are too many nodes in directory, falling back to singular get requests "
                    "(DirName: %v, RequestedNodeCount: %v, DirNodeCount: %v)",
                    listEntry->DirName,
                    listEntry->RequestedEntryCount,
                    listEntry->DirNodeCount);
                listEntry->FetchAsBatch = false;
                for (auto* entry : GetValues(listEntry->BaseNameToEntry)) {
                    entry->FetchAsBatch = false;
                }
            }
        } else {
            for (auto* entry : GetValues(listEntry->BaseNameToEntry)) {
                entry->Error = countOrError;
            }
        }
    }
}

void TBatchAttributeFetcher::FetchAttributes()
{
    auto proxy = CreateObjectServiceReadProxy(Client_, MasterReadOptions_.ReadFrom);
    auto batchReq = proxy.ExecuteBatch();
    SetupBatchRequest(batchReq);

    int listCount = 0;
    int listEntryCount = 0;
    int getCount = 0;

    for (auto& listEntry : ListEntries_) {
        if (listEntry.FetchAsBatch) {
            auto req = TYPathProxy::List(listEntry.DirName);
            ToProto(req->mutable_attributes()->mutable_keys(), AttributeNames_);
            SetupYPathRequest(req);
            ++listCount;
            listEntryCount += listEntry.BaseNameToEntry.size();
            req->Tag() = &listEntry;
            batchReq->AddRequest(req, "list");
        }
    }

    for (auto& entry : Entries_) {
        if (!entry.FetchAsBatch && entry.Error.IsOK()) {
            auto req = TYPathProxy::Get(entry.DirName + "/" + entry.BaseName + "/@");
            ToProto(req->mutable_attributes()->mutable_keys(), AttributeNames_);
            SetupYPathRequest(req);
            req->Tag() = &entry;
            ++getCount;
            batchReq->AddRequest(req, "get");
        }
    }

    YT_LOG_DEBUG(
        "Performing batch attribute request (ListCount: %v, ListEntryCount: %v, GetCount: %v)",
        listCount,
        listEntryCount,
        getCount);

    auto result = WaitFor(batchReq->Invoke())
        .ValueOrThrow();

    // Process list responses.
    for (const auto& [tag, listRspOrError] : result->GetTaggedResponses<TYPathProxy::TRspList>("list")) {
        auto* listEntry = std::any_cast<TListEntry*>(tag);
        if (listRspOrError.IsOK()) {
            const auto& listYson = TYsonString(listRspOrError.Value()->value());
            for (const auto& stringNode : ConvertTo<std::vector<IStringNodePtr>>(listYson)) {
                const auto& baseName = stringNode->GetValue();
                auto it = listEntry->BaseNameToEntry.find(baseName);
                if (it != listEntry->BaseNameToEntry.end()) {
                    it->second->Attributes = stringNode->Attributes().Clone();
                }
            }
            // Fill resolution errors for missing paths.
            for (auto* entry : GetValues(listEntry->BaseNameToEntry)) {
                if (!entry->Attributes) {
                    entry->Error = TError(
                        NYTree::EErrorCode::ResolveError,
                        "Node %v has no child %v",
                        entry->DirName,
                        ToYPathLiteral(entry->BaseName));
                }
            }
        } else {
            for (auto* entry : GetValues(listEntry->BaseNameToEntry)) {
                entry->Error = listRspOrError;
            }
        }
    }

    // Process get responses.
    for (const auto& [tag, getRspOrError] : result->GetTaggedResponses<TYPathProxy::TRspGet>("get")) {
        auto* entry = std::any_cast<TEntry*>(tag);
        if (getRspOrError.IsOK()) {
            const auto& getYson = TYsonString(getRspOrError.Value()->value());
            entry->Attributes = ConvertToAttributes(getYson);
        } else {
            entry->Error = getRspOrError;
        }
    }

    YT_LOG_DEBUG("Batch attribute request finished");
}

void TBatchAttributeFetcher::FetchSymlinks()
{
    // When fetching via list request, we do not resolve symlinks.
    // When such situation occurs, we have to perform second pass
    // manually resolving symlinks via get requests.

    auto proxy = CreateObjectServiceReadProxy(Client_, MasterReadOptions_.ReadFrom);
    auto batchReq = proxy.ExecuteBatch();
    SetupBatchRequest(batchReq);

    int linkCount = 0;

    for (auto& entry : Entries_) {
        if (entry.Error.IsOK()) {
            YT_VERIFY(entry.Attributes);
            if (entry.Attributes->Get<EObjectType>(TypeAttributeName) == EObjectType::Link) {
                auto req = TYPathProxy::Get(entry.DirName + "/" + entry.BaseName + "/@");
                SetupYPathRequest(req);
                ToProto(req->mutable_attributes()->mutable_keys(), AttributeNames_);
                req->Tag() = &entry;
                ++linkCount;
                batchReq->AddRequest(req);
            }
        }
    }

    if (!linkCount) {
        return;
    }

    YT_LOG_DEBUG("Fetching link attributes (LinkCount: %v)", linkCount);

    auto result = WaitFor(batchReq->Invoke())
        .ValueOrThrow();

    YT_LOG_DEBUG("Link attributes fetched");

    for (const auto& [tag, getRspOrError] : result->GetTaggedResponses<TYPathProxy::TRspGet>()) {
        auto* entry = std::any_cast<TEntry*>(tag);
        if (getRspOrError.IsOK()) {
            const auto& getYson = TYsonString(getRspOrError.Value()->value());
            entry->Attributes = ConvertToAttributes(getYson);
        } else {
            entry->Attributes = nullptr;
            entry->Error = getRspOrError;
        }
    }
}

void TBatchAttributeFetcher::FillResult()
{
    // Move results to the public field.
    for (auto& entry : Entries_) {
        if (!entry.Error.IsOK()) {
            Attributes_[entry.Index] = std::move(entry.Error);
        } else {
            YT_VERIFY(entry.Attributes);
            // We are modifying internal state here, but we don't care anymore.
            if (AddedTypeAttribute_) {
                entry.Attributes->Remove(TypeAttributeName);
            }
            Attributes_[entry.Index] = std::move(entry.Attributes);
        }
    }
    for (size_t index = 0; index < Attributes_.size(); ++index) {
        auto referenceTableIndex = DeduplicationReferenceTableIndices_[index];
        if (referenceTableIndex != -1) {
            Attributes_[index] = Attributes_[referenceTableIndex];
        }
    }
}

void TBatchAttributeFetcher::DoFetch()
{
    YT_LOG_DEBUG("Batch attribute fetcher started");
    FetchBatchCounts();
    FetchAttributes();
    FetchSymlinks();
    FillResult();
    YT_LOG_DEBUG("Batch attribute fetcher finished");
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressClient
