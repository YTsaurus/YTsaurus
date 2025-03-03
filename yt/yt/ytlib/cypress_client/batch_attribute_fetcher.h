#pragma once

#include "public.h"

#include <yt/yt/ytlib/api/native/public.h>

#include <yt/yt/ytlib/object_client/object_service_proxy.h>

#include <yt/yt/client/api/client.h>

#include <yt/yt/client/misc/public.h>

#include <yt/yt/core/ypath/public.h>

namespace NYT::NCypressClient {

////////////////////////////////////////////////////////////////////////////////

//! Helper class for fetching fixed attribute list for given paths in Cypress.
//! When several nodes are requested from same directory, they may additionally
//! be grouped together in a single list with attributes request.
class TBatchAttributeFetcher
    : public TRefCounted
{
public:
    DEFINE_BYREF_RW_PROPERTY(std::vector<TErrorOr<NYTree::IAttributeDictionaryPtr>>, Attributes);

public:
    TBatchAttributeFetcher(
        const std::vector<NYPath::TYPath>& paths,
        const std::vector<std::string>& attributeNames,
        const NApi::NNative::IClientPtr& client,
        const IInvokerPtr& invoker,
        const NLogging::TLogger& logger,
        const NApi::TMasterReadOptions& options = {});

    TFuture<void> Fetch();

private:
    struct TEntry
    {
        NYPath::TYPath DirName;
        TString BaseName;
        bool FetchAsBatch = false;
        TError Error;
        NCypressClient::EObjectType Type;
        NYTree::IAttributeDictionaryPtr Attributes;
        //! Index in original path order.
        int Index;
    };

    std::vector<TEntry> Entries_;

    struct TListEntry
    {
        //! Number of requested entries from this directory.
        int RequestedEntryCount = 0;
        //! Total number of nodes in this directory.
        int DirNodeCount = 0;
        NYPath::TYPath DirName;
        THashMap<TString, TEntry*> BaseNameToEntry;
        //! If set to false, directory is too heavy to be fetched as a List request.
        bool FetchAsBatch = true;
    };

    std::vector<TListEntry> ListEntries_;

    std::vector<std::string> AttributeNames_;
    //! Used to track if we added the "type" attribute to the list of requested attributes manually.
    bool AddedTypeAttribute_ = false;
    NApi::NNative::IClientPtr Client_;
    IInvokerPtr Invoker_;
    NApi::TMasterReadOptions MasterReadOptions_;

    NLogging::TLogger Logger;

    //! Used to deal with duplicating tables, mapping from duplicating table index into index of
    //! a table to copy attributes from.
    std::vector<int> DeduplicationReferenceTableIndices_;

    void SetupBatchRequest(const NObjectClient::TObjectServiceProxy::TReqExecuteBatchPtr& batchReq);
    void SetupYPathRequest(const NYTree::TYPathRequestPtr& req);

    void FetchBatchCounts();
    void FetchAttributes();
    void FetchSymlinks();
    void FillResult();

    void DoFetch();
};

DEFINE_REFCOUNTED_TYPE(TBatchAttributeFetcher)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressClient
