#include "helpers.h"

#include "path_resolver.h"
#include "sequoia_service.h"

#include <yt/yt/ytlib/cypress_client/rpc_helpers.h>

#include <yt/yt/ytlib/cypress_client/proto/cypress_ypath.pb.h>

#include <yt/yt/ytlib/object_client/master_ypath_proxy.h>

#include <yt/yt/ytlib/sequoia_client/ypath_detail.h>

#include <yt/yt/client/object_client/helpers.h>

#include <library/cpp/yt/misc/variant.h>

namespace NYT::NCypressProxy {

using namespace NApi::NNative;
using namespace NConcurrency;
using namespace NCypressClient;
using namespace NCypressClient::NProto;
using namespace NObjectClient;
using namespace NRpc;
using namespace NSequoiaClient;
using namespace NYPath;
using namespace NYTree;

using TYPath = NSequoiaClient::TYPath;
using TYPathBuf = NSequoiaClient::TYPathBuf;

////////////////////////////////////////////////////////////////////////////////

namespace {

TYPathBuf SkipAmpersand(TYPathBuf pathSuffix)
{
    TTokenizer tokenizer(pathSuffix.Underlying());
    tokenizer.Advance();
    tokenizer.Skip(ETokenType::Ampersand);
    return TYPathBuf(tokenizer.GetInput());
}

TAbsoluteYPath GetCanonicalYPath(const TResolveResult& resolveResult)
{
    return Visit(resolveResult,
        [] (const TCypressResolveResult& resolveResult) -> TAbsoluteYPath {
            // NB: Cypress resolve result doesn't contain unresolved links.
            return TAbsoluteYPath(resolveResult.Path);
        },
        [] (const TSequoiaResolveResult& resolveResult) -> TAbsoluteYPath {
            // We don't want to distinguish "//tmp/a&/my-link" from
            // "//tmp/a/my-link".
            return resolveResult.Path + SkipAmpersand(resolveResult.UnresolvedSuffix);
        });
}

} // namespace

void ValidateLinkNodeCreation(
    const TSequoiaSessionPtr& session,
    TRawYPath targetPath,
    const TResolveResult& resolveResult)
{
    // TODO(danilalexeev): In case of a master-object root designator the
    // following resolve will not produce a meaningful result. Such YPath has to
    // be resolved by master first.
    // TODO(kvk1920): probably works (since links are stored in both resolve
    // tables now), but has to be tested.
    auto linkPath = GetCanonicalYPath(resolveResult);

    auto checkAcyclicity = [&] (
        TRawYPath pathToResolve,
        const TAbsoluteYPath& forbiddenPrefix)
    {
        std::vector<TSequoiaResolveIterationResult> history;
        auto resolveResult = ResolvePath(session, std::move(pathToResolve), /*method*/ {}, &history);

        for (const auto& [id, path] : history) {
            if (IsLinkType(TypeFromId(id)) && path == forbiddenPrefix) {
                return false;
            }
        }

        return GetCanonicalYPath(resolveResult) != forbiddenPrefix;
    };

    if (!checkAcyclicity(targetPath, linkPath)) {
        THROW_ERROR_EXCEPTION("Failed to create link: link is cyclic")
            << TErrorAttribute("target_path", targetPath.Underlying())
            << TErrorAttribute("path", linkPath);
    }
}

////////////////////////////////////////////////////////////////////////////////

std::vector<TString> TokenizeUnresolvedSuffix(TYPathBuf unresolvedSuffix)
{
    constexpr auto TypicalPathTokenCount = 3;
    std::vector<TString> pathTokens;
    pathTokens.reserve(TypicalPathTokenCount);

    TTokenizer tokenizer(unresolvedSuffix.Underlying());
    tokenizer.Advance();

    while (tokenizer.GetType() != ETokenType::EndOfStream) {
        tokenizer.Expect(ETokenType::Slash);
        tokenizer.Advance();
        tokenizer.Expect(ETokenType::Literal);
        pathTokens.push_back(tokenizer.GetLiteralValue());
        tokenizer.Advance();
    };

    return pathTokens;
}

////////////////////////////////////////////////////////////////////////////////

bool IsSupportedSequoiaType(EObjectType type)
{
    return IsSequoiaCompositeNodeType(type) ||
        IsScalarType(type) ||
        IsChunkOwnerType(type) ||
        type == EObjectType::SequoiaLink;
}

bool IsSequoiaCompositeNodeType(EObjectType type)
{
    return type == EObjectType::SequoiaMapNode || type == EObjectType::Scion;
}

void ValidateSupportedSequoiaType(EObjectType type)
{
    if (!IsSupportedSequoiaType(type)) {
        THROW_ERROR_EXCEPTION(
            "Object type %Qlv is not supported in Sequoia yet",
            type);
    }
}

void ThrowAlreadyExists(const TAbsoluteYPath& path)
{
    THROW_ERROR_EXCEPTION(
        NYTree::EErrorCode::AlreadyExists,
        "Node %v already exists",
        path);
}

void ThrowNoSuchChild(const TAbsoluteYPath& existingPath, TStringBuf missingPath)
{
    THROW_ERROR_EXCEPTION(
        NYTree::EErrorCode::ResolveError,
        "Node %v has no child with key %Qv",
        existingPath,
        missingPath);
}

////////////////////////////////////////////////////////////////////////////////

std::optional<TParsedReqCreate> TryParseReqCreate(ISequoiaServiceContextPtr context)
{
    YT_VERIFY(context->GetRequestHeader().method() == "Create");

    auto typedContext = New<TTypedSequoiaServiceContext<TReqCreate, TRspCreate>>(
        std::move(context),
        THandlerInvocationOptions{});

    // NB: this replies to underlying context on error.
    if (!typedContext->DeserializeRequest()) {
        return std::nullopt;
    }

    const auto& request = typedContext->Request();

    try {
        return TParsedReqCreate{
            .Type = CheckedEnumCast<EObjectType>(request.type()),
            .ExplicitAttributes = request.has_node_attributes()
                ? NYTree::FromProto(request.node_attributes())
                : CreateEphemeralAttributes(),
        };
    } catch (const std::exception& ex) {
        typedContext->Reply(ex);
        return std::nullopt;
    }
}

////////////////////////////////////////////////////////////////////////////////

void FromProto(TCopyOptions* options, const TReqCopy& protoOptions)
{
    options->Mode = CheckedEnumCast<ENodeCloneMode>(protoOptions.mode());
    options->PreserveAcl = protoOptions.preserve_acl();
    options->PreserveAccount = protoOptions.preserve_account();
    options->PreserveOwner = protoOptions.preserve_owner();
    options->PreserveCreationTime = protoOptions.preserve_creation_time();
    options->PreserveModificationTime = protoOptions.preserve_modification_time();
    options->PreserveExpirationTime = protoOptions.preserve_expiration_time();
    options->PreserveExpirationTimeout = protoOptions.preserve_expiration_timeout();
    options->PessimisticQuotaCheck = protoOptions.pessimistic_quota_check();
}

////////////////////////////////////////////////////////////////////////////////

TFuture<NYTree::INodePtr> FetchSingleObject(
    const IClientPtr& client,
    TVersionedObjectId objectId,
    const TAttributeFilter& attributeFilter)
{
    auto request = TYPathProxy::Get();

    if (objectId.TransactionId) {
        SetTransactionId(request, objectId.TransactionId);
    }

    if (attributeFilter) {
        ToProto(request->mutable_attributes(), attributeFilter);
    }

    auto batcher = TMasterYPathProxy::CreateGetBatcher(client, request, {objectId.ObjectId});

    return batcher.Invoke().Apply(BIND([=] (const TMasterYPathProxy::TVectorizedGetBatcher::TVectorizedResponse& rsp) {
        return ConvertToNode(NYson::TYsonString(rsp.at(objectId.ObjectId).ValueOrThrow()->value()));
    }));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressProxy
