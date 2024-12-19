#include "scion_type_handler.h"

#include "grafting_manager.h"
#include "node_detail.h"
#include "scion_node.h"
#include "scion_proxy.h"

namespace NYT::NCypressServer {

using namespace NCellMaster;
using namespace NCypressServer;
using namespace NObjectClient;
using namespace NObjectServer;
using namespace NSequoiaClient;
using namespace NTransactionServer;

////////////////////////////////////////////////////////////////////////////////

class TScionTypeHandler
    : public TSequoiaMapNodeTypeHandlerImpl<TScionNode>
{
public:
    using TSequoiaMapNodeTypeHandlerImpl::TSequoiaMapNodeTypeHandlerImpl;

    EObjectType GetObjectType() const override
    {
        return EObjectType::Scion;
    }

    ETypeFlags GetFlags() const override
    {
        return ETypeFlags::None;
    }

private:
    ICypressNodeProxyPtr DoGetProxy(
        TScionNode* trunkNode,
        TTransaction* transaction) override
    {
        return CreateScionProxy(
            GetBootstrap(),
            &Metadata_,
            transaction,
            trunkNode);
    }

    void DoDestroy(TScionNode* node) override
    {
        if (node->IsTrunk()) {
            const auto& graftingManager = GetBootstrap()->GetGraftingManager();
            graftingManager->OnScionDestroyed(node);
        }

        TCypressNodeTypeHandlerBase::DoDestroy(node);
    }

    void DoSerializeNode(
        TScionNode* /*node*/,
        TSerializeNodeContext* /*context*/) override
    {
        THROW_ERROR_EXCEPTION("Cross-cell copying of scions is not supported");
    }

    void DoMaterializeNode(
        TScionNode* /*trunkNode*/,
        TMaterializeNodeContext* /*context*/) override
    {
        THROW_ERROR_EXCEPTION("Cross-cell copying of scions is not supported");
    }
};

////////////////////////////////////////////////////////////////////////////////

INodeTypeHandlerPtr CreateScionTypeHandler(TBootstrap* bootstrap)
{
    return New<TScionTypeHandler>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressServer
