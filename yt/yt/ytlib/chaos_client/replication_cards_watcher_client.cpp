#include "replication_cards_watcher_client.h"
#include "chaos_node_service_proxy.h"

#include <yt/yt/ytlib/api/native/config.h>
#include <yt/yt/ytlib/api/native/connection.h>

#include <yt/yt/client/chaos_client/replication_card_serialization.h>
#include <yt/yt/client/transaction_client/public.h>

#include <yt/yt/core/actions/future.h>
#include <yt/yt/core/misc/protobuf_helpers.h>

#include <library/cpp/yt/threading/spin_lock.h>

namespace NYT::NChaosClient {

using namespace NHydra;
using namespace NRpc;
using namespace NThreading;
using namespace NTransactionClient;
using namespace NApi::NNative;

using NYT::FromProto;

////////////////////////////////////////////////////////////////////////////////

static const auto Logger = NLogging::TLogger("ReplicationCardWatcherClient");
static constexpr int StickyGroupSize = 3;

////////////////////////////////////////////////////////////////////////////////

class TReplicationCardsWatcherClient
    : public IReplicationCardsWatcherClient
{
public:
    TReplicationCardsWatcherClient(
        std::unique_ptr<IReplicationCardWatcherClientCallbacks> callbacks,
        TWeakPtr<NApi::NNative::IConnection> connection)
        : Connection_(std::move(connection))
        , Callbacks_(std::move(callbacks))
    { }

    TReplicationCardsWatcherClient(
        std::unique_ptr<IReplicationCardWatcherClientCallbacks> callbacks,
        IChannelPtr chaosCacheChannel,
        TWeakPtr<NApi::NNative::IConnection> connection)
        : Connection_(std::move(connection))
        , ChaosCacheChannel_(std::move(chaosCacheChannel))
        , Callbacks_(std::move(callbacks))
    { }

    void WatchReplicationCard(const TReplicationCardId& replicationCardId) override
    {
        auto guard = Guard(Lock_);
        auto& [future, timestamp] = WatchingFutures_[replicationCardId];
        if (future) {
            return;
        }

        future = WatchUpstream(replicationCardId, timestamp);
    }

    void StopWatchingReplicationCard(const TReplicationCardId& replicationCardId) override
    {
        TFuture<void> localFuture;
        {
            auto guard = Guard(Lock_);
            auto it = WatchingFutures_.find(replicationCardId);
            if (it == WatchingFutures_.end()) {
                return;
            }
            localFuture = std::move(it->second.first);
            WatchingFutures_.erase(it);
        }

        localFuture.Cancel(TError("Stopped watching"));
    }

private:
    const TWeakPtr<IConnection> Connection_;
    const IChannelPtr ChaosCacheChannel_;

    std::unique_ptr<IReplicationCardWatcherClientCallbacks> Callbacks_;

    YT_DECLARE_SPIN_LOCK(TSpinLock, Lock_);
    THashMap<TReplicationCardId, std::pair<TFuture<void>, TTimestamp>> WatchingFutures_;

    TFuture<void> WatchUpstream(const TReplicationCardId& replicationCardId, TTimestamp timestamp)
    {
        auto connection = Connection_.Lock();
        if (connection == nullptr) {
            return MakeFuture(TError("Connection is not available"));
        }

        auto channel = ChaosCacheChannel_;
        if (channel == nullptr) {
            channel = connection->GetChaosChannelByCardId(replicationCardId, EPeerKind::Leader);
        }

        auto proxy = TChaosNodeServiceProxy(std::move(channel));
        proxy.SetDefaultTimeout(connection->GetConfig()->DefaultChaosWatcherClientRequestTimeout);

        auto req = proxy.WatchReplicationCard();
        ToProto(req->mutable_replication_card_id(), replicationCardId);
        req->set_replication_card_cache_timestamp(timestamp);

        auto* balancingHeaderExt = req->Header().MutableExtension(NRpc::NProto::TBalancingExt::balancing_ext);
        balancingHeaderExt->set_enable_stickiness(true);
        balancingHeaderExt->set_sticky_group_size(StickyGroupSize);
        balancingHeaderExt->set_balancing_hint(THash<TReplicationCardId>()(replicationCardId));

        return req->Invoke().ApplyUnique(
            BIND(
                &TReplicationCardsWatcherClient::OnReplicationCardWatchResponse,
                MakeStrong(this),
                replicationCardId)
            .AsyncVia(GetCurrentInvoker()));
    }

    void OnReplicationCardWatchResponse(
        const TReplicationCardId& replicationCardId,
        TErrorOr<TChaosNodeServiceProxy::TRspWatchReplicationCardPtr>&& response)
    {
        if (!response.IsOK()) {
            auto guard = Guard(Lock_);
            WatchingFutures_.erase(replicationCardId);
            YT_LOG_DEBUG("Watching is not ok (Response: %v)",
                response);
            return;
        }

        const auto& value = response.Value();
        auto guard = Guard(Lock_);
        auto& [future, timestamp] = WatchingFutures_[replicationCardId];
        auto localFuture = std::move(future);
        if (value->has_replication_card_deleted()) {
            WatchingFutures_.erase(replicationCardId);
            guard.Release();
            Callbacks_->OnReplicationCardDeleted(replicationCardId);
            return;
        }

        if (value->has_unknown_replication_card()) {
            WatchingFutures_.erase(replicationCardId);
            guard.Release();
            YT_LOG_DEBUG("Unknown replication card (Response: %v)", response);
            Callbacks_->OnUnknownReplicationCard(replicationCardId);
            return;
        }

        if (value->has_replication_card_changed()) {
            const auto& newCardResponse = value->replication_card_changed();
            timestamp = FromProto<TTimestamp>(newCardResponse.replication_card_cache_timestamp());

            auto replicationCard = New<TReplicationCard>();
            FromProto(replicationCard.Get(), newCardResponse.replication_card());

            future = WatchUpstream(replicationCardId, timestamp);
            guard.Release();
            YT_LOG_DEBUG("Replication card changed (Response: %v)", response);
            Callbacks_->OnReplicationCardUpdated(replicationCardId, std::move(replicationCard), timestamp);
            return;
        }

        if (value->has_replication_card_not_changed()) {
            future = WatchUpstream(replicationCardId, timestamp);
            guard.Release();
            YT_LOG_DEBUG("Replication card not changed (Response: %v)", response);
            Callbacks_->OnNothingChanged(replicationCardId);
            return;
        }

        if (value->has_replication_card_migrated()) {
            //TODO: Hint residency cache about migration?
            future = WatchUpstream(replicationCardId, timestamp);
            guard.Release();
            YT_LOG_DEBUG("Replication card migrated (Response: %v)", response);
            Callbacks_->OnNothingChanged(replicationCardId);
            return;
        }

        if (value->has_instance_is_not_leader()) {
            future = WatchUpstream(replicationCardId, timestamp);
            guard.Release();
            YT_LOG_DEBUG("Instance is not leader (Response: %v)", response);
            Callbacks_->OnNothingChanged(replicationCardId);
            return;
        }
    }
};

IReplicationCardsWatcherClientPtr CreateReplicationCardsWatcherClient(
    std::unique_ptr<IReplicationCardWatcherClientCallbacks> callbacks,
    TWeakPtr<IConnection> connection)
{
    return New<TReplicationCardsWatcherClient>(
        std::move(callbacks),
        std::move(connection));
}

IReplicationCardsWatcherClientPtr CreateReplicationCardsWatcherClient(
    std::unique_ptr<IReplicationCardWatcherClientCallbacks> callbacks,
    IChannelPtr chaosCacheChannel,
    TWeakPtr<NApi::NNative::IConnection> connection)
{
    return New<TReplicationCardsWatcherClient>(
        std::move(callbacks),
        std::move(chaosCacheChannel),
        std::move(connection));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChaosClient
