#pragma once

#include "spilling_counters.h"

#include <library/cpp/threading/future/future.h>

#include <contrib/ydb/library/yql/dq/runtime/dq_channel_storage.h>
#include "contrib/ydb/library/yql/dq/common/dq_common.h"

#include <contrib/ydb/library/actors/core/actor.h>

namespace NYql::NDq {

struct TDqChannelStorageActorEvents {
    enum {
        EvPut = EventSpaceBegin(NActors::TEvents::EEventSpace::ES_USERSPACE) + 30100,
        EvGet
    };
};

struct TEvDqChannelSpilling {
    struct TEvPut : NActors::TEventLocal<TEvPut, TDqChannelStorageActorEvents::EvPut> {
        TEvPut(ui64 blobId, TChunkedBuffer&& blob, NThreading::TPromise<void>&& promise)
            : BlobId_(blobId)
            , Blob_(std::move(blob))
            , Promise_(std::move(promise))
        {
        }

        ui64 BlobId_;
        TChunkedBuffer Blob_;
        NThreading::TPromise<void> Promise_;
    };

    struct TEvGet : NActors::TEventLocal<TEvGet, TDqChannelStorageActorEvents::EvGet> {
        TEvGet(ui64 blobId, NThreading::TPromise<TBuffer>&& promise)
            : BlobId_(blobId)
            , Promise_(std::move(promise))
        {
        }

        ui64 BlobId_;
        NThreading::TPromise<TBuffer> Promise_;
    };
};

class IDqChannelStorageActor
{
public:
    using TPtr = TIntrusivePtr<IDqChannelStorageActor>;
    using TWakeUpCallback = std::function<void()>;

    virtual ~IDqChannelStorageActor() = default;

    virtual NActors::IActor* GetActor() = 0;
};


IDqChannelStorageActor* CreateDqChannelStorageActor(TTxId txId, ui64 channelId, TWakeUpCallback&& wakeUpCallback, TErrorCallback&& errorCallback,
    TIntrusivePtr<TSpillingTaskCounters> spillingTaskCounters, NActors::TActorSystem* actorSystem);

} // namespace NYql::NDq
