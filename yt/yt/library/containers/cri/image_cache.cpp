#include "image_cache.h"
#include "private.h"

#include <yt/yt/core/misc/async_slru_cache.h>

namespace NYT::NContainers::NCri {

////////////////////////////////////////////////////////////////////////////////

TCriImageCacheEntry::TCriImageCacheEntry(TCriImageDescriptor image, i64 imageSize)
    : TAsyncCacheValueBase(image.Image)
    , Image_(std::move(image))
    , ImageSize_(imageSize)
{
    SetLastUsageTime(TInstant::Now());
}

TCriImageCacheEntry::TCriImageCacheEntry(TCriImageDescriptor imageAlias, TCriImageCacheEntryPtr entry)
    : TAsyncCacheValueBase(imageAlias.Image)
    , Image_(std::move(imageAlias))
    , ImageSize_(entry->GetImageSize())
    , Parent_(std::move(entry))
{
    YT_VERIFY(Image_.Id == Parent_->Image_.Id);
    SetLastUsageTime(TInstant::Now());

    auto guard = Guard(Parent_->SpinLock_);
    Parent_->Aliases_.PushBack(this);
}

TCriImageCacheEntry::~TCriImageCacheEntry()
{
    if (Parent_) {
        auto guard = Guard(Parent_->SpinLock_);
        Unlink();
    }
}

bool TCriImageCacheEntry::IsAlias() const
{
    return bool(Parent_);
}

bool TCriImageCacheEntry::HasAliases() const
{
    auto guard = Guard(SpinLock_);
    return !Aliases_.Empty();
}

////////////////////////////////////////////////////////////////////////////////

class TCriImageCache
    : public ICriImageCache
    , private TAsyncSlruCacheBase<TString, TCriImageCacheEntry>
{
public:
    TCriImageCache(TCriImageCacheConfigPtr config, ICriExecutorPtr executor)
        : TAsyncSlruCacheBase(config)
        , Config_(std::move(config))
        , Executor_(std::move(executor))
    { }

    TFuture<void> Initialize() override
    {
        YT_LOG_DEBUG("Initializing docker image cache");

        std::vector<TCallback<TFuture<TCriImageCacheEntryPtr>()>> prefetchers;
        for (const auto& prefetchImage : Config_->PinnedImages) {
            TCriImageDescriptor image{.Image = prefetchImage};
            prefetchers.push_back(BIND(&TCriImageCache::PullImage,
                    MakeStrong(this),
                    image,
                    /*authConfig*/ nullptr,
                    EImagePullPolicy::Default));
        }

        return Executor_->ListImages()
            .Apply(BIND([=, this, this_ = MakeStrong(this)] (const TCriImageApi::TRspListImagesPtr& imagesList) {
                for (auto protoImage : imagesList->images()) {
                    bool managed = false;
                    for (const auto& repoTag : protoImage.repo_tags()) {
                        TCriImageDescriptor alias{.Image = repoTag};
                        if (IsManagedImage(alias)) {
                            auto cookie = BeginInsert(alias.Image);
                            if (cookie.IsActive()) {
                                DoInsertImage(cookie, protoImage);
                            }
                            managed = true;
                        }
                    }
                    for (const auto& repoDigest : protoImage.repo_digests()) {
                        TCriImageDescriptor alias{.Image = repoDigest};
                        if (IsManagedImage(alias)) {
                            auto cookie = BeginInsert(alias.Image);
                            if (cookie.IsActive()) {
                                DoInsertImage(cookie, protoImage);
                            }
                            managed = true;
                        }
                    }
                    if (!managed) {
                        YT_LOG_DEBUG("Found unmanaged docker image entry (Image: %v, ImageId: %v, ImageSize: %v)",
                            protoImage.repo_tags().size() != 0 ? protoImage.repo_tags()[0] : protoImage.id(),
                            protoImage.id(),
                            protoImage.size());
                    }
                }
                return PruneImages();
            }))
            .Apply(BIND([prefetchers = std::move(prefetchers)] () mutable {
                return CancelableRunWithBoundedConcurrency(std::move(prefetchers), 1)
                    .AsVoid();
            }));
    }

    std::vector<TCriImageCacheEntryPtr> ListImages() override
    {
        return GetAll();
    }

    void TouchImage(const TCriImageCacheEntryPtr& entry)
    {
        auto now = TInstant::Now();
        Touch(entry);
        entry->SetLastUsageTime(now);
        if (const auto& parent = entry->Parent()) {
            Touch(parent);
            parent->SetLastUsageTime(now);
        }
    }

    TFuture<TCriImageCacheEntryPtr> LookupImage(const TCriImageDescriptor& image) override
    {
        if (auto valueFuture = Lookup(image.Image)) {
            return valueFuture
                .Apply(BIND([=, this] (const TCriImageCacheEntryPtr& entry) {
                    TouchImage(entry);
                    return entry;
                }));
        }

        return Executor_->GetImageStatus(image)
            .Apply(BIND([=, this, this_ = MakeStrong(this)] (const TCriImageApi::TRspImageStatusPtr& imageStatus) {
                if (!imageStatus->has_image()) {
                    return MakeFuture<TCriImageCacheEntryPtr>(TError("Docker image not found in cache (Image: %v)", image));
                }
                auto protoImage = imageStatus->image();
                if (!IsManagedImage(image)) {
                    TCriImageDescriptor cachedImage{
                        .Image = image.Image,
                        .Id = protoImage.id(),
                    };
                    auto entry = New<TCriImageCacheEntry>(cachedImage, protoImage.size());
                    YT_LOG_DEBUG("Unmanaged docker image found in cache (Image: %v)", cachedImage);
                    return MakeFuture(entry);
                }
                auto cookie = BeginInsert(image.Image);
                auto imageFuture = cookie.GetValue();
                if (cookie.IsActive()) {
                    DoInsertImage(cookie, protoImage);
                }
                return imageFuture;
            }));
    }

    TFuture<TCriImageCacheEntryPtr> PullImage(const TCriImageDescriptor& image, TCriAuthConfigPtr authConfig, EImagePullPolicy pullPolicy) override
    {
        if (!IsManagedImage(image)) {
            // Bypass cache for unmanaged images.
            return Executor_->GetImageStatus(image)
                .Apply(BIND([=] (const TCriImageApi::TRspImageStatusPtr& imageStatus) {
                    if (imageStatus->has_image()) {
                        auto protoImage = imageStatus->image();
                        TCriImageDescriptor cachedImage {
                            .Image = image.Image,
                            .Id = protoImage.id(),
                        };
                        YT_LOG_DEBUG("Unmanaged docker image found in cache (Image: %v)", cachedImage);
                        return New<TCriImageCacheEntry>(cachedImage, protoImage.size());
                    } else {
                        THROW_ERROR_EXCEPTION("Unmanaged docker image not found in cache (Image: %v)",
                            image);
                    }
                }));
        }

        switch (pullPolicy) {
            case EImagePullPolicy::Never:
                return LookupImage(image);
            case EImagePullPolicy::Missing:
                break;
            case EImagePullPolicy::Default:
                if (Config_->AlwaysPullLatest && image.Image.EndsWith(":latest")) {
                    pullPolicy = EImagePullPolicy::Always;
                }

                if (Config_->PullPeriod) {
                    auto imageEntry = Find(image.Image);
                    if (imageEntry && imageEntry->GetLastPullTime() + Config_->PullPeriod < TInstant::Now()) {
                        pullPolicy = EImagePullPolicy::Always;
                    }
                }

                if (pullPolicy != EImagePullPolicy::Always) {
                    break;
                }
                [[fallthrough]];
            case EImagePullPolicy::Always:
                // Should not remove incomplete entries.
                TryRemove(image.Image, /*forbidResurrection*/ true);
                break;
        }

        auto cookie = BeginInsert(image.Image, Config_->ImageSizeEstimation);

        if (!cookie.IsActive()) {
            auto imageFuture = cookie.GetValue();

            if (auto entryOrError = imageFuture.TryGet(); entryOrError && entryOrError->IsOK()) {
                return Executor_->GetImageStatus(image)
                    .Apply(BIND([=, this, this_ = MakeStrong(this), entry = std::move(entryOrError->Value())] (
                                const TCriImageApi::TRspImageStatusPtr& imageStatus) mutable {
                        if (!imageStatus->has_image() || imageStatus->image().id() != entry->Image().Id) {
                            YT_LOG_DEBUG("Docker image was removed externally (Image: %v)", entry->Image());
                            TryRemove(image.Image, /*forbidResurrection*/ true);
                            return PullImage(image, std::move(authConfig), pullPolicy);
                        }
                        YT_LOG_DEBUG("Docker image found in cache (Image: %v, LastPullTime: %v, LastUsageTime: %v)",
                            entry->Image(),
                            entry->GetLastPullTime(),
                            entry->GetLastUsageTime());
                        TouchImage(entry);
                        return MakeFuture<TCriImageCacheEntryPtr>(std::move(entry));
                    }));
            }

            YT_LOG_DEBUG("Waiting for in-flight docker image pull (Image: %v)", image);
            return imageFuture
                .Apply(BIND([
                    this,
                    this_ = MakeStrong(this),
                    image = std::move(image),
                    authConfig = std::move(authConfig),
                    pullPolicy
                ] (const TErrorOr<TCriImageCacheEntryPtr>& imageOrError) mutable {
                    if (imageOrError.IsOK()) {
                        return MakeFuture<TCriImageCacheEntryPtr>(std::move(imageOrError.Value()));
                    }
                    YT_LOG_DEBUG("Retry docker image pull (Image: %v)", image);
                    return PullImage(image, std::move(authConfig), pullPolicy);
                }));
        }

        return PruneImages()
            .Apply(BIND([=, this, this_ = MakeStrong(this)] () {
                return Executor_->GetImageStatus(image);
            }))
            .Apply(BIND([=, this, this_ = MakeStrong(this)] (const TCriImageApi::TRspImageStatusPtr& imageStatus) {
                if (imageStatus->has_image() && pullPolicy != EImagePullPolicy::Always) {
                    YT_LOG_DEBUG("Docker image already pulled into cache (Image: %v, ImageId: %v)",
                        image,
                        imageStatus->image().id());
                    return MakeFuture<TCriImageApi::TRspImageStatusPtr>(imageStatus);
                }
                YT_LOG_DEBUG("Docker image pull started (Image: %v, Authenticated: %v)",
                    image,
                    bool(authConfig));
                return Executor_->PullImage(image, authConfig)
                    .Apply(BIND([=, this, this_ = MakeStrong(this)] (const TCriImageApi::TRspPullImagePtr& result) {
                        TCriImageDescriptor cachedImage {
                            .Image = image.Image,
                            .Id = result->image_ref(),
                        };
                        return Executor_->GetImageStatus(cachedImage);
                    }));
            }))
            .Apply(BIND([=, this, this_ = MakeStrong(this), cookie = std::move(cookie)] (
                        const TErrorOr<TCriImageApi::TRspImageStatusPtr>& imageStatusOrError) mutable
            {
                if (!imageStatusOrError.IsOK()) {
                    YT_LOG_DEBUG(imageStatusOrError,
                        "Docker image pull failed (Image: %v)",
                        image);
                    cookie.Cancel(imageStatusOrError);
                    imageStatusOrError.ThrowOnError();
                }
                auto& imageStatus = imageStatusOrError.Value();
                if (!imageStatus->has_image()) {
                    THROW_ERROR_EXCEPTION("Docker image is not found in cache after pull");
                }
                auto& protoImage = imageStatus->image();
                TCriImageDescriptor cachedImage {
                    .Image = image.Image,
                    .Id = protoImage.id(),
                };
                YT_LOG_DEBUG("Docker image pull finished (Image: %v)", cachedImage);
                auto imageFuture = cookie.GetValue();
                DoInsertImage(cookie, protoImage, /*pullTime*/ TInstant::Now());
                return imageFuture;
            }));
    }

private:
    bool IsManagedImage(const TCriImageDescriptor& image) const
    {
        for (const auto& prefix : Config_->UnmanagedPrefixes) {
            if (image.Image.StartsWith(prefix)) {
                return false;
            }
        }
        if (Config_->ManagedPrefixes.empty()) {
            return true;
        }
        for (const auto& prefix : Config_->ManagedPrefixes) {
            if (image.Image.StartsWith(prefix)) {
                return true;
            }
        }
        return false;
    }

    void DoInsertImage(TInsertCookie& cookie, const NCri::NProto::Image& protoImage, TInstant pullTime = TInstant::Zero())
    {
        TCriImageDescriptor cachedImage{
            .Image = cookie.GetKey(),
            .Id = protoImage.id(),
        };
        i64 imageSize = protoImage.size();
        bool pinned = false;

        for (const auto& image : Config_->PinnedImages) {
            if (image == cachedImage.Image) {
                pinned = true;
                break;
            }
        }

        if (cachedImage.Image == cachedImage.Id) {
            auto imageEntry = New<TCriImageCacheEntry>(cachedImage, imageSize);
            cookie.EndInsert(std::move(imageEntry));
        } else {
            cookie.UpdateWeight(0);
            auto imageCookie = BeginInsert(cachedImage.Id, imageSize);
            if (imageCookie.IsActive()) {
                auto imageEntry = New<TCriImageCacheEntry>(cachedImage, imageSize);
                imageEntry->SetLastPullTime(pullTime);
                imageCookie.EndInsert(std::move(imageEntry));
            }
            auto imageFuture = imageCookie.GetValue();
            imageFuture.Subscribe(BIND([=, cookie = std::move(cookie)] (
                        const TErrorOr<TCriImageCacheEntryPtr>& entryOrError) mutable
            {
                if (entryOrError.IsOK()) {
                    auto& imageEntry = entryOrError.Value();
                    auto aliasEntry = New<TCriImageCacheEntry>(cachedImage, imageEntry);
                    aliasEntry->SetPinned(pinned);
                    aliasEntry->SetLastPullTime(pullTime);
                    cookie.EndInsert(std::move(aliasEntry));
                } else {
                    cookie.Cancel(entryOrError);
                }
            }));
        }
    }

    TFuture<void> DoRemoveImage(TCriImageCacheEntryPtr entry)
    {
        YT_VERIFY(!entry->IsAlias());

        CacheSize_ -= entry->GetImageSize();

        // FIXME(khlebnikov): Remove sequence is slightly racy, should be ok for real life cases.
        if (entry->HasAliases() || Find(entry->GetKey())) {
            YT_LOG_DEBUG("Retain docker image (Image: %v, ImageSize: %v, LastPullTime: %v, LastUsageTime: %v)",
                entry->Image(),
                entry->GetImageSize(),
                entry->GetLastPullTime(),
                entry->GetLastUsageTime());
            auto cookie = BeginInsert(entry->GetKey());
            if (cookie.IsActive()) {
                cookie.EndInsert(std::move(entry));
            }
            return VoidFuture;
        }

        YT_LOG_DEBUG("Removing docker image (Image: %v, ImageSize: %v, LastPullTime: %v, LastUsageTime: %v)",
            entry->Image(),
            entry->GetImageSize(),
            entry->GetLastPullTime(),
            entry->GetLastUsageTime());

        TryRemoveValue(entry, /*forbidResurrection*/ true);

        return Executor_->RemoveImage(entry->Image())
            .Apply(BIND([entry = std::move(entry)] (const TError& error) {
                if (error.IsOK()) {
                    YT_LOG_DEBUG("Docker image removed (Image: %v)", entry->Image());
                } else {
                    YT_LOG_WARNING(error, "Cannot remove docker image (Image: %v)", entry->Image());
                }
                return error;
            }));
    }

    TFuture<void> PruneImages()
    {
        std::vector<TCriImageCacheEntryPtr> images;
        {
            auto guard = Guard(SpinLock_);
            images.swap(DeathRow_);
        }
        if (images.empty()) {
            return VoidFuture;
        }
        YT_LOG_DEBUG("Prune docker image cache (CacheSize: %v)", CacheSize_.load());
        std::vector<TCallback<TFuture<void>()>> callbacks;
        for (auto& image : images) {
            callbacks.push_back(BIND(&TCriImageCache::DoRemoveImage, MakeStrong(this), Passed(std::move(image))));
        }
        // FIXME(khlebnikov): Maybe retry here, DoRemoveImage might resurrect images and remove others.
        return CancelableRunWithBoundedConcurrency(std::move(callbacks), 1)
            .AsVoid()
            .Apply(BIND([this, this_ = MakeStrong(this)] {
                YT_LOG_DEBUG("Docker image cache pruned (CacheSize: %v)", CacheSize_.load());
            }));
    }

protected:
    i64 GetWeight(const TCriImageCacheEntryPtr& value) const override
    {
        if (value->IsAlias()) {
            return 0;
        }
        return value->GetImageSize() * Config_->ImageCompressionRatioEstimation;
    }

    void OnAdded(const TValuePtr& entry) override
    {
        if (entry->IsAlias()) {
            YT_LOG_DEBUG("Docker image alias is added into cache (Image: %v)",
                entry->Image());
        } else {
            YT_LOG_DEBUG("Docker image entry is added into cache (Image: %v, ImageSize: %v)",
                entry->Image(),
                entry->GetImageSize());
            CacheSize_ += entry->GetImageSize();
        }
    }

    void OnRemoved(const TValuePtr& entry) override
    {
        if (entry->IsAlias()) {
            YT_LOG_DEBUG("Docker image alias is removed from cache (Image: %v, LastPullTime: %v, LastUsageTime: %v)",
                entry->Image(),
                entry->GetLastPullTime(),
                entry->GetLastUsageTime());
        } else {
            YT_LOG_DEBUG("Docker image entry is removed from cache (Image: %v, ImageSize: %v, LastPullTime: %v, LastUsageTime: %v)",
                entry->Image(),
                entry->GetImageSize(),
                entry->GetLastPullTime(),
                entry->GetLastUsageTime());
            auto guard = Guard(SpinLock_);
            DeathRow_.push_back(entry);
        }
    }

private:
    const TCriImageCacheConfigPtr Config_;
    const ICriExecutorPtr Executor_;
    std::atomic<i64> CacheSize_ = 0;
    std::vector<TCriImageCacheEntryPtr> DeathRow_;
    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, SpinLock_);
};

////////////////////////////////////////////////////////////////////////////////

ICriImageCachePtr CreateCriImageCache(
    TCriImageCacheConfigPtr config,
    ICriExecutorPtr executor)
{
    // Image and aliases must be in same shard to make drain work reliably.
    YT_VERIFY(config->ShardCount == 1);

    return New<TCriImageCache>(
        std::move(config),
        std::move(executor));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NContainers::NCri
