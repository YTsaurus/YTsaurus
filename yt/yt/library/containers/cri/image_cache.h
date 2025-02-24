#include "public.h"
#include "cri_executor.h"

#include <yt/yt/core/misc/async_slru_cache.h>

namespace NYT::NContainers::NCri {

////////////////////////////////////////////////////////////////////////////////

class TCriImageCacheEntry
    : public TAsyncCacheValueBase<TString, TCriImageCacheEntry>
    , private TIntrusiveListItem<TCriImageCacheEntry>
{
public:
    TCriImageCacheEntry(TCriImageDescriptor image, i64 imageSize);
    TCriImageCacheEntry(TCriImageDescriptor imageAlias, TCriImageCacheEntryPtr entry);
    ~TCriImageCacheEntry();

    bool IsAlias() const;
    bool HasAliases() const;

    DEFINE_BYREF_RO_PROPERTY_NO_INIT(TCriImageDescriptor, Image);
    DEFINE_BYVAL_RO_PROPERTY_NO_INIT(i64, ImageSize);
    DEFINE_BYREF_RO_PROPERTY_NO_INIT(TCriImageCacheEntryPtr, Parent);

    DEFINE_BYVAL_RW_PROPERTY(TInstant, LastPullTime);
    DEFINE_BYVAL_RW_PROPERTY(TInstant, LastUsageTime);
    DEFINE_BYVAL_RW_PROPERTY(bool, Pinned, false);

private:
    TIntrusiveList<TCriImageCacheEntry> Aliases_;
    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, SpinLock_);
};

DEFINE_REFCOUNTED_TYPE(TCriImageCacheEntry)

////////////////////////////////////////////////////////////////////////////////

enum class EImagePullPolicy
{
    Default,
    Never,
    Missing,
    Always,
};

////////////////////////////////////////////////////////////////////////////////

struct ICriImageCache
    : public virtual TRefCounted
{
    virtual TFuture<void> Initialize() = 0;

    virtual std::vector<TCriImageCacheEntryPtr> ListImages() = 0;

    virtual TFuture<TCriImageCacheEntryPtr> LookupImage(const TCriImageDescriptor& image) = 0;

    virtual TFuture<TCriImageCacheEntryPtr> PullImage(
        const TCriImageDescriptor& image,
        TCriAuthConfigPtr authConfig = nullptr,
        EImagePullPolicy pullPolicy = EImagePullPolicy::Default) = 0;
};

DEFINE_REFCOUNTED_TYPE(ICriImageCache)

////////////////////////////////////////////////////////////////////////////////

ICriImageCachePtr CreateCriImageCache(
    TCriImageCacheConfigPtr config,
    ICriExecutorPtr executor);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NContainers::NCri
