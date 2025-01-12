#pragma once

#include "public.h"

#include <yt/yt/core/ytree/attributes.h>

#include <library/cpp/yt/compact_containers/compact_vector.h>

#include <util/generic/algorithm.h>

namespace NYT::NYTree {

////////////////////////////////////////////////////////////////////////////////

//! Overlays attribute dictionaries on top of each other. This essentially gives
//! an appearance of merged dictionaries without actually merging them.
/*!
 *  Ownership depends on the template argument.
 *
 *  Underlying dictionaries may be null. For the purposes of reading, a null
 *  dictionary is equivalent to an empty one.
 *
 *  The class is const-correct in the sense that T may dereference to a const
 *  dictionary. Calling SetYson() or Remove() will cause runtime error in this
 *  case (compile-time diagnostics can't be achieved because of
 *  IAttributeDictionary's extension methods).
 *
 *  In a non-const overlay, removing an attribute is always supported and
 *  removes it from all underlying dictionaries. Setting an attribute is only
 *  supported iff at least one dictionary is not null. This sets the attribute
 *  in the topmost underlying dictionary and removes it from everywhere else.
 */
template <class T>
class TOverlaidAttributeDictionary
    : public IAttributeDictionary
{
public:
    // Parameters go from topmost to bottommost (i.e. highest to lowest priority).
    template <class... Args>
    explicit TOverlaidAttributeDictionary(Args&&... underlyingDicts);

    template <class U, class... Args>
    void PushBottom(U&& topmostUnderlyingDict, Args&&... underlyingDicts);

    template <class U>
    void PushBottom(U&& underlyingDict);

    std::vector<TString> ListKeys() const override;
    std::vector<TKeyValuePair> ListPairs() const override;
    NYson::TYsonString FindYson(TStringBuf key) const override;
    void SetYson(const TString& key, const NYson::TYsonString& value) override;
    bool Remove(const TString& key) override;

private:
    void SetYson(IAttributeDictionary& dict, const TString& key, const NYson::TYsonString& value);
    void SetYson(const IAttributeDictionary& dict, const TString& key, const NYson::TYsonString& value);

    bool Remove(IAttributeDictionary& dict, const TString& key);
    bool Remove(const IAttributeDictionary& dict, const TString& key);

    // From top to bottom (the earlier the dictionary, the higher its priority).
    TCompactVector<T, 2> UnderlyingDictionaries_;
};

////////////////////////////////////////////////////////////////////////////////

// Type deduction helper. All arguments are expected to be of same type.
template <class T, class... Args>
TIntrusivePtr<TOverlaidAttributeDictionary<typename std::decay<T>::type>> OverlayAttributeDictionaries(
    T&& topmostUnderlyingDict,
    Args&&... underlyingDicts);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYTree

#define OVERLAID_ATTRIBUTE_DICTIONARIES_INL_H_
#include "overlaid_attribute_dictionaries-inl.h"
#undef OVERLAID_ATTRIBUTE_DICTIONARIES_INL_H_

