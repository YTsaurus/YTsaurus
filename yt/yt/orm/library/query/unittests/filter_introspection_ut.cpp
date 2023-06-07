#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/orm/library/query/filter_introspection.h>

#include <yt/yt/library/query/base/query_preparer.h>

#include <yt/yt/core/misc/error.h>

namespace NYT::NOrm::NQuery::NTests {
namespace {

using namespace NYT::NQueryClient;
using namespace NYT::NQueryClient::NAst;

////////////////////////////////////////////////////////////////////////////////

TEST(FilterIntrospection, DefinedAttributeValue)
{
    // Invalid attribute path.
    EXPECT_THROW(IntrospectFilterForDefinedAttributeValue("1=1", "//"), TErrorException);
    EXPECT_THROW(IntrospectFilterForDefinedAttributeValue("1=1", "/abr/"), TErrorException);
    EXPECT_THROW(IntrospectFilterForDefinedAttributeValue("1=1", ""), TErrorException);

    // Invalid filter.
    EXPECT_THROW(IntrospectFilterForDefinedAttributeValue("=", "/meta/id"), TErrorException);
    EXPECT_THROW(IntrospectFilterForDefinedAttributeValue("1=", "/meta/id"), TErrorException);
    EXPECT_THROW(IntrospectFilterForDefinedAttributeValue("(a,b,c)", "/meta/id"), TErrorException);

    // Other types.
    EXPECT_EQ(IntrospectFilterForDefinedAttributeValue("[/meta/id]=1", "/meta/id").TryMoveAs<i64>(), 1);
    EXPECT_EQ(IntrospectFilterForDefinedAttributeValue("[/meta/id]=1u", "/meta/id").TryMoveAs<ui64>(), 1);
    EXPECT_EQ(IntrospectFilterForDefinedAttributeValue("[/meta/id]=1u", "/meta/id").TryMoveAs<i64>(), std::nullopt);
    EXPECT_EQ(IntrospectFilterForDefinedAttributeValue("[/meta/id]=null", "/meta/id").TryMoveAs<TNullLiteralValue>(), TNullLiteralValue{});
    EXPECT_EQ(IntrospectFilterForDefinedAttributeValue("[/meta/id]=3.5", "/meta/id").TryMoveAs<double>(), 3.5);
    EXPECT_EQ(IntrospectFilterForDefinedAttributeValue("[/meta/id]=%false", "/meta/id").TryMoveAs<bool>(), std::make_optional(false));

    // Incorrect type.
    EXPECT_EQ(IntrospectFilterForDefinedAttributeValue("[/meta/id]=(1,2)", "/meta/id").Value, std::nullopt);
    EXPECT_EQ(IntrospectFilterForDefinedAttributeValue("[/meta/id]=(1+2+3,2)", "/meta/id").Value, std::nullopt);

    // Equality.
    EXPECT_EQ(IntrospectFilterForDefinedAttributeValue("[/meta/id]=\"aba\"", "/meta/id").TryMoveAs<TString>(), "aba");

    // And.
    EXPECT_EQ(IntrospectFilterForDefinedAttributeValue("[/meta/id]=\"aba\" and [/meta/creation_time] > 100", "/meta/id").TryMoveAs<TString>(), "aba");
    EXPECT_EQ(IntrospectFilterForDefinedAttributeValue("[/meta/id]=\"aba\" and not ([/meta/id]=\"aba\")", "/meta/id").TryMoveAs<TString>(), "aba");
    EXPECT_EQ(IntrospectFilterForDefinedAttributeValue("[/meta/id]=\"aba\" and [/meta/id]=\"cde\"", "/meta/id").TryMoveAs<TString>(), "aba");
    EXPECT_EQ(IntrospectFilterForDefinedAttributeValue("[/meta/id]=\"aba\" and 123", "/meta/id").TryMoveAs<TString>(), "aba");

    // Or.
    EXPECT_EQ(IntrospectFilterForDefinedAttributeValue("[/meta/id]=\"aba\" or [/meta/id]=\"cde\"", "/meta/id").Value, std::nullopt);
    EXPECT_EQ(IntrospectFilterForDefinedAttributeValue("[/meta/id]=\"aba\" or [/meta/id]=\"aba\"", "/meta/id").TryMoveAs<TString>(), "aba");
    EXPECT_EQ(IntrospectFilterForDefinedAttributeValue("[/meta/id]=\"aba\" or 123", "/meta/id").Value, std::nullopt);

    // Too complex for now.
    EXPECT_EQ(IntrospectFilterForDefinedAttributeValue("[/meta/id]=\"a\"+\"b\"", "/meta/id").Value, std::nullopt);
    EXPECT_EQ(IntrospectFilterForDefinedAttributeValue("([/meta/id],[/meta/creation_time])=(\"aba\",1020)", "/meta/id").Value, std::nullopt);

    // Other.
    EXPECT_EQ(IntrospectFilterForDefinedAttributeValue("not ([/meta/id]=\"aba\")", "/meta/id").Value, std::nullopt);
    EXPECT_EQ(IntrospectFilterForDefinedAttributeValue("10+20+30", "/meta/id").Value, std::nullopt);
    EXPECT_EQ(IntrospectFilterForDefinedAttributeValue("abracadabra", "/meta/id").Value, std::nullopt);
    EXPECT_EQ(IntrospectFilterForDefinedAttributeValue("", "/meta/id").Value, std::nullopt);
}

////////////////////////////////////////////////////////////////////////////////

bool RunIntrospectFilterForDefinedReference(
    const TString& expressionString,
    const NYPath::TYPath& referenceName,
    const std::optional<TString>& tableName = std::nullopt,
    bool allowValueRange = true)
{
    auto parsedQuery = ParseSource(expressionString, NQueryClient::EParseMode::Expression);
    auto expression = std::get<NAst::TExpressionPtr>(parsedQuery->AstHead.Ast);

    return IntrospectFilterForDefinedReference(expression, referenceName, tableName, allowValueRange);
}

TEST(FilterIntrospection, DefinedReference)
{
    // Defined simple.
    EXPECT_TRUE(RunIntrospectFilterForDefinedReference(
        "[/spec/year]=1",
        "/spec/year"));
    EXPECT_TRUE(RunIntrospectFilterForDefinedReference(
        "i.[/spec/year]=1",
        "/spec/year",
        "i"));
    EXPECT_TRUE(RunIntrospectFilterForDefinedReference(
        "[/spec/year]<2",
        "/spec/year"));
    EXPECT_TRUE(RunIntrospectFilterForDefinedReference(
        "[/spec/name]=\"text\"",
         "/spec/name"));
    EXPECT_TRUE(RunIntrospectFilterForDefinedReference(
        "[/spec/year] in (1, 2, 3)",
         "/spec/year"));

    // Defined AND.
    EXPECT_TRUE(RunIntrospectFilterForDefinedReference(
        "[/spec/name]=\"text\" AND [/spec/year]=1",
         "/spec/year"));
    EXPECT_TRUE(RunIntrospectFilterForDefinedReference(
        "[/spec/name]=\"text\" AND ([/spec/author]=\"Tom\" AND [/spec/year]>0)",
         "/spec/year"));
    EXPECT_TRUE(RunIntrospectFilterForDefinedReference(
        "[/spec/year]=1 AND true",
        "/spec/year"));

    // Defined OR.
    EXPECT_TRUE(RunIntrospectFilterForDefinedReference(
        "[/spec/year]>1990 OR [/spec/year]<=2000",
         "/spec/year"));
    EXPECT_TRUE(RunIntrospectFilterForDefinedReference(
        "[/spec/year]>1990 OR ([/spec/publisher]=\"O'Relly\" AND [/spec/year]<=2000)",
         "/spec/year"));

    // Not defined simple.
    EXPECT_FALSE(RunIntrospectFilterForDefinedReference(
        "[/spec/year]=1",
        "/spec/name"));
    EXPECT_FALSE(RunIntrospectFilterForDefinedReference(
        "True",
        "/spec/name"));
    EXPECT_FALSE(RunIntrospectFilterForDefinedReference(
        "[/spec/year]=1",
        ""));
    EXPECT_FALSE(RunIntrospectFilterForDefinedReference(
        "[/spec/year]=1",
        "spec.year"));
    EXPECT_FALSE(RunIntrospectFilterForDefinedReference(
        "[/spec/year]",
        "/spec/year"));
    EXPECT_FALSE(RunIntrospectFilterForDefinedReference(
        "[/spec/year]<2",
        "/spec/year",
        /*tableName*/ std::nullopt,
        /*allowValueRange*/ false));

    // Not defined yet.
    EXPECT_FALSE(RunIntrospectFilterForDefinedReference(
        "list_contains([/spec/genres], \"fantasy\")",
        "/spec/genres"));
     EXPECT_FALSE(RunIntrospectFilterForDefinedReference(
        "[/spec/year]>1990 OR false",
         "/spec/year"));

    // Not defined AND.
    EXPECT_FALSE(RunIntrospectFilterForDefinedReference(
        "[/spec/name]=\"text\" AND [/spec/year]=1",
         "/spec/author"));

    // Not defined OR.
    EXPECT_FALSE(RunIntrospectFilterForDefinedReference(
        "[/spec/year]>1990 OR [/spec/name]=\"text\"",
         "/spec/year"));
    EXPECT_FALSE(RunIntrospectFilterForDefinedReference(
        "[/spec/year]>1990 OR [/spec/name]=\"text\"",
         "/spec/genres"));
}

////////////////////////////////////////////////////////////////////////////////

TEST(FilterIntrospection, ExtractAllReferences)
{
    // Check constant node filters.
    for (const auto& nodeFilter : {
            "",
            "%true",
            "%false",
            "1 > 2",
            "(5 + 4) * 2",
            "is_substr(\"Intel\", \"Intel(R) Xeon(R) CPU E5-2660 0 @ 2.20GHz\")",
        })
    {
        EXPECT_EQ(ExtractFilterAttributeReferences(nodeFilter), THashSet<TString>());
    }

    // Check simple expressions.
    for (const auto& opString : {"=", "!=", ">", "<", "<=", ">="}) {
        EXPECT_EQ(
            ExtractFilterAttributeReferences(Format("[/spec/weight] %v 152", opString)),
            THashSet<TString>{"/spec/weight"});
    }

    // Check complex expression.
    EXPECT_EQ(
        ExtractFilterAttributeReferences(
            "[/labels/position] = 153 OR list_contains([/spec/supported_modes], \"CMP\") "
            "AND NOT ([/status/disabled] = %true OR is_substr(\"disabled\", "
            "[/status/state/raw]))"),
        THashSet<TString>({
            "/labels/position",
            "/spec/supported_modes",
            "/status/disabled",
            "/status/state/raw",
        }));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NOrm::NQuery::NTests
