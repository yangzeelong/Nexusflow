#include <nexusflow/Any.hpp>

#include <gtest/gtest.h>

#include <string>
#include <utility>

namespace nexusflow {
namespace {

TEST(AnyTest, DefaultConstructedIsEmpty) {
    Any any;
    EXPECT_FALSE(any.hasValue<int>());
    EXPECT_EQ(any.getType(), typeid(void));
    EXPECT_EQ(any.get<int>(), nullptr);
    EXPECT_EQ(any.get<const int>(), nullptr);
}

TEST(AnyTest, StoresAndReadsAnInt) {
    Any any(42);
    ASSERT_TRUE(any.hasValue<int>());
    ASSERT_NE(any.get<int>(), nullptr);
    EXPECT_EQ(*any.get<int>(), 42);
    // cv-qualifiers are ignored when looking the value up.
    EXPECT_EQ(any.get<const int>(), any.get<int>());
    EXPECT_FALSE(any.hasValue<double>());
    EXPECT_EQ(any.get<double>(), nullptr);
}

TEST(AnyTest, CopiesAreDeepCopies) {
    Any original(std::string("hello"));
    Any copy(original);
    ASSERT_NE(copy.get<std::string>(), nullptr);
    EXPECT_EQ(*copy.get<std::string>(), "hello");

    *copy.get<std::string>() = "world";
    EXPECT_EQ(*original.get<std::string>(), "hello");
    EXPECT_EQ(*copy.get<std::string>(), "world");
}

TEST(AnyTest, MoveTransfersTheValue) {
    Any source(std::string("moved"));
    Any destination(std::move(source));
    ASSERT_NE(destination.get<std::string>(), nullptr);
    EXPECT_EQ(*destination.get<std::string>(), "moved");
    EXPECT_FALSE(source.hasValue<std::string>());
}

TEST(AnyTest, ConstAnyAllowsReadOnlyLookup) {
    const Any any(std::string("const"));
    const std::string* value = any.get<std::string>();
    ASSERT_NE(value, nullptr);
    EXPECT_EQ(*value, "const");
}

} // namespace
} // namespace nexusflow
