#include "../../src/common/Optional.hpp"

#include <gtest/gtest.h>

#include <stdexcept>
#include <utility>

namespace {

// Tracks whether every constructed ThrowingCopy is destroyed exactly once, and
// throws from its copy constructor while the static budget is exhausted.
struct ThrowingCopy {
    static int copiesLeft;
    static int constructed;
    static int destroyed;

    ThrowingCopy() { ++constructed; }

    ThrowingCopy(const ThrowingCopy&) {
        if (copiesLeft <= 0) {
            throw std::runtime_error("ThrowingCopy: copy failed");
        }
        --copiesLeft;
        ++constructed;
    }

    ~ThrowingCopy() { ++destroyed; }

    static void ResetCounters(int copies) {
        copiesLeft = copies;
        constructed = 0;
        destroyed = 0;
    }
};

int ThrowingCopy::copiesLeft = 0;
int ThrowingCopy::constructed = 0;
int ThrowingCopy::destroyed = 0;

TEST(OptionalThrowingTest, ResetLeavesOptionalEmptyWhenConstructionThrows) {
    ThrowingCopy::ResetCounters(0);

    Optional<ThrowingCopy> optional;
    ASSERT_FALSE(optional.hasValue());

    EXPECT_THROW(optional.reset(ThrowingCopy()), std::runtime_error);
    EXPECT_FALSE(optional.hasValue());

    // An empty optional must not destroy the value that was never constructed.
    const int destroyedBefore = ThrowingCopy::destroyed;
    optional.reset();
    EXPECT_EQ(ThrowingCopy::destroyed, destroyedBefore);
}

TEST(OptionalThrowingTest, CopyAssignmentLeavesDestinationEmptyWhenCopyThrows) {
    ThrowingCopy::ResetCounters(10);

    Optional<ThrowingCopy> source;
    source.reset(ThrowingCopy());
    ASSERT_TRUE(source.hasValue());

    Optional<ThrowingCopy> destination;
    destination.reset(ThrowingCopy());
    ASSERT_TRUE(destination.hasValue());

    ThrowingCopy::copiesLeft = 0; // The next copy throws.
    EXPECT_THROW(destination = source, std::runtime_error);
    EXPECT_FALSE(destination.hasValue());
    EXPECT_TRUE(source.hasValue());
}

TEST(OptionalThrowingTest, CopyConstructionFailureKeepsSourceValid) {
    ThrowingCopy::ResetCounters(1);

    Optional<ThrowingCopy> source;
    source.reset(ThrowingCopy());
    ASSERT_TRUE(source.hasValue());

    ThrowingCopy::copiesLeft = 0; // The next copy throws.
    EXPECT_THROW(Optional<ThrowingCopy> copy(source), std::runtime_error);
    EXPECT_TRUE(source.hasValue());
}

} // namespace
