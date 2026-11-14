#include <nexusflow/Message.hpp>

#include <gtest/gtest.h>

#include <string>

using namespace nexusflow;

TEST(MessageMetaTest, ModuleTimingsAreCopyOnWrite) {
    MessageMeta meta;
    meta.SetModuleTimingMs("first", 1.5);

    // Copies share the timing map until one of them writes to it.
    MessageMeta copy = meta;
    copy.SetModuleTimingMs("second", 2.5);

    EXPECT_EQ(meta.GetModuleTimings().size(), 1u);
    EXPECT_EQ(meta.GetModuleTimings().count("second"), 0u);
    ASSERT_EQ(meta.GetModuleTimings().count("first"), 1u);
    EXPECT_DOUBLE_EQ(meta.GetModuleTimings().at("first"), 1.5);

    EXPECT_EQ(copy.GetModuleTimings().size(), 2u);
    EXPECT_DOUBLE_EQ(copy.GetModuleTimings().at("second"), 2.5);
}

TEST(MessageMetaTest, EmptyTimingsAreReadableWithoutAllocating) {
    MessageMeta meta;
    EXPECT_TRUE(meta.GetModuleTimings().empty());
    EXPECT_EQ(meta.moduleTimingMs, nullptr);
}

TEST(MessageMetaTest, CopiedMessageAddsTimingsWithoutTouchingTheOriginal) {
    Message original = MakeMessage(std::string("payload"));
    Message copy = original;

    copy.MetaData().SetModuleTimingMs("copy_stage", 3.0);

    EXPECT_TRUE(original.MetaData().GetModuleTimings().empty());
    ASSERT_EQ(copy.MetaData().GetModuleTimings().count("copy_stage"), 1u);
    EXPECT_DOUBLE_EQ(copy.MetaData().GetModuleTimings().at("copy_stage"), 3.0);
}
