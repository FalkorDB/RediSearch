/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#include "gtest/gtest.h"

#include "src/info/index_error.h"
#include "redismock/internal.h"

#include <thread>
#include <vector>

class IndexErrorTest : public ::testing::Test {};

TEST_F(IndexErrorTest, testBasic) {
  IndexError error;
  error = IndexError_Init();
  const char* expected = "secret";
  RedisModuleString *key = RedisModule_CreateString(NULL, expected, 6);
  IndexError_AddError(&error, "error", "error1", key);
  ASSERT_STREQ(error.last_error_with_user_data, "error1");
  ASSERT_STREQ(error.last_error_without_user_data, "error");
  RedisModuleString *lastErrorKey = IndexError_LastErrorKey(&error);
  ASSERT_EQ(key, lastErrorKey);
  const char* text = RedisModule_StringPtrLen(lastErrorKey, NULL);
  ASSERT_STREQ(text, expected);
  RedisModule_FreeString(NULL, lastErrorKey);

  error.last_error_time = {0};
  lastErrorKey = IndexError_LastErrorKeyObfuscated(&error);
  text = RedisModule_StringPtrLen(lastErrorKey, NULL);
  ASSERT_NE(key, lastErrorKey);
  ASSERT_STREQ("Key@0", text);
  RedisModule_FreeString(NULL, lastErrorKey);
  IndexError_Clear(error);
  RedisModule_FreeString(NULL, key);
}

TEST_F(IndexErrorTest, testNAKeyIsBorrowed) {
  IndexError first = IndexError_Init();
  RedisModuleString *na = first.key;
  size_t refcount = na->refcount;
  IndexError second = IndexError_Init();
  ASSERT_EQ(na, second.key);
  ASSERT_EQ(refcount, na->refcount);
  IndexError_Clear(second);
  IndexError_Clear(first);
  ASSERT_EQ(refcount, na->refcount);
}

// An LLAPI host can free an index on another thread than the one that created it.
TEST_F(IndexErrorTest, testInitAndClearOnManyThreads) {
  IndexError error = IndexError_Init();
  RedisModuleString *na = error.key;
  size_t refcount = na->refcount;
  std::vector<std::thread> threads;
  for (int t = 0; t < 4; ++t) {
    threads.emplace_back([] {
      for (int i = 0; i < 100000; ++i) {
        IndexError_Clear(IndexError_Init());
      }
    });
  }
  for (auto &t : threads) {
    t.join();
  }
  ASSERT_EQ(refcount, na->refcount);
  ASSERT_STREQ("N/A", RedisModule_StringPtrLen(error.key, NULL));
  IndexError_Clear(error);
}
