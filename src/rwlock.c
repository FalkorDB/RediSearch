/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#include "rwlock.h"
#include "rmalloc.h"
#include "util/arr_rm_alloc.h"
#include "rmutil/rm_assert.h"

pthread_mutex_t rwLockMutex;
pthread_rwlock_t RWLock = PTHREAD_RWLOCK_INITIALIZER;
pthread_key_t _lockKey;

typedef enum lockType { lockType_None, lockType_Read, lockType_Write } lockType;

typedef struct rwlockThreadLocal {
  size_t locked;
  lockType type;
} rwlockThreadLocal;

rwlockThreadLocal** rwlocks;

// Thread-local destructor: frees the state of an exiting thread that took the lock.
static void RediSearch_FreeLockThreadData(void* data) {
  pthread_mutex_lock(&rwLockMutex);
  // NULL after RediSearch_LockDestory, which frees every entry.
  for (size_t i = 0; rwlocks && i < array_len(rwlocks); ++i) {
    if (rwlocks[i] == data) {
      if (rwlocks[i]->locked) {
        // The thread holds RWLock once, however deep its nesting.
        pthread_rwlock_unlock(&RWLock);
        RedisModule_Log(NULL, "warning", "A thread exited while it held the RediSearch global lock");
      }
      array_del_fast(rwlocks, i);
      rm_free(data);
      break;
    }
  }
  pthread_mutex_unlock(&rwLockMutex);
}

int RediSearch_LockInit(RedisModuleCtx* ctx) {
  rwlocks = array_new(rwlockThreadLocal*, 10);
  pthread_mutex_init(&rwLockMutex, NULL);
  int err = pthread_key_create(&_lockKey, RediSearch_FreeLockThreadData);
  if (err) {
    if (ctx) {
      RedisModule_Log(ctx, "warning", "could not initialize rwlock thread local");
    }
    return REDISMODULE_ERR;
  }
  return REDISMODULE_OK;
}

static rwlockThreadLocal* RediSearch_GetLockThreadData() {
  rwlockThreadLocal* rwData = pthread_getspecific(_lockKey);
  if (!rwData) {
    rwData = rm_malloc(sizeof(*rwData));
    rwData->locked = 0;
    rwData->type = lockType_None;
    pthread_setspecific(_lockKey, rwData);
    pthread_mutex_lock(&rwLockMutex);
    array_append(rwlocks, rwData);
    pthread_mutex_unlock(&rwLockMutex);
  }
  return rwData;
}

void RediSearch_LockRead() {
  rwlockThreadLocal* rwData = RediSearch_GetLockThreadData();
  RS_ASSERT(rwData->type != lockType_Write);
  if (rwData->locked == 0) {
    pthread_rwlock_rdlock(&RWLock);
    rwData->type = lockType_Read;
  }
  RS_ASSERT(rwData->type == lockType_Read);
  ++rwData->locked;
}

void RediSearch_LockWrite() {
  rwlockThreadLocal* rwData = RediSearch_GetLockThreadData();
  RS_ASSERT(rwData->type != lockType_Read);
  if (rwData->locked == 0) {
    pthread_rwlock_wrlock(&RWLock);
    rwData->type = lockType_Write;
  }
  RS_ASSERT(rwData->type == lockType_Write);
  ++rwData->locked;
}

void RediSearch_LockRelease() {
  rwlockThreadLocal* rwData = RediSearch_GetLockThreadData();
  RS_ASSERT(rwData->locked > 0);
  if ((--rwData->locked) == 0) {
    pthread_rwlock_unlock(&RWLock);
    rwData->type = lockType_None;
  }
}

void RediSearch_LockDestory() {
  // A destructor that runs after the module unloads would call unmapped code.
  pthread_key_delete(_lockKey);
  pthread_mutex_lock(&rwLockMutex);
  for (size_t i = 0; i < array_len(rwlocks); ++i) {
    rm_free(rwlocks[i]);
  }
  array_free(rwlocks);
  rwlocks = NULL;
  pthread_mutex_unlock(&rwLockMutex);
}
