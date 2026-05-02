/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#pragma once

/*
 * Internal header shared between `module.c` (the library code) and
 * `module_main.c` (the standalone-module entry points). Defines the
 * command-registration framework used by `RedisModule_OnLoad`'s FT.*
 * tables and declares the helpers it calls back into the library for.
 *
 * `module_main.c` is excluded from the static archive when RediSearch
 * is built with -DREDISEARCH_BUILD_AS_LIBRARY=ON, so embedders can
 * supply their own `RedisModule_OnLoad`.
 */

#include "redismodule.h"

#ifdef __cplusplus
extern "C" {
#endif

/* Command registration framework -- types & macros */

typedef struct {
  int firstkey, lastkey, keystep;
} CommandKeys;

typedef enum {
  NONE,
  SET_COMMAND_INFO,
  SUBSCRIBE_SUBCOMMANDS
} SelectedCallbackType;

typedef int (*SetCommandInfo)(RedisModuleCommand *);
typedef int (*SubscribeSubCommands)(RedisModuleCtx *, RedisModuleCommand *);

typedef union {
  void *ptr;
  SetCommandInfo setCommandInfo;
  SubscribeSubCommands subscribeSubCommands;
} MutuallyExclusiveCommandCallbacks;

typedef struct {
  const char *name;
  const char *flags;
  // if false, the command will not be registered as a module command
  bool shouldRegister;
  const char *aclCategories;
  RedisModuleCmdFunc handler;
  MutuallyExclusiveCommandCallbacks callback;
  SelectedCallbackType selectedCallbackType;
  CommandKeys position;
  // if true, the command will be registered as an internal command
  bool internal;
} SearchCommand;

#define DEFINE_COMMAND_KEYS(firstkey_, lastkey_, keystep_) \
  (CommandKeys){.firstkey = firstkey_, .lastkey = lastkey_, .keystep = keystep_}

#define DEFINE_COMMAND(name_, func_, flags_, callback_, callback_type_, acl_, register_condition_, keys_, internal_) \
  { .name = name_, .flags = flags_, .shouldRegister = register_condition_,                                \
    .aclCategories = acl_, .handler = func_, .callback.ptr = callback_,                                   \
    .selectedCallbackType = callback_type_,                                                               \
    .position = keys_, .internal = internal_ }

/* Logs the result of an init step and propagates REDISMODULE_ERR.
 * Assumes a `RedisModuleCtx *ctx` is in scope. */
#define RM_TRY_F(f, ...)                                                       \
  if (f(__VA_ARGS__) == REDISMODULE_ERR) {                                     \
    RedisModule_Log(ctx, "warning", "Could not run " #f "(" #__VA_ARGS__ ")"); \
    return REDISMODULE_ERR;                                                    \
  } else {                                                                     \
    RedisModule_Log(ctx, "verbose", "Successfully executed " #f);              \
  }

/* Distributed-search thread pool id. Initialized by `RedisModule_OnLoad`,
 * read by the coord/dist command paths. */
extern int DIST_THREADPOOL;

/* Populates the global redisVersion / rlecVersion structs by querying
 * the running server's `INFO server` reply. */
void GetRedisVersion(RedisModuleCtx *ctx);

/* Returns `f` if the running coord/cluster shape supports it, otherwise
 * returns the disabled-command stub. */
RedisModuleCmdFunc SafeCmd(RedisModuleCmdFunc f);

/* Registers each entry in `commands` as a Redis module command. */
int CreateSearchCommands(RedisModuleCtx *ctx, const SearchCommand *commands, size_t count);

/* Sub-command registrars for FT.CONFIG and FT.CURSOR. */
int RegisterCoordConfigSubCommands(RedisModuleCtx *ctx, RedisModuleCommand *configCommand);
int RegisterCoordCursorCommands(RedisModuleCtx *ctx, RedisModuleCommand *cursorCommand);

#ifdef __cplusplus
}
#endif
