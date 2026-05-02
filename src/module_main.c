/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

/*
 * Standalone-module entry points: `RedisModule_OnLoad`,
 * `RedisModule_OnUnload`, the FT.* command-table registrations, and the
 * helpers those entry points need. Excluded from the static archive
 * when -DREDISEARCH_BUILD_AS_LIBRARY=ON so embedders can supply their
 * own module entry point.
 */

#define REDISMODULE_MAIN

#include <stdio.h>
#include <string.h>

#include "redismodule.h"
#include "module.h"
#include "module_main.h"
#include "rmalloc.h"
#include "config.h"
#include "spec.h"
#include "search_disk.h"
#include "rmutil/rm_assert.h"
#include "rmutil/strings.h"

#include "asm_state_machine.h"
#include "slot_ranges.h"

#include "concurrent_ctx.h"
#include "module_init.h"

#include "coord/config.h"
#include "coord/rmr/rmr.h"
#include "coord/rmr/redis_cluster.h"

#include "util/dict/dict.h"

#include "hiredis/async.h"
#include "libuv/include/uv.h"

#define CEIL_DIV(a, b) ((a + b - 1) / b)

/* Command handlers and info setters used by the FT.* tables below. */
int ConfigCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);
int InfoCommandHandler(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);
int DistSearchCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);
int DistAggregateCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);
int ProfileCommandHandler(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);
int SpellCheckCommandHandler(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);
int DistHybridCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);
int FanoutCommandHandlerIndexless(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);
int FanoutCommandHandlerWithIndexAtFirstArg(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);
int FanoutCommandHandlerWithIndexAtSecondArg(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);
int SetClusterCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);
int RefreshClusterCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);
int ClusterInfoCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);
int MGetCommandHandler(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);
int TagValsCommandHandler(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);

int SetFtInfoInfo(RedisModuleCommand *cmd);
int SetFtSearchInfo(RedisModuleCommand *cmd);
int SetFtAggregateInfo(RedisModuleCommand *cmd);
int SetFtProfileInfo(RedisModuleCommand *cmd);
int SetFtSpellcheckInfo(RedisModuleCommand *cmd);
int SetFtHybridInfo(RedisModuleCommand *cmd);
int SetFtCreateInfo(RedisModuleCommand *cmd);
int SetFtAlterInfo(RedisModuleCommand *cmd);
int SetFtDropindexInfo(RedisModuleCommand *cmd);
int SetFtDictaddInfo(RedisModuleCommand *cmd);
int SetFtDictdelInfo(RedisModuleCommand *cmd);
int SetFtAliasaddInfo(RedisModuleCommand *cmd);
int SetFtAliasdelInfo(RedisModuleCommand *cmd);
int SetFtAliasupdateInfo(RedisModuleCommand *cmd);
int SetFtSynupdateInfo(RedisModuleCommand *cmd);
int SetFtTagvalsInfo(RedisModuleCommand *cmd);

extern size_t NumShards;
bool IsEnterpriseBuild(void);

/**
 * A wrapper function to override hiredis allocators with redis allocators.
 * It should be called after RedisModule_Init.
 */
void setHiredisAllocators(){
  hiredisAllocFuncs ha = {
    .mallocFn = rm_malloc,
    .callocFn = rm_calloc,
    .reallocFn = rm_realloc,
    .strdupFn = rm_strdup,
    .freeFn = rm_free,
  };

  hiredisSetAllocators(&ha);
}

void Coordinator_ShutdownEvent(RedisModuleCtx *ctx, RedisModuleEvent eid, uint64_t subevent, void *data) {
  RedisModule_Log(ctx, "notice", "%s", "Begin releasing RediSearch resources on shutdown");
  RediSearch_CleanupModule();
  RedisModule_Log(ctx, "notice", "%s", "End releasing RediSearch resources");
}

void Initialize_CoordKeyspaceNotifications(RedisModuleCtx *ctx) {
  // To be called after `Initialize_ServerEventNotifications` as callbacks are overridden.
  if (RedisModule_SubscribeToServerEvent && getenv("RS_GLOBAL_DTORS")) {
    // clear resources when the server exits
    // used only with sanitizer or valgrind
    RedisModule_Log(ctx, "notice", "%s", "Subscribe to clear resources on shutdown");
    RedisModule_SubscribeToServerEvent(ctx, RedisModuleEvent_Shutdown, Coordinator_ShutdownEvent);
  }
}

static bool checkClusterEnabled(RedisModuleCtx *ctx) {
  RedisModuleCallReply *rep = RedisModule_Call(ctx, "CONFIG", "cc", "GET", "cluster-enabled");
  RS_ASSERT_ALWAYS(rep && RedisModule_CallReplyType(rep) == REDISMODULE_REPLY_ARRAY &&
                     RedisModule_CallReplyLength(rep) == 2);
  size_t len;
  const char *isCluster = RedisModule_CallReplyStringPtr(RedisModule_CallReplyArrayElement(rep, 1), &len);
  bool isClusterEnabled = STR_EQCASE(isCluster, len, "yes");
  RedisModule_FreeCallReply(rep);
  return isClusterEnabled;
}

static int RediSearch_InitModuleConfig(RedisModuleCtx *ctx, RedisModuleString **argv, int argc, int isClusterEnabled) {
  // register the module configuration with redis, use loaded values from command line as defaults
  if (RegisterModuleConfig_Local(ctx) == REDISMODULE_ERR) {
    RedisModule_Log(ctx, "warning", "Error registering module configuration");
    return REDISMODULE_ERR;
  }
  if (isClusterEnabled || clusterConfig.type == ClusterType_RedisLabs) {
    // Register module configuration parameters for cluster
    RM_TRY_F(RegisterClusterModuleConfig, ctx);
  }

  // Load default values
  RM_TRY_F(RedisModule_LoadDefaultConfigs, ctx);

  char *err = NULL;
  // Read module configuration from module ARGS
  if (ReadConfig(argv, argc, &err) == REDISMODULE_ERR) {
    RedisModule_Log(ctx, "warning", "Invalid Configurations: %s", err);
    rm_free(err);
    return REDISMODULE_ERR;
  }
  // Apply configuration redis has loaded from the configuration file
  RM_TRY_F(RedisModule_LoadConfigs, ctx);
  return REDISMODULE_OK;
}

/* Perform basic configurations and init all threads and global structures */
static int initSearchCluster(RedisModuleCtx *ctx, RedisModuleString **argv, int argc, bool isClusterEnabled) {
  RedisModule_Log(ctx, "notice",
                  "Cluster configuration: AUTO partitions, type: %d, coordinator timeout: %dms",
                  clusterConfig.type, clusterConfig.timeoutMS);

  if (clusterConfig.type == ClusterType_RedisOSS) {
    if (isClusterEnabled) {
      // Init the topology updater cron loop.
      InitRedisTopologyUpdater(ctx);
    } else {
      // We are not in cluster mode. No need to init the topology updater cron loop.
      // Set the number of shards to 1 to indicate the topology is "set"
      NumShards = 1;
      // Setting all slots for the case where we send/test internal commands directly from client (potentially with _SLOTS_INFO)
      RedisModuleSlotRangeArray *all_slots = rm_malloc(SlotRangeArray_SizeOf(1));
      all_slots->num_ranges = 1;
      all_slots->ranges[0].start = 0;
      all_slots->ranges[0].end = 16383;
      ASM_StateMachine_SetLocalSlots(all_slots);
      rm_free(all_slots);
    }
  }

  size_t num_connections_per_shard;
  if (clusterConfig.connPerShard) {
    num_connections_per_shard = clusterConfig.connPerShard;
  } else {
    // default
    num_connections_per_shard = RSGlobalConfig.numWorkerThreads + 1;
  }

  size_t num_io_threads = clusterConfig.coordinatorIOThreads;
  size_t conn_pool_size = CEIL_DIV(num_connections_per_shard, num_io_threads);

  MR_Init(num_io_threads, conn_pool_size, clusterConfig.timeoutMS);
  MR_InitLocalNodeId();

  return REDISMODULE_OK;
}

int __attribute__((visibility("default")))
RedisModule_OnLoad(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {

  if (RedisModule_Init(ctx, REDISEARCH_MODULE_NAME, REDISEARCH_MODULE_VERSION,
                       REDISMODULE_APIVER_1) == REDISMODULE_ERR) {
    return REDISMODULE_ERR;
  }

  TracingRedisModule_Init(ctx);
  RustPanicHook_Init();

  setHiredisAllocators();
  uv_replace_allocator(rm_malloc, rm_realloc, rm_calloc, rm_free);

  if (!RSDummyContext) {
    RSDummyContext = RedisModule_GetDetachedThreadSafeContext(ctx);
  }

  // Chain the config into RediSearch's global config and set the default values
  clusterConfig = DEFAULT_CLUSTER_CONFIG;
  RSConfigOptions_AddConfigs(&RSGlobalConfigOptions, GetClusterConfigOptions());
  ClusterConfig_RegisterTriggers();

  // Register the module configuration parameters
  GetRedisVersion(ctx);

  // Check if we are actually in cluster mode
  const bool isClusterEnabled = checkClusterEnabled(ctx);

  legacySpecRules = dictCreate(&dictTypeHeapHiddenStrings, NULL);

  if (RediSearch_InitModuleConfig(ctx, argv, argc, isClusterEnabled) == REDISMODULE_ERR) {
    return REDISMODULE_ERR;
  }

  // Init RediSearch internal search
  if (RediSearch_InitModuleInternal(ctx) == REDISMODULE_ERR) {
    RedisModule_Log(ctx, "warning", "Could not init search library...");
    return REDISMODULE_ERR;
  }

  // Init the global cluster structs
  if (initSearchCluster(ctx, argv, argc, isClusterEnabled) == REDISMODULE_ERR) {
    RedisModule_Log(ctx, "warning", "Could not init MR search cluster");
    return REDISMODULE_ERR;
  }

  // Init the aggregation thread pool
  DIST_THREADPOOL = ConcurrentSearch_CreatePool(clusterConfig.coordinatorPoolSize);

  Initialize_CoordKeyspaceNotifications(ctx);

  if (RedisModule_ACLCheckKeyPrefixPermissions == NULL) {
    // Running against a Redis version that does not support module ACL protection
    RedisModule_Log(ctx, "warning", "Redis version does not support ACL API necessary for index protection");
  }

  // read commands
  // Commands that don't operate on Redis keys use (0, 0, 0).
  // The proxy gets key-spec from the RAMP file (pack/ramp-enterprise.yml).
  const CommandKeys noKeyArgs = DEFINE_COMMAND_KEYS(0, 0, 0);

  SearchCommand readCommands[] = {
    // read commands
    DEFINE_COMMAND("FT.INFO",       SafeCmd(InfoCommandHandler),       "readonly", SetFtInfoInfo,               SET_COMMAND_INFO,      "",     true, noKeyArgs, false),
    DEFINE_COMMAND("FT.SEARCH",     SafeCmd(DistSearchCommand),        "readonly", SetFtSearchInfo,             SET_COMMAND_INFO,      "read", true, noKeyArgs, false),
    DEFINE_COMMAND("FT.AGGREGATE",  SafeCmd(DistAggregateCommand),     "readonly", SetFtAggregateInfo,          SET_COMMAND_INFO,      "read", true, noKeyArgs, false),
    DEFINE_COMMAND("FT.PROFILE",    SafeCmd(ProfileCommandHandler),    "readonly", SetFtProfileInfo,            SET_COMMAND_INFO,      "read", true, noKeyArgs, false),
    DEFINE_COMMAND("FT.SPELLCHECK", SafeCmd(SpellCheckCommandHandler), "readonly", SetFtSpellcheckInfo,         SET_COMMAND_INFO,      "",     true, noKeyArgs, false),
    DEFINE_COMMAND("FT.HYBRID",     SafeCmd(DistHybridCommand),        "readonly", SetFtHybridInfo,             SET_COMMAND_INFO,      "read", true, noKeyArgs, false),
    DEFINE_COMMAND("FT.CURSOR",     NULL,                              "readonly", RegisterCoordCursorCommands, SUBSCRIBE_SUBCOMMANDS, "read", true, noKeyArgs, false),
  };
  if (CreateSearchCommands(ctx, readCommands, sizeof(readCommands) / sizeof(SearchCommand)) != REDISMODULE_OK) {
    return REDISMODULE_ERR;
  }

  // OSS commands (registered via proxy in Enterprise)
  if (!IsEnterpriseBuild()) {
    SearchCommand writeCommands[] = {
      DEFINE_COMMAND("FT.CREATE",         SafeCmd(FanoutCommandHandlerIndexless),                  "write deny-oom", SetFtCreateInfo,                SET_COMMAND_INFO,      "",                     true,                noKeyArgs, false),
      DEFINE_COMMAND("FT._CREATEIFNX",    SafeCmd(FanoutCommandHandlerIndexless),                  "write deny-oom", SetFtCreateInfo,                SET_COMMAND_INFO,      "",                     true,                noKeyArgs, false),
      DEFINE_COMMAND("FT.ALTER",          SafeCmd(FanoutCommandHandlerWithIndexAtFirstArg),        "write deny-oom", SetFtAlterInfo,                 SET_COMMAND_INFO,      "",                     true,                noKeyArgs, false),
      DEFINE_COMMAND("FT._ALTERIFNX",     SafeCmd(FanoutCommandHandlerWithIndexAtFirstArg),        "write deny-oom", SetFtAlterInfo,                 SET_COMMAND_INFO,      "",                     true,                noKeyArgs, false),
      DEFINE_COMMAND("FT.DROPINDEX",      SafeCmd(FanoutCommandHandlerWithIndexAtFirstArg),        "write",          SetFtDropindexInfo,             SET_COMMAND_INFO,      "write slow dangerous", true,                noKeyArgs, false),
      // TODO: Either make ALL replication commands internal (such that no need for ACL check), or add ACL check.true
      DEFINE_COMMAND("FT._DROPINDEXIFX",  SafeCmd(FanoutCommandHandlerIndexless),                  "write",          SetFtDropindexInfo,             SET_COMMAND_INFO,      "write slow dangerous", true,                noKeyArgs, false),
      DEFINE_COMMAND("FT.DICTADD",        SafeCmd(FanoutCommandHandlerIndexless),                  "write deny-oom", SetFtDictaddInfo,               SET_COMMAND_INFO,      "",                     true,                noKeyArgs, false),
      DEFINE_COMMAND("FT.DICTDEL",        SafeCmd(FanoutCommandHandlerIndexless),                  "write",          SetFtDictdelInfo,               SET_COMMAND_INFO,      "",                     true,                noKeyArgs, false),
      DEFINE_COMMAND("FT.ALIASADD",       SafeCmd(FanoutCommandHandlerWithIndexAtSecondArg),       "write deny-oom", SetFtAliasaddInfo,              SET_COMMAND_INFO,      "",                     true,                noKeyArgs, false),
      DEFINE_COMMAND("FT._ALIASADDIFNX",  SafeCmd(FanoutCommandHandlerIndexless),                  "write deny-oom", SetFtAliasaddInfo,              SET_COMMAND_INFO,      "",                     true,                noKeyArgs, false),
      DEFINE_COMMAND("FT.ALIASDEL",       SafeCmd(FanoutCommandHandlerIndexless),                  "write",          SetFtAliasdelInfo,              SET_COMMAND_INFO,      "",                     true,                noKeyArgs, false),
      DEFINE_COMMAND("FT._ALIASDELIFX",   SafeCmd(FanoutCommandHandlerIndexless),                  "write",          SetFtAliasdelInfo,              SET_COMMAND_INFO,      "",                     true,                noKeyArgs, false),
      DEFINE_COMMAND("FT.ALIASUPDATE",    SafeCmd(FanoutCommandHandlerWithIndexAtSecondArg),       "write deny-oom", SetFtAliasupdateInfo,           SET_COMMAND_INFO,      "",                     true,                noKeyArgs, false),
      DEFINE_COMMAND("FT.SYNUPDATE",      SafeCmd(FanoutCommandHandlerWithIndexAtFirstArg),        "write deny-oom", SetFtSynupdateInfo,             SET_COMMAND_INFO,      "",                     true,                noKeyArgs, false),
      DEFINE_COMMAND("FT.CONFIG",         NULL,                                                    "readonly",       RegisterCoordConfigSubCommands, SUBSCRIBE_SUBCOMMANDS, "admin",                !isClusterEnabled,   noKeyArgs, false),

      // // Deprecated OSS commands
      DEFINE_COMMAND("FT.DROP",      SafeCmd(FanoutCommandHandlerWithIndexAtFirstArg), "write", NULL, NONE,    "write slow dangerous", true,          noKeyArgs, false),
      DEFINE_COMMAND("FT._DROPIFX",  SafeCmd(FanoutCommandHandlerIndexless),           "write", NULL, NONE,    "write",                true,          noKeyArgs, false),
    };
    if (CreateSearchCommands(ctx, writeCommands, sizeof(writeCommands) / sizeof(SearchCommand)) != REDISMODULE_OK) {
      return REDISMODULE_ERR;
    }
  }

  // cluster set commands
  SearchCommand clusterSetCommands[] = {
    DEFINE_COMMAND(REDISEARCH_MODULE_NAME ".CLUSTERSET",     SafeCmd(SetClusterCommand),     IsEnterprise() ? "readonly allow-loading deny-script " CMD_PROXY_FILTERED : "readonly allow-loading deny-script", NULL, NONE, "", true, noKeyArgs, false),
    DEFINE_COMMAND(REDISEARCH_MODULE_NAME ".CLUSTERREFRESH", SafeCmd(RefreshClusterCommand), IsEnterprise() ? "readonly deny-script " CMD_PROXY_FILTERED               : "readonly deny-script",               NULL, NONE, "", true, noKeyArgs, false),
    DEFINE_COMMAND(REDISEARCH_MODULE_NAME ".CLUSTERINFO",    SafeCmd(ClusterInfoCommand),    IsEnterprise() ? "readonly allow-loading deny-script " CMD_PROXY_FILTERED : "readonly allow-loading deny-script", NULL, NONE, "", true, noKeyArgs, false),
  };
  if (CreateSearchCommands(ctx, clusterSetCommands, sizeof(clusterSetCommands) / sizeof(SearchCommand)) != REDISMODULE_OK) {
    return REDISMODULE_ERR;
  }
  // Deprecated commands. Grouped here for easy tracking
  SearchCommand deprecatedCommands[] = {
    DEFINE_COMMAND("FT.MGET",           SafeCmd(MGetCommandHandler),    "readonly", NULL,             NONE,             "read",           true, noKeyArgs, false),
    DEFINE_COMMAND("FT.TAGVALS",        SafeCmd(TagValsCommandHandler), "readonly", SetFtTagvalsInfo, SET_COMMAND_INFO, "read slow dangerous", true, noKeyArgs, false)
  };
  if (CreateSearchCommands(ctx, deprecatedCommands, sizeof(deprecatedCommands) / sizeof(SearchCommand)) != REDISMODULE_OK) {
    return REDISMODULE_ERR;
  }

  return REDISMODULE_OK;
}

int RedisModule_OnUnload(RedisModuleCtx *ctx) {
  if (config_ext_load) {
    RedisModule_FreeString(ctx, config_ext_load);
    config_ext_load = NULL;
  }
  if (config_friso_ini) {
    RedisModule_FreeString(ctx, config_friso_ini);
    config_friso_ini = NULL;
  }
  if (config_default_scorer) {
    RedisModule_FreeString(ctx, config_default_scorer);
    config_default_scorer = NULL;
  }
  if (RSGlobalConfig.extLoad) {
    rm_free((void *)RSGlobalConfig.extLoad);
    RSGlobalConfig.extLoad = NULL;
  }
  if (RSGlobalConfig.frisoIni) {
    rm_free((void *)RSGlobalConfig.frisoIni);
    RSGlobalConfig.frisoIni = NULL;
  }
  if (RSGlobalConfig.defaultScorer) {
    rm_free((void *)RSGlobalConfig.defaultScorer);
    RSGlobalConfig.defaultScorer = NULL;
  }

  SearchDisk_Close();

  return REDISMODULE_OK;
}
