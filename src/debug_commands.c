#include "debug_commands.h"
#include "inverted_index.h"
#include "index.h"
#include "redis_index.h"
#include "tag_index.h"
#include "numeric_index.h"
#include "phonetic_manager.h"
#include "gc.h"

///////////////////////////////////////////////////////////////////////////////////////////////

#define DUMP_PHONETIC_HASH "DUMP_PHONETIC_HASH"

#define DEBUG_COMMAND(name) static int name(RedisModuleCtx *ctx, RedisModuleString **argv, int argc)

#define GET_SEARCH_CTX(name)                                        \
  RedisSearchCtx *sctx = new RedisSearchCtx(ctx, name, true);             \
  if (!sctx) {                                                      \
    RedisModule_ReplyWithError(ctx, "Can not create a search ctx"); \
    return REDISMODULE_OK;                                          \
  }

#define REPLY_WITH_LONG_LONG(name, val, len)                  \
  RedisModule_ReplyWithStringBuffer(ctx, name, strlen(name)); \
  RedisModule_ReplyWithLongLong(ctx, val);                    \
  len += 2;

#define REPLY_WITH_Str(name, val)                             \
  RedisModule_ReplyWithStringBuffer(ctx, name, strlen(name)); \
  RedisModule_ReplyWithStringBuffer(ctx, val, strlen(val));   \
  bulkLen += 2;

//---------------------------------------------------------------------------------------------

static void ReplyReaderResults(IndexReader *reader, RedisModuleCtx *ctx) {
  auto iter = reader->NewReadIterator();
  IndexResult *r;
  size_t resultSize = 0;
  RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_ARRAY_LEN);
  while (iter->Read(&r) != INDEXREAD_EOF) {
    RedisModule_ReplyWithLongLong(ctx, r->docId);
    ++resultSize;
  }
  RedisModule_ReplySetArrayLength(ctx, resultSize);
  delete iter;
}

//---------------------------------------------------------------------------------------------

static RedisModuleString *getFieldKeyName(IndexSpec *spec, RedisModuleString *fieldNameRS,
                                          FieldType t) {
  const char *fieldName = RedisModule_StringPtrLen(fieldNameRS, NULL);
  const FieldSpec *fieldSpec = spec->GetField(fieldName, strlen(fieldName));
  if (!fieldSpec) {
    return NULL;
  }
  return spec->GetFormattedKey(fieldSpec, t);
}

//---------------------------------------------------------------------------------------------

DEBUG_COMMAND(DumpTerms) {
  if (argc != 1) {
    return RedisModule_WrongArity(ctx);
  }
  GET_SEARCH_CTX(argv[0])

  rune *rstr = NULL;
  t_len slen = 0;
  float score = 0;
  int dist = 0;
  size_t termLen;

  RedisModule_ReplyWithArray(ctx, sctx->spec->terms->size);

  TrieIterator<DFAFilter> it = sctx->spec->terms->Iterate("", 0, 0, 1);
  while (it.Next(&rstr, &slen, NULL, &score, &dist)) {
    char *res = runesToStr(rstr, slen, &termLen);
    RedisModule_ReplyWithStringBuffer(ctx, res, termLen);
    rm_free(res);
  }

  delete sctx;
  return REDISMODULE_OK;
}

//---------------------------------------------------------------------------------------------

DEBUG_COMMAND(InvertedIndexSummary) {
  if (argc != 2) {
    return RedisModule_WrongArity(ctx);
  }

  GET_SEARCH_CTX(argv[0])
  RedisModuleKey *keyp = NULL;
  size_t len;
  const char *invIdxName = RedisModule_StringPtrLen(argv[1], &len);
  InvertedIndex *invidx = Redis_OpenInvertedIndexEx(sctx, invIdxName, len, 0, &keyp);
  size_t invIdxBulkLen = 0;

  if (!invidx) {
    RedisModule_ReplyWithError(sctx->redisCtx, "Can not find the inverted index");
    goto end;
  }
  RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_ARRAY_LEN);

  REPLY_WITH_LONG_LONG("numDocs", invidx->numDocs, invIdxBulkLen);
  REPLY_WITH_LONG_LONG("lastId", invidx->lastId, invIdxBulkLen);
  REPLY_WITH_LONG_LONG("flags", invidx->flags, invIdxBulkLen);
  REPLY_WITH_LONG_LONG("numberOfBlocks", invidx->size, invIdxBulkLen);

  RedisModule_ReplyWithStringBuffer(ctx, "blocks", strlen("blocks"));

  for (uint32_t i = 0; i < invidx->size; ++i) {
    size_t blockBulkLen = 0;
    IndexBlock *block = invidx->blocks + i;
    RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_ARRAY_LEN);

    REPLY_WITH_LONG_LONG("firstId", block->firstId, blockBulkLen);
    REPLY_WITH_LONG_LONG("lastId", block->lastId, blockBulkLen);
    REPLY_WITH_LONG_LONG("numDocs", block->numDocs, blockBulkLen);

    RedisModule_ReplySetArrayLength(ctx, blockBulkLen);
  }

  invIdxBulkLen += 2;

  RedisModule_ReplySetArrayLength(ctx, invIdxBulkLen);

end:
  if (keyp) {
    RedisModule_CloseKey(keyp);
  }
  delete sctx;
  return REDISMODULE_OK;
}

//---------------------------------------------------------------------------------------------

DEBUG_COMMAND(DumpInvertedIndex) {
  if (argc != 2) {
    return RedisModule_WrongArity(ctx);
  }
  GET_SEARCH_CTX(argv[0])
  RedisModuleKey *keyp = NULL;
  size_t len;
  const char *invIdxName = RedisModule_StringPtrLen(argv[1], &len);
  InvertedIndex *invidx = Redis_OpenInvertedIndexEx(sctx, invIdxName, len, 0, &keyp);
  IndexReader *reader = NULL;

  if (!invidx) {
    RedisModule_ReplyWithError(sctx->redisCtx, "Can not find the inverted index");
    goto end;
  }

  reader = new TermIndexReader(invidx, NULL, RS_FIELDMASK_ALL, NULL, 1);
  ReplyReaderResults(reader, sctx->redisCtx);
end:
  if (keyp) {
    RedisModule_CloseKey(keyp);
  }
  delete sctx;
  return REDISMODULE_OK;
}

//---------------------------------------------------------------------------------------------

DEBUG_COMMAND(NumericIndexSummary) {
  if (argc != 2) {
    return RedisModule_WrongArity(ctx);
  }
  GET_SEARCH_CTX(argv[0])
  size_t invIdxBulkLen = 0;
  RedisModuleKey *keyp = NULL;
  RedisModuleString *keyName = getFieldKeyName(sctx->spec, argv[1], INDEXFLD_T_NUMERIC);
  NumericRangeTree *rt = NULL;

  if (!keyName) {
    RedisModule_ReplyWithError(sctx->redisCtx, "Could not find given field in index spec");
    goto end;
  }

  rt = OpenNumericIndex(sctx, keyName, &keyp);
  if (!rt) {
    RedisModule_ReplyWithError(sctx->redisCtx, "can not open numeric field");
    goto end;
  }

  RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_ARRAY_LEN);

  REPLY_WITH_LONG_LONG("numRanges", rt->numRanges, invIdxBulkLen);
  REPLY_WITH_LONG_LONG("numEntries", rt->numEntries, invIdxBulkLen);
  REPLY_WITH_LONG_LONG("lastDocId", rt->lastDocId, invIdxBulkLen);
  REPLY_WITH_LONG_LONG("revisionId", rt->revisionId, invIdxBulkLen);

  RedisModule_ReplySetArrayLength(ctx, invIdxBulkLen);

end:
  if (keyp) {
    RedisModule_CloseKey(keyp);
  }
  delete sctx;
  return REDISMODULE_OK;
}

//---------------------------------------------------------------------------------------------

DEBUG_COMMAND(DumpNumericIndex) {
  if (argc != 2) {
    return RedisModule_WrongArity(ctx);
  }
  GET_SEARCH_CTX(argv[0])
  RedisModuleKey *keyp = NULL;
  RedisModuleString *keyName = getFieldKeyName(sctx->spec, argv[1], INDEXFLD_T_NUMERIC);
  size_t resultSize = 0;
  NumericRangeNode *currNode;
  NumericRangeTreeIterator *iter = NULL;
  NumericRangeTree *rt = NULL;

  if (!keyName) {
    RedisModule_ReplyWithError(sctx->redisCtx, "Could not find given field in index spec");
    goto end;
  }
  rt = OpenNumericIndex(sctx, keyName, &keyp);
  if (!rt) {
    RedisModule_ReplyWithError(sctx->redisCtx, "can not open numeric field");
    goto end;
  }

  iter = new NumericRangeTreeIterator(rt);
  RedisModule_ReplyWithArray(sctx->redisCtx, REDISMODULE_POSTPONED_ARRAY_LEN);
  while (currNode = iter->Next()) {
    if (currNode->range) {
      IndexReader *reader = new NumericIndexReader(&currNode->range->entries);
      ReplyReaderResults(reader, sctx->redisCtx);
      ++resultSize;
    }
  }
  RedisModule_ReplySetArrayLength(sctx->redisCtx, resultSize);
end:
  if (keyp) {
    RedisModule_CloseKey(keyp);
  }
  delete sctx;
  return REDISMODULE_OK;
}

//---------------------------------------------------------------------------------------------

DEBUG_COMMAND(DumpTagIndex) {
  if (argc != 2) {
    return RedisModule_WrongArity(ctx);
  }

  char *tag;
  tm_len_t len;
  InvertedIndex *iv;
  size_t resultSize = 0;
  TrieMapIterator *iter = NULL;
  TagIndex *tagIndex = NULL;

  GET_SEARCH_CTX(argv[0])
  RedisModuleKey *keyp = NULL;
  RedisModuleString *keyName = getFieldKeyName(sctx->spec, argv[1], INDEXFLD_T_TAG);
  if (!keyName) {
    RedisModule_ReplyWithError(sctx->redisCtx, "Could not find given field in index spec");
    goto end;
  }
  tagIndex = TagIndex::Open(sctx, keyName, false, &keyp);
  if (!tagIndex) {
    RedisModule_ReplyWithError(sctx->redisCtx, "can not open tag field");
    goto end;
  }

  iter = tagIndex->values->Iterate("", 0);
  RedisModule_ReplyWithArray(sctx->redisCtx, REDISMODULE_POSTPONED_ARRAY_LEN);
  while (iter->Next(&tag, &len, (void **)&iv)) {
    RedisModule_ReplyWithArray(sctx->redisCtx, 2);
    RedisModule_ReplyWithStringBuffer(sctx->redisCtx, tag, len);
    IndexReader *reader = new TermIndexReader(iv, NULL, RS_FIELDMASK_ALL, NULL, 1);
    ReplyReaderResults(reader, sctx->redisCtx);
    ++resultSize;
  }
  RedisModule_ReplySetArrayLength(sctx->redisCtx, resultSize);
  delete iter;

end:
  if (keyp) {
    RedisModule_CloseKey(keyp);
  }
  delete sctx;
  return REDISMODULE_OK;
}

//---------------------------------------------------------------------------------------------

DEBUG_COMMAND(IdToDocId) {
  if (argc != 2) {
    return RedisModule_WrongArity(ctx);
  }
  GET_SEARCH_CTX(argv[0])
  long long id;
  RSDocumentMetadata *doc = NULL;
  if (RedisModule_StringToLongLong(argv[1], &id) != REDISMODULE_OK) {
    RedisModule_ReplyWithError(sctx->redisCtx, "bad id given");
    goto end;
  }
  doc = sctx->spec->docs.Get(id);
  if (!doc || (doc->flags & Document_Deleted)) {
    RedisModule_ReplyWithError(sctx->redisCtx, "document was removed");
  } else {
    RedisModule_ReplyWithStringBuffer(sctx->redisCtx, doc->keyPtr, strlen(doc->keyPtr));
  }
end:
  delete sctx;
  return REDISMODULE_OK;
}

//---------------------------------------------------------------------------------------------

DEBUG_COMMAND(DocIdToId) {
  if (argc != 2) {
    return RedisModule_WrongArity(ctx);
  }
  GET_SEARCH_CTX(argv[0])
  size_t n;
  const char *key = RedisModule_StringPtrLen(argv[1], &n);
  t_docId id = sctx->spec->docs.GetId(key, n);
  RedisModule_ReplyWithLongLong(sctx->redisCtx, id);
  delete sctx;
  return REDISMODULE_OK;
}

//---------------------------------------------------------------------------------------------

DEBUG_COMMAND(DumpPhoneticHash) {
  if (argc != 1) {
    return RedisModule_WrongArity(ctx);
  }
  size_t len;
  const char *term_c = RedisModule_StringPtrLen(argv[0], &len);

  char *primary = NULL;
  char *secondary = NULL;

  PhoneticManager::ExpandPhonetics(term_c, len, &primary, &secondary);

  RedisModule_ReplyWithArray(ctx, 2);
  RedisModule_ReplyWithStringBuffer(ctx, primary, strlen(primary));
  RedisModule_ReplyWithStringBuffer(ctx, secondary, strlen(secondary));

  rm_free(primary);
  rm_free(secondary);
  return REDISMODULE_OK;
}

//---------------------------------------------------------------------------------------------

static int GCForceInvokeReply(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
#define REPLY "DONE"
  RedisModule_ReplyWithStringBuffer(ctx, REPLY, strlen(REPLY));
  return REDISMODULE_OK;
}

//---------------------------------------------------------------------------------------------

static int GCForceInvokeReplyTimeout(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
#define ERROR_REPLY "INVOCATION FAILED"
  RedisModule_ReplyWithError(ctx, ERROR_REPLY);
  return REDISMODULE_OK;
}

//---------------------------------------------------------------------------------------------

DEBUG_COMMAND(GCForceInvoke) {
#define INVOKATION_TIMEOUT 30000  // gc invocation timeout ms
  if (argc < 1) {
    return RedisModule_WrongArity(ctx);
  }
  IndexSpec *sp = IndexSpec::Load(ctx, RedisModule_StringPtrLen(argv[0], NULL), 0);
  if (!sp) {
    RedisModule_ReplyWithError(ctx, "Unknown index name");
    return REDISMODULE_OK;
  }
  RedisModuleBlockedClient *bc = RedisModule_BlockClient(
      ctx, GCForceInvokeReply, GCForceInvokeReplyTimeout, NULL, INVOKATION_TIMEOUT);
  sp->gc->ForceInvoke(bc);
  return REDISMODULE_OK;
}

//---------------------------------------------------------------------------------------------

DEBUG_COMMAND(GCForceBGInvoke) {
  if (argc < 1) {
    return RedisModule_WrongArity(ctx);
  }
  IndexSpec *sp = IndexSpec::Load(ctx, RedisModule_StringPtrLen(argv[0], NULL), 0);
  if (!sp) {
    RedisModule_ReplyWithError(ctx, "Unknown index name");
    return REDISMODULE_OK;
  }
  sp->gc->ForceBGInvoke();
  RedisModule_ReplyWithSimpleString(ctx, "OK");
  return REDISMODULE_OK;
}

//---------------------------------------------------------------------------------------------

DEBUG_COMMAND(GitSha) {
#ifdef RS_GIT_SHA
  RedisModule_ReplyWithStringBuffer(ctx, RS_GIT_SHA, strlen(RS_GIT_SHA));
#else
  RedisModule_ReplyWithError(ctx, "GIT SHA was not defined on compilation");
#endif
  return REDISMODULE_OK;
}

//---------------------------------------------------------------------------------------------

DEBUG_COMMAND(LogAssert) {
  RS_LOG_ASSERT(7 == 42, "DEBUG");
  return REDISMODULE_OK;
}

//---------------------------------------------------------------------------------------------

typedef struct {
  // Whether to enumerate the number of docids per entry
  int countValueEntries;

  // Whether to enumerate the *actual* document IDs in the entry
  int dumpIdEntries;

  // offset and limit for the tag entry
  unsigned offset, limit;

  // only inspect this value
  const char *prefix;
} DumpOptions;

static void seekTagIterator(TrieMapIterator *it, size_t offset) {
  char *tag;
  tm_len_t len;
  InvertedIndex *iv;

  for (size_t n = 0; n < offset; n++) {
    if (!it->Next(&tag, &len, (void **)&iv)) {
      break;
    }
  }
}

//---------------------------------------------------------------------------------------------

/**
 * INFO_TAGIDX <index> <field> [OPTIONS...]
 */
DEBUG_COMMAND(InfoTagIndex) {
  if (argc < 2) {
    return RedisModule_WrongArity(ctx);
  }
   GET_SEARCH_CTX(argv[0]);
  DumpOptions options = {0};
  ACArgSpec argspecs[] = {{.name = "count_value_entries",
                           .type = AC_ARGTYPE_BOOLFLAG,
                           .target = &options.countValueEntries},
                          {.name = "dump_id_entries",
                           .type = AC_ARGTYPE_BOOLFLAG,
                           .target = &options.dumpIdEntries},
                          {.name = "prefix", .type = AC_ARGTYPE_STRING, .target = &options.prefix},
                          {.name = "offset", .type = AC_ARGTYPE_UINT, .target = &options.offset},
                          {.name = "limit", .type = AC_ARGTYPE_UINT, .target = &options.limit},
                          {NULL}};
  RedisModuleKey *keyp = NULL;
  RedisModuleString *keyName = NULL;
  const TagIndex *idx = NULL;
  ArgsCursor ac;
  ACArgSpec *errSpec = NULL;
  ac.InitRString(argv + 2, argc - 2);
  int rv = ac.ParseArgSpec(argspecs, &errSpec);
  size_t nelem = 0;
  int shouldDescend;
  size_t limit;
  TrieMapIterator *iter = NULL;
  char *tag;
  tm_len_t len;
  InvertedIndex *iv;
  size_t nvalues = 0;

  if (rv != AC_OK) {
    RedisModule_ReplyWithError(ctx, "Could not parse argument (argspec fixme)");
    goto end;
  }

  keyName = getFieldKeyName(sctx->spec, argv[1], INDEXFLD_T_TAG);
  if (!keyName) {
    RedisModule_ReplyWithError(sctx->redisCtx, "Could not find given field in index spec");
    goto end;
  }

  idx = TagIndex::Open(sctx, keyName, false, &keyp);
  if (!idx) {
    RedisModule_ReplyWithError(sctx->redisCtx, "can not open tag field");
    goto end;
  }

  RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_ARRAY_LEN);
  RedisModule_ReplyWithSimpleString(ctx, "num_values");
  RedisModule_ReplyWithLongLong(ctx, idx->values->cardinality);
  nelem += 2;

  if (options.dumpIdEntries) {
    options.countValueEntries = 1;
  }

  shouldDescend = options.countValueEntries || options.dumpIdEntries;
  if (!shouldDescend) {
    goto reply_done;
  }

  limit = options.limit ? options.limit : 0;
  iter = idx->values->Iterate("", 0);
  nelem += 2;
  RedisModule_ReplyWithSimpleString(ctx, "values");
  RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_ARRAY_LEN);

  seekTagIterator(iter, options.offset);
  while (nvalues++ < limit && iter->Next(&tag, &len, (void **)&iv)) {
    size_t nsubelem = 8;
    if (!options.dumpIdEntries) {
      nsubelem -= 2;
    }
    RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_ARRAY_LEN);

    RedisModule_ReplyWithSimpleString(ctx, "value");
    RedisModule_ReplyWithStringBuffer(ctx, tag, len);

    RedisModule_ReplyWithSimpleString(ctx, "num_entries");
    RedisModule_ReplyWithLongLong(ctx, iv->numDocs);

    RedisModule_ReplyWithSimpleString(ctx, "num_blocks");
    RedisModule_ReplyWithLongLong(ctx, iv->size);

    if (options.dumpIdEntries) {
      RedisModule_ReplyWithSimpleString(ctx, "entries");
      IndexReader *reader = new TermIndexReader(iv, NULL, RS_FIELDMASK_ALL, NULL, 1);
      ReplyReaderResults(reader, sctx->redisCtx);
    }

    RedisModule_ReplySetArrayLength(ctx, nsubelem);
  }
  delete iter;
  RedisModule_ReplySetArrayLength(ctx, nvalues - 1);

reply_done:
  RedisModule_ReplySetArrayLength(ctx, nelem);

end:
  if (keyp) {
    RedisModule_CloseKey(keyp);
  }
  delete sctx;
  return REDISMODULE_OK;
}

//---------------------------------------------------------------------------------------------

static void replyDocFlags(const RSDocumentMetadata *dmd, RedisModuleCtx *ctx) {
  char buf[1024] = {0};
  sprintf(buf, "(0x%x):", dmd->flags);
  if (dmd->flags & Document_Deleted) {
    strcat(buf, "Deleted,");
  }
  if (dmd->flags & Document_HasPayload) {
    strcat(buf, "HasPayload,");
  }
  if (dmd->flags & Document_HasSortVector) {
    strcat(buf, "HasSortVector,");
  }
  if (dmd->flags & Document_HasOffsetVector) {
    strcat(buf, "HasOffsetVector,");
  }
  RedisModule_ReplyWithSimpleString(ctx, buf);
}

//---------------------------------------------------------------------------------------------

static void replySortVector(const RSDocumentMetadata *dmd, RedisSearchCtx *sctx) {
  RSSortingVector *sv = dmd->sortVector;
  RedisModule_ReplyWithArray(sctx->redisCtx, REDISMODULE_POSTPONED_ARRAY_LEN);
  size_t nelem = 0;
  for (size_t ii = 0; ii < sv->len; ++ii) {
    if (!sv->values[ii]) {
      continue;
    }
    RedisModule_ReplyWithArray(sctx->redisCtx, 6);
    RedisModule_ReplyWithSimpleString(sctx->redisCtx, "index");
    RedisModule_ReplyWithLongLong(sctx->redisCtx, ii);
    RedisModule_ReplyWithSimpleString(sctx->redisCtx, "field");
    const FieldSpec *fs = sctx->spec->GetFieldBySortingIndex(ii);
    RedisModule_ReplyWithSimpleString(sctx->redisCtx, fs ? fs->name : "!!!???");
    RedisModule_ReplyWithSimpleString(sctx->redisCtx, "value");
    sv->values[ii]->SendReply(sctx->redisCtx, false);
    nelem++;
  }
  RedisModule_ReplySetArrayLength(sctx->redisCtx, nelem);
}

//---------------------------------------------------------------------------------------------

/**
 * FT.DEBUG DOC_INFO <index> <doc>
 */
DEBUG_COMMAND(DocInfo) {
  if (argc < 2) {
    return RedisModule_WrongArity(ctx);
  }
  GET_SEARCH_CTX(argv[0]);

  const RSDocumentMetadata *dmd = sctx->spec->docs.GetByKeyR(argv[1]);
  if (!dmd) {
    delete sctx;
    return RedisModule_ReplyWithError(ctx, "Document not found in index");
  }

  size_t nelem = 0;
  RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_ARRAY_LEN);
  RedisModule_ReplyWithSimpleString(ctx, "internal_id");
  RedisModule_ReplyWithLongLong(ctx, dmd->id);
  nelem += 2;
  RedisModule_ReplyWithSimpleString(ctx, "flags");
  replyDocFlags(dmd, ctx);
  nelem += 2;
  RedisModule_ReplyWithSimpleString(ctx, "score");
  RedisModule_ReplyWithDouble(ctx, dmd->score);
  nelem += 2;
  RedisModule_ReplyWithSimpleString(ctx, "num_tokens");
  RedisModule_ReplyWithLongLong(ctx, dmd->len);
  nelem += 2;
  RedisModule_ReplyWithSimpleString(ctx, "max_freq");
  RedisModule_ReplyWithLongLong(ctx, dmd->maxFreq);
  nelem += 2;
  RedisModule_ReplyWithSimpleString(ctx, "refcount");
  RedisModule_ReplyWithLongLong(ctx, dmd->ref_count);
  nelem += 2;
  if (dmd->sortVector) {
    RedisModule_ReplyWithSimpleString(ctx, "sortables");
    replySortVector(dmd, sctx);
    nelem += 2;
  }
  RedisModule_ReplySetArrayLength(ctx, nelem);
  delete sctx;
  return REDISMODULE_OK;
}

//---------------------------------------------------------------------------------------------

typedef struct DebugCommandType {
  char *name;
  int (*callback)(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);
} DebugCommandType;

DebugCommandType commands[] = {{"DUMP_INVIDX", DumpInvertedIndex},
                               {"DUMP_NUMIDX", DumpNumericIndex},
                               {"DUMP_TAGIDX", DumpTagIndex},
                               {"INFO_TAGIDX", InfoTagIndex},
                               {"IDTODOCID", IdToDocId},
                               {"DOCIDTOID", DocIdToId},
                               {"DOCINFO", DocInfo},
                               {"DUMP_PHONETIC_HASH", DumpPhoneticHash},
                               {"DUMP_TERMS", DumpTerms},
                               {"INVIDX_SUMMARY", InvertedIndexSummary},
                               {"NUMIDX_SUMMARY", NumericIndexSummary},
                               {"GC_FORCEINVOKE", GCForceInvoke},
                               {"GC_FORCEBGINVOKE", GCForceBGInvoke},
                               {"GIT_SHA", GitSha},
                               {"LOGASSERT", LogAssert},
                               {NULL, NULL}};

//---------------------------------------------------------------------------------------------

int DebugCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {

  if (argc < 2) {
    return RedisModule_WrongArity(ctx);
  }

  const char *subCommand = RedisModule_StringPtrLen(argv[1], NULL);

  if (strcasecmp(subCommand, "help") == 0) {
    size_t len = 0;
    RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_ARRAY_LEN);
    for (DebugCommandType *c = &commands[0]; c->name != NULL; c++) {
      RedisModule_ReplyWithStringBuffer(ctx, c->name, strlen(c->name));
      ++len;
    }
    RedisModule_ReplySetArrayLength(ctx, len);
    return REDISMODULE_OK;
  }

  for (DebugCommandType *c = &commands[0]; c->name != NULL; c++) {
    if (strcasecmp(c->name, subCommand) == 0) {
      return c->callback(ctx, argv + 2, argc - 2);
    }
  }

  RedisModule_ReplyWithError(ctx, "subcommand was not found");

  return REDISMODULE_OK;
}
