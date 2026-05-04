/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#include "spec.h"
#include "field_spec.h"
#include "document.h"
#include "rmutil/rm_assert.h"
#include "util/dict.h"
#include "util/references.h"
#include "query_node.h"
#include "search_options.h"
#include "query_internal.h"
#include "numeric_filter.h"
#include "suffix.h"
#include "query.h"
#include "indexer.h"
#include "extension.h"
#include "ext/default.h"
#include <float.h>
#include "rwlock.h"
#include "fork_gc.h"
#include "module.h"
#include "cursor.h"
#include "info/indexes_info.h"
#include "config.h"
#include "util/workers.h"
#include "util/arr/arr.h"
#include "vector_index.h"
#include "util/threadpool_api.h"

/**
 * Most of the spec interaction is done through the RefManager, which is wrapped by a strong or weak reference struct.
 * In the LLAPI we return a pointer to an RSIndex. In order to not break the API, we typedef the RSIndex to be RefManager
 * and we return it instead of the strong reference that should wrap it. we can assume that every time we get a RefManager,
 * we can cast it to (wrap it with) a strong reference and use it as such.
 */

int RediSearch_GetCApiVersion() {
  return REDISEARCH_CAPI_VERSION;
}

RefManager* RediSearch_CreateIndex(const char* name, const RSIndexOptions* options) {
  RSIndexOptions opts_s = {.gcPolicy = GC_POLICY_FORK, .stopwordsLen = -1};
  if (!options) {
    options = &opts_s;
  }
  IndexSpec* spec = NewIndexSpec(NewHiddenString(name, strlen(name), true));
  spec->own_ref = StrongRef_New(spec, (RefManager_Free)IndexSpec_Free);
  IndexSpec_MakeKeyless(spec);
  spec->flags |= Index_Temporary;  // temporary is so that we will not use threads!!
  spec->flags |= Index_FromLLAPI;

  if (options->score || options->lang) {
    spec->rule = rm_calloc(1, sizeof *spec->rule);
    spec->rule->score_default = options->score ? options->score : DEFAULT_SCORE;
    spec->rule->lang_default = RSLanguage_Find(options->lang, 0);
  }

  spec->getValue = options->gvcb;
  spec->getValueCtx = options->gvcbData;
  if (options->flags & RSIDXOPT_DOCTBLSIZE_UNLIMITED) {
    spec->docs.maxSize = DOCID_MAX;
  }
  if (options->gcPolicy != GC_POLICY_NONE) {
    IndexSpec_StartGCFromSpec(spec->own_ref, spec, options->gcPolicy);
  }
  if (options->stopwordsLen != -1) {
    // replace default list which is a global so no need to free anything.
    spec->stopwords = NewStopWordListCStr((const char **)options->stopwords,
                                                         options->stopwordsLen);
  }
  return spec->own_ref.rm;
}

void RediSearch_DropIndex(RefManager* rm) {
  RWLOCK_ACQUIRE_WRITE();
  StrongRef ref = {rm};
  StrongRef_Invalidate(ref);
  StrongRef_Release(ref);
  RWLOCK_RELEASE();
}

RefManager* RediSearch_IndexClone(RefManager* rm) {
  StrongRef cloned = StrongRef_Clone((StrongRef){rm});
  return cloned.rm;
}

void RediSearch_IndexRelease(RefManager* rm) {
  StrongRef_Release((StrongRef){rm});
}

int RediSearch_SetDefaultScorer(const char* name) {
  if (!name) {
    return REDISEARCH_ERR;
  }
  if (!Extensions_InitDone() ||
      Extensions_GetScoringFunction(NULL, name) == NULL) {
    return REDISEARCH_ERR;
  }
  if (RSGlobalConfig.defaultScorer) {
    rm_free((void*)RSGlobalConfig.defaultScorer);
  }
  RSGlobalConfig.defaultScorer = rm_strdup(name);
  return REDISEARCH_OK;
}

int RediSearch_SetNumWorkerThreads(size_t n) {
  if (n > MAX_WORKER_THREADS) {
    return REDISEARCH_ERR;
  }
  RSGlobalConfig.numWorkerThreads = n;
  // If the pool is already up (we're past RediSearch_Init), live-resize
  // it. Pre-init this is a no-op; the pool is created at the requested
  // size during init.
  if (workersThreadPool_NumThreads() > 0 || n > 0) {
    workersThreadPool_SetNumWorkers();
  }
  return REDISEARCH_OK;
}

char **RediSearch_IndexGetStopwords(RefManager* rm, size_t *size) {
  IndexSpec *spec = __RefManager_Get_Object(rm);
  return GetStopWordsList(spec->stopwords, size);
}

void RediSearch_StopwordsList_Free(char **list, size_t size) {
  for (int i = 0; i < size; ++i) {
    rm_free(list[i]);
  }
  rm_free(list);
}

double RediSearch_IndexGetScore(RefManager* rm) {
  IndexSpec *spec = __RefManager_Get_Object(rm);
  if (spec->rule) {
    return spec->rule->score_default;
  }
  return DEFAULT_SCORE;
}

const char *RediSearch_IndexGetLanguage(RefManager* rm) {
  IndexSpec *spec = __RefManager_Get_Object(rm);
  if (spec->rule) {
    return RSLanguage_ToString(spec->rule->lang_default);
  }
  return RSLanguage_ToString(DEFAULT_LANGUAGE);
}

int RediSearch_ValidateLanguage(const char *lang) {
  if (!lang || RSLanguage_Find(lang, 0) == RS_LANG_UNSUPPORTED) {
    return REDISEARCH_ERR;
  }
  return REDISEARCH_OK;
}

RSFieldID RediSearch_CreateField(RefManager* rm, const char* name, unsigned types,
                                 unsigned options) {
  RS_LOG_ASSERT(types, "types should not be RSFLDTYPE_DEFAULT");
  RWLOCK_ACQUIRE_WRITE();
  IndexSpec *sp = __RefManager_Get_Object(rm);

  // TODO: add a function which can take both path and name
  FieldSpec* fs = IndexSpec_CreateField(sp, name, NULL);
  RS_LOG_ASSERT_FMT(fs, "Failed to create field %s", name);

  int numTypes = 0;
  if (types & RSFLDTYPE_FULLTEXT) {
    numTypes++;
    int txtId = IndexSpec_CreateTextId(sp, fs->index);
    if (txtId < 0) {
      RWLOCK_RELEASE();
      return RSFIELD_INVALID;
    }
    fs->ftId = txtId;
    fs->types |= INDEXFLD_T_FULLTEXT;
  }

  if (types & RSFLDTYPE_NUMERIC) {
    numTypes++;
    fs->types |= INDEXFLD_T_NUMERIC;
  }
  if (types & RSFLDTYPE_GEO) {
    fs->types |= INDEXFLD_T_GEO;
    numTypes++;
  }
  if (types & RSFLDTYPE_VECTOR) {
    fs->types |= INDEXFLD_T_VECTOR;
    numTypes++;
  }
  if (types & RSFLDTYPE_TAG) {
    fs->types |= INDEXFLD_T_TAG;
    numTypes++;
  }
  // TODO: GEOMETRY
  // if (types & RSFLDTYPE_GEOMETRY) {
  //   fs->types |= INDEXFLD_T_GEOMETRY;
  //   numTypes++;
  // }

  if (numTypes > 1) {
    fs->options |= FieldSpec_Dynamic;
  }

  if (options & RSFLDOPT_NOINDEX) {
    fs->options |= FieldSpec_NotIndexable;
  }
  if (options & RSFLDOPT_SORTABLE) {
    fs->options |= FieldSpec_Sortable;
    fs->sortIdx = sp->numSortableFields++;
  }
  if (options & RSFLDOPT_TXTNOSTEM) {
    fs->options |= FieldSpec_NoStemming;
  }
  if (options & RSFLDOPT_TXTPHONETIC) {
    fs->options |= FieldSpec_Phonetics;
    sp->flags |= Index_HasPhonetic;
  }
  if (options & RSFLDOPT_WITHSUFFIXTRIE) {
    fs->options |= FieldSpec_WithSuffixTrie;
    if (fs->types == INDEXFLD_T_FULLTEXT) {
      sp->suffixMask |= FIELD_BIT(fs);
      if (!sp->suffix) {
        sp->suffix = NewTrie(suffixTrie_freeCallback, Trie_Sort_Lex);
        sp->flags |= Index_HasSuffixTrie;
      }
    }
  }

  RWLOCK_RELEASE();
  return fs->index;
}

void RediSearch_IndexExisting(RefManager* rm, SchemaRuleArgs* args) {
  RWLOCK_ACQUIRE_WRITE();
  IndexSpec *sp = __RefManager_Get_Object(rm);
  if (!sp->rule) {
    sp->rule = SchemaRule_Create(args, IndexSpec_GetStrongRefUnsafe(sp), NULL);
  }
  sp->rule->index_all = true;
  RWLOCK_RELEASE();
}

void RediSearch_TextFieldSetWeight(RefManager* rm, RSFieldID id, double w) {
  IndexSpec *sp = __RefManager_Get_Object(rm);
  FieldSpec* fs = sp->fields + id;
  RS_LOG_ASSERT(FIELD_IS(fs, INDEXFLD_T_FULLTEXT), "types should be INDEXFLD_T_FULLTEXT");
  fs->ftWeight = w;
}

void RediSearch_TagFieldSetSeparator(RefManager* rm, RSFieldID id, char sep) {
  IndexSpec *sp = __RefManager_Get_Object(rm);
  FieldSpec* fs = sp->fields + id;
  RS_LOG_ASSERT(FIELD_IS(fs, INDEXFLD_T_TAG), "types should be INDEXFLD_T_TAG");
  fs->tagOpts.tagSep = sep;
}

void RediSearch_TagFieldSetCaseSensitive(RefManager* rm, RSFieldID id, int enable) {
  IndexSpec *sp = __RefManager_Get_Object(rm);
  FieldSpec* fs = sp->fields + id;
  RS_LOG_ASSERT(FIELD_IS(fs, INDEXFLD_T_TAG), "types should be INDEXFLD_T_TAG");
  if (enable) {
    fs->tagOpts.tagFlags |= TagField_CaseSensitive;
  } else {
    fs->tagOpts.tagFlags &= ~TagField_CaseSensitive;
  }
}

RSDoc* RediSearch_CreateDocument(const void* docKey, size_t len, double score, const char* lang) {
  RedisModuleString* docKeyStr = RedisModule_CreateString(NULL, docKey, len);
  RSLanguage language = lang ? RSLanguage_Find(lang, 0) : DEFAULT_LANGUAGE;
  Document* ret = rm_calloc(1, sizeof(*ret));
  Document_Init(ret, docKeyStr, score, language, DocumentType_Hash);
  Document_MakeStringsOwner(ret);
  RedisModule_FreeString(RSDummyContext, docKeyStr);
  return ret;
}

RSDoc* RediSearch_CreateDocument2(const void* docKey, size_t len, RefManager* rm,
                                  double score, const char* lang) {
  IndexSpec* sp = __RefManager_Get_Object(rm);
  RedisModuleString* docKeyStr = RedisModule_CreateString(NULL, docKey, len);

  RSLanguage language = lang ? RSLanguage_Find(lang, 0) :
             (sp && sp->rule) ? sp->rule->lang_default : DEFAULT_LANGUAGE;
  double docScore = !isnan(score) ? score :
             (sp && sp->rule) ? sp->rule->score_default : DEFAULT_SCORE;

  Document* ret = rm_calloc(1, sizeof(*ret));
  Document_Init(ret, docKeyStr, docScore, language, DocumentType_Hash);
  Document_MakeStringsOwner(ret);
  RedisModule_FreeString(RSDummyContext, docKeyStr);
  return ret;
}

void RediSearch_FreeDocument(RSDoc* doc) {
  Document_Free(doc);
  rm_free(doc);
}

int RediSearch_DeleteDocument(RefManager* rm, const void* docKey, size_t len) {
  RWLOCK_ACQUIRE_WRITE();
  IndexSpec* sp = __RefManager_Get_Object(rm);
  int rc = REDISMODULE_OK;
  t_docId id = DocTable_GetId(&sp->docs, docKey, len);
  if (id == 0) {
    rc = REDISMODULE_ERR;
  } else {
    RSDocumentMetadata* md = DocTable_Pop(&sp->docs, docKey, len);
    if (md) {
      // Delete returns true/false, not RM_{OK,ERR}
      RS_LOG_ASSERT(sp->stats.numDocuments > 0, "numDocuments cannot be negative");
      sp->stats.numDocuments--;
      RS_LOG_ASSERT(sp->stats.totalDocsLen >= md->docLen, "totalDocsLen is smaller than md->docLen");
      sp->stats.totalDocsLen -= md->docLen;
      DMD_Return(md);

      if (sp->gc) {
        GCContext_OnDelete(sp->gc);
      }
    } else {
      rc = REDISMODULE_ERR;
    }
  }

  RWLOCK_RELEASE();
  return rc;
}

void RediSearch_DocumentAddField(Document* d, const char* fieldName, RedisModuleString* value,
                                 unsigned as) {
  Document_AddField(d, fieldName, value, as);
}

void RediSearch_DocumentAddFieldString(Document* d, const char* fieldName, const char* s, size_t n,
                                       unsigned as) {
  Document_AddFieldC(d, fieldName, s, n, as);
}

void RediSearch_DocumentAddFieldNumber(Document* d, const char* fieldName, double val, unsigned as) {
  if (as == RSFLDTYPE_NUMERIC) {
    Document_AddNumericField(d, fieldName, val, as);
  } else {
    char buf[512];
    size_t len = sprintf(buf, "%lf", val);
    Document_AddFieldC(d, fieldName, buf, len, as);
  }
}

void RediSearch_DocumentAddFieldVector(Document* d, const char* fieldName,
                                       const char* vector, size_t nbytes) {
  DocumentField* f = addFieldCommon(d, fieldName, INDEXFLD_T_VECTOR);
  f->blobArr = rm_malloc(nbytes);
  memcpy(f->blobArr, vector, nbytes);
  f->blobSize = nbytes;
  f->blobArrLen = 1;
  f->unionType = FLD_VAR_T_BLOB_ARRAY;
}

void RediSearch_DocumentAddFieldNumericArray(Document* d, const char* fieldName,
                                             const double* values, size_t count,
                                             unsigned indexAsTypes) {
  arrayof(double) arr = array_new(double, count);
  for (size_t i = 0; i < count; ++i) {
    array_append(arr, values[i]);
  }
  DocumentField* f = addFieldCommon(d, fieldName, indexAsTypes);
  f->arrNumval = arr;
  // arrayLen overlays the second 8 bytes of the union with the
  // string-array variant. estimtateTermCount() reads it for every
  // FLD_VAR_T_ARRAY field as a multiVal length, so we must zero it
  // (the numeric indexer uses arr.h's array_len() instead).
  f->arrayLen = 0;
  // multisv lives outside the union; addFieldCommon doesn't initialize
  // it, and the array path reads it (e.g. in fulltextPreprocessor).
  f->multisv = NULL;
  f->unionType = FLD_VAR_T_ARRAY;
}

void RediSearch_DocumentAddFieldStringArray(Document* d, const char* fieldName,
                                            const char** values, size_t count,
                                            unsigned indexAsTypes) {
  DocumentField* f = addFieldCommon(d, fieldName, indexAsTypes);
  f->multiVal = rm_malloc(count * sizeof(*f->multiVal));
  for (size_t i = 0; i < count; ++i) {
    f->multiVal[i] = rm_strdup(values[i]);
  }
  f->arrayLen = count;
  f->multisv = NULL;
  f->unionType = FLD_VAR_T_ARRAY;
}

// Compute the per-vector blob size from the algorithm-level params,
// unwrapping VecSimAlgo_TIERED to look at the primary index params.
static size_t vecSimParams_ExpBlobSize(const VecSimParams* p) {
  const VecSimParams* primary = p;
  if (p->algo == VecSimAlgo_TIERED) {
    primary = p->algoParams.tieredParams.primaryIndexParams;
    if (!primary) return 0;
  }
  switch (primary->algo) {
    case VecSimAlgo_BF:
      return primary->algoParams.bfParams.dim *
             VecSimType_sizeof(primary->algoParams.bfParams.type);
    case VecSimAlgo_HNSWLIB:
      return primary->algoParams.hnswParams.dim *
             VecSimType_sizeof(primary->algoParams.hnswParams.type);
    case VecSimAlgo_SVS:
      return primary->algoParams.svsParams.dim *
             VecSimType_sizeof(primary->algoParams.svsParams.type);
    default:
      return 0;
  }
}

// _workers_thpool is the process-global worker pool defined in
// util/workers.c. workers.h doesn't expose it because callers should
// use the workersThreadPool_* helpers. The TIERED runtime needs the
// raw pool pointer so we forward-declare it here.
extern redisearch_thpool_t *_workers_thpool;

int RediSearch_VecSimTieredParams_Init(VecSimParams* params, RefManager* rm,
                                       const char* field_name) {
  if (!params || !rm || !field_name) return REDISEARCH_ERR;
  if (params->algo != VecSimAlgo_TIERED) return REDISEARCH_ERR;
  if (!params->algoParams.tieredParams.primaryIndexParams) return REDISEARCH_ERR;

  TieredIndexParams* tp = &params->algoParams.tieredParams;
  tp->jobQueue = _workers_thpool;
  tp->flatBufferLimit = RSGlobalConfig.tieredVecSimIndexBufferLimit;
  StrongRef sref = {rm};
  tp->jobQueueCtx = StrongRef_Demote(sref).rm;
  tp->submitCb = (SubmitCB)ThreadPoolAPI_SubmitIndexJobs;

  // logCtx tags log messages with the field name. parseVectorField
  // uses HiddenString_GetUnsafe(spec_owned_name) which is borrowed; we
  // own a heap copy here since the embedder's `field_name` lifetime is
  // not tied to the spec.
  VecSimLogCtx* logCtx = rm_new(VecSimLogCtx);
  logCtx->index_field_name = rm_strdup(field_name);
  params->logCtx = logCtx;
  // Mirror the same logCtx pointer into the primary, matching
  // parseVectorField. Single-owner; freed once when VecSim destroys
  // the index.
  params->algoParams.tieredParams.primaryIndexParams->logCtx = logCtx;

  return REDISEARCH_OK;
}

int RediSearch_VectorFieldSetParams(RefManager* rm, RSFieldID id,
                                    const VecSimParams* params) {
  if (!rm || !params) return REDISEARCH_ERR;
  IndexSpec* sp = __RefManager_Get_Object(rm);
  if (!sp || id >= sp->numFields) return REDISEARCH_ERR;
  FieldSpec* fs = sp->fields + id;
  if (!FIELD_IS(fs, INDEXFLD_T_VECTOR)) return REDISEARCH_ERR;

  size_t exp = vecSimParams_ExpBlobSize(params);
  if (exp == 0) return REDISEARCH_ERR;

  // Deep-copy `params`. For TIERED we own the primaryIndexParams
  // allocation so the caller can free their stack copy on return.
  fs->vectorOpts.vecSimParams = *params;
  if (params->algo == VecSimAlgo_TIERED) {
    VecSimParams* primary_copy = rm_calloc(1, sizeof(VecSimParams));
    *primary_copy = *params->algoParams.tieredParams.primaryIndexParams;
    fs->vectorOpts.vecSimParams.algoParams.tieredParams.primaryIndexParams =
        primary_copy;
  }

  // VecSim invokes the global log callback with params->logCtx as the
  // first argument; the default callback (VecSimLogCallback) dereferences
  // it. If the embedder didn't populate logCtx (typical for FLAT, where
  // there's no TIERED helper to call), default-allocate one tagged with
  // the field name. For TIERED we mirror it into the primary too, matching
  // parseVectorField.
  if (!fs->vectorOpts.vecSimParams.logCtx) {
    VecSimLogCtx* logCtx = rm_new(VecSimLogCtx);
    logCtx->index_field_name = HiddenString_GetUnsafe(fs->fieldName, NULL);
    fs->vectorOpts.vecSimParams.logCtx = logCtx;
    if (params->algo == VecSimAlgo_TIERED) {
      fs->vectorOpts.vecSimParams.algoParams.tieredParams
          .primaryIndexParams->logCtx = logCtx;
    }
  }

  fs->vectorOpts.expBlobSize = exp;

  // Construct the underlying VecSim index eagerly so any algorithm-
  // setup error surfaces here rather than at first add.
  if (!openVectorIndex(fs, CREATE_INDEX)) return REDISEARCH_ERR;
  sp->flags |= Index_HasVecSim;

  return REDISEARCH_OK;
}

QueryNode* RediSearch_CreateVecSimNode(RefManager* rm, const char* field,
                                       const char* vector,
                                       size_t vector_nbytes, size_t k) {
  if (!rm || !field || !vector) return NULL;
  IndexSpec* sp = __RefManager_Get_Object(rm);
  if (!sp) return NULL;
  const FieldSpec* fs = IndexSpec_GetFieldWithLength(sp, field, strlen(field));
  if (!fs || !FIELD_IS(fs, INDEXFLD_T_VECTOR)) return NULL;

  QueryNode* ret = NewQueryNode(QN_VECTOR);
  VectorQuery* vq = rm_calloc(1, sizeof(*vq));
  ret->vn.vq = vq;
  vq->type = VECSIM_QT_KNN;
  vq->field = (FieldSpec*)fs;  // borrowed; spec owns the FieldSpec
  ret->opts.flags |= QueryNode_YieldsDistance;
  ret->opts.fieldIndex = fs->index;
  ret->opts.fieldMask = IndexSpec_GetFieldBit(sp, field, strlen(field));

  vq->knn.vector = rm_malloc(vector_nbytes);
  memcpy((void*)vq->knn.vector, vector, vector_nbytes);
  vq->knn.vecLen = vector_nbytes;
  vq->knn.k = k;
  vq->knn.shardWindowRatio = DEFAULT_SHARD_WINDOW_RATIO;

  return ret;
}

void RediSearch_VecSimSetLogCallback(RediSearch_VecSimLogCB cb) {
  // logCallbackFunction has the same shape as RediSearch_VecSimLogCB.
  VecSim_SetLogCallbackFunction((logCallbackFunction)cb);
}

void* RediSearch_IndexWeakRef(RefManager* rm) {
  if (!rm) return NULL;
  StrongRef sref = {rm};
  WeakRef wref = StrongRef_Demote(sref);
  return wref.rm;
}

RefManager* RediSearch_WeakRefPromote(void* weak_ref) {
  WeakRef wref = {weak_ref};
  StrongRef sref = WeakRef_Promote(wref);
  return sref.rm;
}

int RediSearch_DocumentAddFieldGeo(Document* d, const char* fieldName,
                                    double lat, double lon, unsigned as) {
  if (lat > GEO_LAT_MAX || lat < GEO_LAT_MIN || lon > GEO_LONG_MAX || lon < GEO_LONG_MIN) {
    // out of range
    return REDISMODULE_ERR;
  }

  if (as == RSFLDTYPE_GEO) {
    Document_AddGeoField(d, fieldName, lon, lat, as);
  } else {
    char buf[24];
    size_t len = sprintf(buf, "%.6lf,%.6lf", lon, lat);
    Document_AddFieldC(d, fieldName, buf, len, as);
  }

  return REDISMODULE_OK;
}

typedef struct {
  char** s;
  int hasErr;
} RSError;

void RediSearch_AddDocDone(RSAddDocumentCtx* aCtx, RedisModuleCtx* ctx, void* err) {
  RSError* ourErr = err;
  if (QueryError_HasError(&aCtx->status)) {
    if (ourErr->s) {
      *ourErr->s = rm_strdup(QueryError_GetUserError(&aCtx->status));
    }
    ourErr->hasErr = QueryError_GetCode(&aCtx->status);
  }
}

int RediSearch_IndexAddDocument(RefManager* rm, Document* d, int options, char** errs) {
  RWLOCK_ACQUIRE_WRITE();
  IndexSpec* sp = __RefManager_Get_Object(rm);
  pthread_rwlock_wrlock(&sp->rwlock);

  RSError err = {.s = errs};
  QueryError status = QueryError_Default();
  RSAddDocumentCtx* aCtx = NewAddDocumentCtx(sp, d, &status);
  if (aCtx == NULL) {
    QueryError_ClearError(&status);
    pthread_rwlock_unlock(&sp->rwlock);
    RWLOCK_RELEASE();
    return REDISMODULE_ERR;
  }
  aCtx->donecb = RediSearch_AddDocDone;
  aCtx->donecbData = &err;
  RedisSearchCtx sctx = {.redisCtx = NULL, .spec = sp};
  int exists = !!DocTable_GetIdR(&sp->docs, d->docKey);
  if (exists) {
    if (options & REDISEARCH_ADD_REPLACE) {
      options |= DOCUMENT_ADD_REPLACE;
    } else {
      if (errs) {
        *errs = rm_strdup("Document already exists");
      }
      AddDocumentCtx_Free(aCtx);
      pthread_rwlock_unlock(&sp->rwlock);
      RWLOCK_RELEASE();
      return REDISMODULE_ERR;
    }
  }

  options |= DOCUMENT_ADD_NOSAVE;
  AddDocumentCtx_Submit(aCtx, &sctx, options);
  QueryError_ClearError(&status);
  rm_free(d);

  pthread_rwlock_unlock(&sp->rwlock);
  RWLOCK_RELEASE();
  return err.hasErr ? REDISMODULE_ERR : REDISMODULE_OK;
}

QueryNode* RediSearch_CreateTokenNode(RefManager* rm, const char* fieldName, const char* token) {
  IndexSpec* sp = __RefManager_Get_Object(rm);
  if (StopWordList_Contains(sp->stopwords, token, strlen(token))) {
    return NULL;
  }
  QueryNode* ret = NewQueryNode(QN_TOKEN);

  ret->tn = (QueryTokenNode){
      .str = (char*)rm_strdup(token), .len = strlen(token), .expanded = 0, .flags = 0};
  if (fieldName) {
    ret->opts.fieldMask = IndexSpec_GetFieldBit(sp, fieldName, strlen(fieldName));
  }
  return ret;
}

QueryNode* RediSearch_CreateTagTokenNode(RefManager* rm, const char* token) {
  QueryNode* ret = NewQueryNode(QN_TOKEN);
  ret->tn = (QueryTokenNode){
      .str = (char*)rm_strdup(token), .len = strlen(token), .expanded = 0, .flags = 0};
  return ret;
}

QueryNode* RediSearch_CreateNumericNode(RefManager* rm, const char* field, double max, double min,
                                        int includeMax, int includeMin) {
  QueryNode* ret = NewQueryNode(QN_NUMERIC);
  const size_t len = strlen(field);
  const FieldSpec *fs = IndexSpec_GetFieldWithLength(__RefManager_Get_Object(rm), field, len);
  ret->nn.nf = NewNumericFilter(min, max, includeMin, includeMax, true, fs);
  ret->opts.fieldMask = IndexSpec_GetFieldBit(__RefManager_Get_Object(rm), field, len);
  return ret;
}

QueryNode* RediSearch_CreateGeoNode(RefManager* rm, const char* field, double lat, double lon,
                                        double radius, RSGeoDistance unitType) {
  QueryNode* ret = NewQueryNode(QN_GEO);
  ret->opts.fieldMask = IndexSpec_GetFieldBit(__RefManager_Get_Object(rm), field, strlen(field));

  GeoFilter *flt = rm_malloc(sizeof(*flt));
  flt->lat = lat;
  flt->lon = lon;
  flt->radius = radius;
  flt->numericFilters = NULL;
  flt->fieldSpec = IndexSpec_GetFieldWithLength(__RefManager_Get_Object(rm), field, strlen(field));
  flt->unitType = (GeoDistance)unitType;

  ret->gn.gf = flt;

  return ret;
}

#define NODE_PREFIX 0x1
#define NODE_SUFFIX 0x2

static QueryNode* RediSearch_CreateAffixNode(IndexSpec* sp, const char* fieldName,
                                             const char* s, int flags) {
  QueryNode* ret = NewQueryNode(QN_PREFIX);
  ret->pfx = (QueryPrefixNode){
    .tok = (RSToken){.str = (char*)rm_strdup(s), .len = strlen(s), .expanded = 0, .flags = 0},
    .prefix = flags & NODE_PREFIX,
    .suffix = flags & NODE_SUFFIX,
  };
  if (fieldName) {
    ret->opts.fieldMask = IndexSpec_GetFieldBit(sp, fieldName, strlen(fieldName));
  }
  return ret;
}

QueryNode* RediSearch_CreatePrefixNode(RefManager* rm, const char* fieldName, const char* s) {
  return RediSearch_CreateAffixNode(__RefManager_Get_Object(rm), fieldName, s, NODE_PREFIX);
}

QueryNode* RediSearch_CreateContainsNode(RefManager* rm, const char* fieldName, const char* s) {
  return RediSearch_CreateAffixNode(__RefManager_Get_Object(rm), fieldName, s, NODE_PREFIX | NODE_SUFFIX);
}

QueryNode* RediSearch_CreateSuffixNode(RefManager* rm, const char* fieldName, const char* s) {
  return RediSearch_CreateAffixNode(__RefManager_Get_Object(rm), fieldName, s, NODE_SUFFIX);
}

static QueryNode* RediSearch_CreateTagAffixNode(IndexSpec* sp, const char* s, int flags) {
  QueryNode* ret = NewQueryNode(QN_PREFIX);
  ret->pfx = (QueryPrefixNode){
    .tok = (RSToken){.str = (char*)rm_strdup(s), .len = strlen(s), .expanded = 0, .flags = 0},
    .prefix = flags & NODE_PREFIX,
    .suffix = flags & NODE_SUFFIX,
  };
  return ret;
}

QueryNode* RediSearch_CreateTagPrefixNode(RefManager* rm, const char* s) {
  return RediSearch_CreateTagAffixNode(__RefManager_Get_Object(rm), s, NODE_PREFIX);
}

QueryNode* RediSearch_CreateTagContainsNode(RefManager* rm, const char* s) {
  return RediSearch_CreateTagAffixNode(__RefManager_Get_Object(rm), s, NODE_PREFIX | NODE_SUFFIX);
}

QueryNode* RediSearch_CreateTagSuffixNode(RefManager* rm, const char* s) {
  return RediSearch_CreateTagAffixNode(__RefManager_Get_Object(rm), s, NODE_SUFFIX);
}

QueryNode* RediSearch_CreateLexRangeNode(RefManager* rm, const char* fieldName, const char* begin,
                                         const char* end, int includeBegin, int includeEnd) {
  QueryNode* ret = NewQueryNode(QN_LEXRANGE);
  if (begin) {
    ret->lxrng.begin = begin ? rm_strdup(begin) : NULL;
    ret->lxrng.includeBegin = includeBegin;
  }
  if (end) {
    ret->lxrng.end = end ? rm_strdup(end) : NULL;
    ret->lxrng.includeEnd = includeEnd;
  }
  if (fieldName) {
    ret->opts.fieldMask = IndexSpec_GetFieldBit(__RefManager_Get_Object(rm), fieldName, strlen(fieldName));
    const FieldSpec* fs = IndexSpec_GetFieldWithLength(__RefManager_Get_Object(rm), fieldName, strlen(fieldName));
    ret->opts.fieldIndex = fs ? fs->index : RS_INVALID_FIELD_INDEX;
  }
  return ret;
}

QueryNode* RediSearch_CreateTagLexRangeNode(RefManager* rm, const char* fieldName, const char* begin,
                                         const char* end, int includeBegin, int includeEnd) {
  QueryNode* ret = NewQueryNode(QN_LEXRANGE);
  if (begin) {
    ret->lxrng.begin = begin ? rm_strdup(begin) : NULL;
    ret->lxrng.includeBegin = includeBegin;
  }
  if (end) {
    ret->lxrng.end = end ? rm_strdup(end) : NULL;
    ret->lxrng.includeEnd = includeEnd;
  }
  if (fieldName) {
    ret->opts.fieldMask = IndexSpec_GetFieldBit(__RefManager_Get_Object(rm), fieldName, strlen(fieldName));
    const FieldSpec* fs = IndexSpec_GetFieldWithLength(__RefManager_Get_Object(rm), fieldName, strlen(fieldName));
    ret->opts.fieldIndex = fs ? fs->index : RS_INVALID_FIELD_INDEX;
  }
  return ret;
}

QueryNode* RediSearch_CreateTagNode(RefManager* rm, const char* field) {
  QueryNode* ret = NewQueryNode(QN_TAG);
  size_t len = strlen(field);
  ret->tag.fs = IndexSpec_GetFieldWithLength(__RefManager_Get_Object(rm), field, len);
  ret->opts.fieldMask = IndexSpec_GetFieldBit(__RefManager_Get_Object(rm), field, len);
  return ret;
}

QueryNode* RediSearch_CreateIntersectNode(RefManager* rm, int exact) {
  QueryNode* ret = NewQueryNode(QN_PHRASE);
  ret->pn.exact = exact;
  return ret;
}

QueryNode* RediSearch_CreateUnionNode(RefManager* rm) {
  return NewQueryNode(QN_UNION);
}

QueryNode* RediSearch_CreateEmptyNode(RefManager* rm) {
  return NewQueryNode(QN_NULL);
}

QueryNode* RediSearch_CreateNotNode(RefManager* rm) {
  return NewQueryNode(QN_NOT);
}

int RediSearch_QueryNodeGetFieldMask(QueryNode* qn) {
  return qn->opts.fieldMask;
}

void RediSearch_QueryNodeAddChild(QueryNode* parent, QueryNode* child) {
  QueryNode_AddChild(parent, child);
}

void RediSearch_QueryNodeClearChildren(QueryNode* qn) {
  QueryNode_ClearChildren(qn, 1);
}

QueryNode* RediSearch_QueryNodeGetChild(const QueryNode* qn, size_t ix) {
  return QueryNode_GetChild(qn, ix);
}

size_t RediSearch_QueryNodeNumChildren(const QueryNode* qn) {
  return QueryNode_NumChildren(qn);
}

typedef struct RS_ApiIter {
  QueryIterator* internal;
  RedisSearchCtx sctx;
  RSIndexResult* res;
  const RSDocumentMetadata* lastmd;
  ScoringFunctionArgs scargs;
  RSScoringFunction scorer;
  RSFreeFunction scorerFree;
  double minscore;  // Used for scoring
  QueryAST qast;    // Used for string queries..
  IndexSpec* sp;
} RS_ApiIter;

#define QUERY_INPUT_STRING 1
#define QUERY_INPUT_NODE 2

typedef struct {
  int qtype;
  union {
    struct {
      const char* qs;
      size_t n;
      unsigned int dialect;
    } s;
    QueryNode* qn;
  } u;
} QueryInput;

static RS_ApiIter* handleIterCommon(IndexSpec* sp, QueryInput* input,
                                    uint64_t timeout_ms, char** error) {
  /* Two-level locking scheme:
   * 1. RWLOCK (global) - protects access to all indexes, prevents index destruction
   * 2. sp->rwlock (per-spec) - protects this index's data structures (e.g., TTL table)
   * Both locks are acquired here and released in RediSearch_ResultsIteratorFree.
   * On error, cleanup is done here if iter->sp wasn't set, otherwise in Free. */
  RWLOCK_ACQUIRE_READ();
  pthread_rwlock_rdlock(&sp->rwlock);
  /* We might have multiple readers that reads from the index,
   * Avoid rehashing the terms dictionary */
  dictPauseRehashing(sp->keysDict);
  /* Also pause rehashing on the TTL table if it exists */
  if (sp->docs.ttl) {
    dictPauseRehashing(sp->docs.ttl);
  }

  RSSearchOptions options = {0};
  QueryError status = QueryError_Default();
  RSSearchOptions_Init(&options);
  if(sp->rule != NULL && sp->rule->lang_default != DEFAULT_LANGUAGE) {
    options.language = sp->rule->lang_default;
  }

  RS_ApiIter* it = rm_calloc(1, sizeof(*it));
  it->sctx = SEARCH_CTX_STATIC(NULL, sp);
  // Apply the per-query deadline (0 == no enforcement). The trie /
  // numeric / VecSim iterators consult sctx->time while reading.
  // Cast clamps any value beyond INT32_MAX to INT32_MAX, which the
  // SearchCtx_UpdateTime path already treats as "no real deadline".
  SearchCtx_UpdateTime(&it->sctx,
                       timeout_ms > INT32_MAX ? INT32_MAX : (int32_t)timeout_ms);

  if (input->qtype == QUERY_INPUT_STRING) {
    if (QAST_Parse(&it->qast, &it->sctx, &options, input->u.s.qs, input->u.s.n, input->u.s.dialect, &status) !=
        REDISMODULE_OK) {
      goto end;
    }
  } else {
    it->qast.root = input->u.qn;
  }

  // set queryAST configuration parameters
  iteratorsConfig_init(&it->qast.config);

  if (QAST_Expand(&it->qast, NULL, &options, &it->sctx, &status) != REDISMODULE_OK) {
    goto end;
  }

  it->internal = QAST_Iterate(&it->qast, &options, &it->sctx, 0, &status);
  RS_ASSERT(it->internal);

  IndexSpec_GetStats(sp, &it->scargs.indexStats);
  const char *defaultScorer = RSGlobalConfig.defaultScorer;
  RS_LOG_ASSERT(defaultScorer, "No default scorer");
  ExtScoringFunctionCtx* scoreCtx = Extensions_GetScoringFunction(&it->scargs, defaultScorer);
  RS_LOG_ASSERT(scoreCtx, "GetScoringFunction failed");
  it->scorer = scoreCtx->sf;
  it->scorerFree = scoreCtx->ff;
  it->minscore = DBL_MAX;
  it->sp = sp;

end:

  if (QueryError_HasError(&status)) {
    /* Resume rehashing and release locks only if iter->sp was not set,
     * since RediSearch_ResultsIteratorFree won't do it in that case.
     * If iter->sp is set, RediSearch_ResultsIteratorFree will handle cleanup. */
    if (!it->sp) {
      dictResumeRehashing(sp->keysDict);
      if (sp->docs.ttl) {
        dictResumeRehashing(sp->docs.ttl);
      }
      pthread_rwlock_unlock(&sp->rwlock);
    }
    RediSearch_ResultsIteratorFree(it);
    it = NULL;
    if (error) {
      *error = rm_strdup(QueryError_GetUserError(&status));
    }
  }
  QueryError_ClearError(&status);
  return it;
}

int RediSearch_DocumentExists(RefManager* rm, const void* docKey, size_t len) {
  IndexSpec* sp = __RefManager_Get_Object(rm);
  return DocTable_GetId(&sp->docs, docKey, len) != 0;
}

RS_ApiIter* RediSearch_IterateQuery(RefManager* rm, const char* s, size_t n, char** error) {
  QueryInput input = {.qtype = QUERY_INPUT_STRING, .u = {.s = {.qs = s, .n = n, .dialect = 1}}};
  return handleIterCommon(__RefManager_Get_Object(rm), &input, 0, error);
}

RS_ApiIter* RediSearch_IterateQueryWithTimeout(RefManager* rm, const char* s, size_t n,
                                               uint64_t timeout_ms, char** error) {
  QueryInput input = {.qtype = QUERY_INPUT_STRING, .u = {.s = {.qs = s, .n = n, .dialect = 1}}};
  return handleIterCommon(__RefManager_Get_Object(rm), &input, timeout_ms, error);
}

RS_ApiIter* RediSearch_IterateQueryWithDialect(RefManager* rm, const char* s, size_t n, unsigned int dialect, char** error) {
  QueryInput input = {.qtype = QUERY_INPUT_STRING, .u = {.s = {.qs = s, .n = n, .dialect = dialect}}};
  return handleIterCommon(__RefManager_Get_Object(rm), &input, 0, error);
}

RS_ApiIter* RediSearch_GetResultsIterator(QueryNode* qn, RefManager* rm) {
  QueryInput input = {.qtype = QUERY_INPUT_NODE, .u = {.qn = qn}};
  return handleIterCommon(__RefManager_Get_Object(rm), &input, 0, NULL);
}

RS_ApiIter* RediSearch_GetResultsIteratorWithTimeout(QueryNode* qn, RefManager* rm,
                                                    uint64_t timeout_ms) {
  QueryInput input = {.qtype = QUERY_INPUT_NODE, .u = {.qn = qn}};
  return handleIterCommon(__RefManager_Get_Object(rm), &input, timeout_ms, NULL);
}

void RediSearch_QueryNodeFree(QueryNode* qn) {
  QueryNode_Free(qn);
}

int RediSearch_QueryNodeType(QueryNode* qn) {
  return qn->type;
}

// use only by LLAPI + unittest
const void* RediSearch_ResultsIteratorNext(RS_ApiIter* iter, RefManager* rm, size_t* len) {
  IndexSpec *sp = __RefManager_Get_Object(rm);
  while (iter->internal->Read(iter->internal) == ITERATOR_OK) {
    iter->res = iter->internal->current;
    const RSDocumentMetadata* md = DocTable_Borrow(&sp->docs, iter->res->docId);
    if (md == NULL) {
      continue;
    }
    DMD_Return(iter->lastmd);
    iter->lastmd = md;
    if (len) {
      *len = sdslen(md->keyPtr);
    }
    return md->keyPtr;
  }
  return NULL;
}

double RediSearch_ResultsIteratorGetScore(const RS_ApiIter* it) {
  return it->scorer(&it->scargs, it->res, it->lastmd, 0);
}

void RediSearch_ResultsIteratorFree(RS_ApiIter* iter) {
  if (iter->internal) {
    iter->internal->Free(iter->internal);
  }
  if (iter->scorerFree) {
    iter->scorerFree(iter->scargs.extdata);
  }
  QAST_Destroy(&iter->qast);
  DMD_Return(iter->lastmd);
  /* Release locks only if iter->sp is set. On error paths in handleIterCommon,
   * if iter->sp is NULL, locks were already released there before calling this.
   * Lock release order: spec lock first, then global lock (reverse of acquisition). */
  if (iter->sp) {
    if (iter->sp->keysDict) {
      dictResumeRehashing(iter->sp->keysDict);
    }
    if (iter->sp->docs.ttl) {
      dictResumeRehashing(iter->sp->docs.ttl);
    }
    pthread_rwlock_unlock(&iter->sp->rwlock);
  }
  rm_free(iter);
  RWLOCK_RELEASE();
}

void RediSearch_ResultsIteratorReset(RS_ApiIter* iter) {
  iter->internal->Rewind(iter->internal);
}

RSIndexOptions* RediSearch_CreateIndexOptions() {
  RSIndexOptions* ret = rm_calloc(1, sizeof(RSIndexOptions));
  ret->gcPolicy = GC_POLICY_NONE;
  ret->stopwordsLen = -1;
  return ret;
}

void RediSearch_FreeIndexOptions(RSIndexOptions* options) {
  if (options->stopwordsLen > 0) {
    for (int i = 0; i < options->stopwordsLen; i++) {
      rm_free(options->stopwords[i]);
    }
    rm_free(options->stopwords);
  }
  rm_free(options);
}

void RediSearch_IndexOptionsSetGetValueCallback(RSIndexOptions* options, RSGetValueCallback cb,
                                                void* ctx) {
  options->gvcb = cb;
  options->gvcbData = ctx;
}

void RediSearch_IndexOptionsSetStopwords(RSIndexOptions* opts, const char **stopwords, int stopwordsLen) {
  if (opts->stopwordsLen > 0) {
    for (int i = 0; i < opts->stopwordsLen; i++) {
      rm_free(opts->stopwords[i]);
    }
    rm_free(opts->stopwords);
  }

  opts->stopwords = NULL;

  if (stopwordsLen > 0) {
    opts->stopwords = rm_malloc(sizeof(*opts->stopwords) * stopwordsLen);
    for (int i = 0; i < stopwordsLen; i++) {
      opts->stopwords[i] = rm_strdup(stopwords[i]);
    }
  }
  opts->stopwordsLen = stopwordsLen;
}

void RediSearch_IndexOptionsSetFlags(RSIndexOptions* options, uint32_t flags) {
  options->flags = flags;
}

void RediSearch_IndexOptionsSetGCPolicy(RSIndexOptions* options, int policy) {
  options->gcPolicy = policy;
}

int RediSearch_IndexOptionsSetScore(RSIndexOptions* options, double score) {
  if (score < 0 || score > 1) {
    return REDISEARCH_ERR;
  }
  options->score = score;
  return REDISEARCH_OK;
}

int RediSearch_IndexOptionsSetLanguage(RSIndexOptions* options, const char *lang) {
  if (!lang || RediSearch_ValidateLanguage(lang) != REDISEARCH_OK) {
    return REDISEARCH_ERR;
  }
  options->lang = lang;
  return REDISEARCH_OK;
}

#define REGISTER_API(name)                                                          \
  if (RedisModule_ExportSharedAPI(ctx, "RediSearch_" #name, RediSearch_##name) !=   \
      REDISMODULE_OK) {                                                             \
    RedisModule_Log(ctx, "warning", "could not register RediSearch_" #name "\r\n"); \
    return REDISMODULE_ERR;                                                         \
  }

int RediSearch_ExportCapi(RedisModuleCtx* ctx) {
  if (RedisModule_ExportSharedAPI == NULL) {
    RedisModule_Log(ctx, "warning", "Upgrade redis-server to use Redis Search's C API");
    return REDISMODULE_ERR;
  }
  RS_XAPIFUNC(REGISTER_API)
  return REDISMODULE_OK;
}

void RediSearch_SetCriteriaTesterThreshold(size_t num) {
}

const char *RediSearch_HiddenStringGet(const HiddenString* value) {
  return HiddenString_GetUnsafe(value, NULL);
}

int RediSearch_StopwordsList_Contains(RSIndex* idx, const char *term, size_t len) {
  IndexSpec *sp = __RefManager_Get_Object(idx);
  return StopWordList_Contains(sp->stopwords, term, len);
}

void RediSearch_FieldInfo(struct RSIdxField *infoField, FieldSpec *specField) {
  HiddenString_Clone(specField->fieldName, &infoField->name);
  HiddenString_Clone(specField->fieldPath, &infoField->path);
  if (specField->types & INDEXFLD_T_FULLTEXT) {
    infoField->types |= RSFLDTYPE_FULLTEXT;
    infoField->textWeight = specField->ftWeight;
  }
  if (specField->types & INDEXFLD_T_NUMERIC) {
    infoField->types |= RSFLDTYPE_NUMERIC;
  }
  if (specField->types & INDEXFLD_T_TAG) {
    infoField->types |= RSFLDTYPE_TAG;
    infoField->tagSeperator = specField->tagOpts.tagSep;
    infoField->tagCaseSensitive = specField->tagOpts.tagFlags & TagField_CaseSensitive ? 1 : 0;
  }
  if (specField->types & INDEXFLD_T_GEO) {
    infoField->types |= RSFLDTYPE_GEO;
  }
  // TODO: GEMOMETRY
  // if (specField->types & INDEXFLD_T_GEOMETRY) {
  //   infoField->types |= RSFLDTYPE_GEOMETRY;
  // }


  if (FieldSpec_IsSortable(specField)) {
    infoField->options |= RSFLDOPT_SORTABLE;
  }
  if (FieldSpec_IsNoStem(specField)) {
    infoField->options |= RSFLDOPT_TXTNOSTEM;
  }
  if (FieldSpec_IsPhonetics(specField)) {
    infoField->options |= RSFLDOPT_TXTPHONETIC;
  }
  if (!FieldSpec_IsIndexable(specField)) {
    infoField->options |= RSFLDOPT_NOINDEX;
  }
}

int RediSearch_IndexInfo(RSIndex* rm, RSIdxInfo *info) {
  if (info->version < RS_INFO_INIT_VERSION || info->version > RS_INFO_CURRENT_VERSION) {
    return REDISEARCH_ERR;
  }

  RWLOCK_ACQUIRE_READ();
  IndexSpec *sp = __RefManager_Get_Object(rm);
  /* Acquire spec read lock to synchronize with config changes that may destroy TTL table */
  pthread_rwlock_rdlock(&sp->rwlock);
  /* We might have multiple readers that reads from the index,
   * Avoid rehashing the terms dictionary */
  dictPauseRehashing(sp->keysDict);
  /* Also pause rehashing on the TTL table if it exists */
  if (sp->docs.ttl) {
    dictPauseRehashing(sp->docs.ttl);
  }

  info->gcPolicy = sp->gc ? GC_POLICY_FORK : GC_POLICY_NONE;
  if (sp->rule) {
    info->score = sp->rule->score_default;
    info->lang = RSLanguage_ToString(sp->rule->lang_default);
  } else {
    info->score = DEFAULT_SCORE;
    info->lang = RSLanguage_ToString(DEFAULT_LANGUAGE);
  }

  info->numFields = sp->numFields;
  info->fields = rm_calloc(sp->numFields, sizeof(*info->fields));
  for (int i = 0; i < info->numFields; ++i) {
    RediSearch_FieldInfo(&info->fields[i], &sp->fields[i]);
  }

  info->numDocuments = sp->stats.numDocuments;
  info->maxDocId = sp->docs.maxDocId;
  info->docTableSize = sp->docs.memsize;
  info->sortablesSize = sp->docs.sortablesSize;
  info->docTrieSize = TrieMap_MemUsage(sp->docs.dim.tm);
  info->numTerms = sp->stats.numTerms;
  info->numRecords = sp->stats.numRecords;
  info->invertedSize = sp->stats.invertedSize;
  info->invertedCap = 0;
  info->skipIndexesSize = 0;
  info->scoreIndexesSize = 0;
  info->offsetVecsSize = sp->stats.offsetVecsSize;
  info->offsetVecRecords = sp->stats.offsetVecRecords;
  info->termsSize = sp->stats.termsSize;
  info->indexingFailures = sp->stats.indexError.error_count;

  if (sp->gc) {
    // LLAPI always uses ForkGC
    ForkGCStats gcStats = ((ForkGC *)sp->gc->gcCtx)->stats;

    info->totalCollected = gcStats.totalCollected;
    info->numCycles = gcStats.numCycles;
    info->totalMSRun = gcStats.totalMSRun;
    info->lastRunTimeMs = gcStats.lastRunTimeMs;
  }

  dictResumeRehashing(sp->keysDict);
  /* Also resume rehashing on the TTL table if it exists */
  if (sp->docs.ttl) {
    dictResumeRehashing(sp->docs.ttl);
  }
  /* Release spec read lock */
  pthread_rwlock_unlock(&sp->rwlock);
  RWLOCK_RELEASE();

  return REDISEARCH_OK;
}

size_t RediSearch_MemUsage(RSIndex* rm) {
  IndexSpec *sp = __RefManager_Get_Object(rm);
  return IndexSpec_TotalMemUsage(sp, 0, 0, 0, 0);
}

// Collect statistics of all the currently existing indexes
TotalIndexesInfo RediSearch_TotalInfo(void) {
  return IndexesInfo_TotalInfo();
}

void RediSearch_IndexInfoFree(RSIdxInfo *info) {
  for (int i = 0; i < info->numFields; ++i) {
    HiddenString_Free(info->fields[i].name, true);
    HiddenString_Free(info->fields[i].path, true);
  }
  rm_free((void *)info->fields);
}
