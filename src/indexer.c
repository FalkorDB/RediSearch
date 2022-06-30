#include "indexer.h"
#include "forward_index.h"
#include "numeric_index.h"
#include "inverted_index.h"
#include "geo_index.h"
#include "index.h"
#include "redis_index.h"

#include "rmutil/rm_assert.h"

#include <unistd.h>

///////////////////////////////////////////////////////////////////////////////////////////////

static void Indexer_FreeInternal(DocumentIndexer *indexer);

static void writeIndexEntry(IndexSpec *spec, InvertedIndex *idx, IndexEncoder encoder,
                            const ForwardIndexEntry &entry) {
  size_t sz = idx->WriteForwardIndexEntry(encoder, entry);

  // Update index statistics:

  // Number of additional bytes
  spec->stats.invertedSize += sz;
  // Number of records
  spec->stats.numRecords++;

  /* Record the space saved for offset vectors */
  if (spec->flags & Index_StoreTermOffsets) {
    spec->stats.offsetVecsSize += entry.vw->GetByteLength();
    spec->stats.offsetVecRecords += entry.vw->GetCount();
  }
}

//---------------------------------------------------------------------------------------------

// Number of terms for each block-allocator block
#define TERMS_PER_BLOCK 128

// Effectively limits the maximum number of documents whose terms can be merged
#define MAX_BULK_DOCS 1024

// Entry for the merged dictionary
struct mergedEntry {
  KHTableEntry base;        // Base structure
  ForwardIndexEntry *head;  // First document containing the term
  ForwardIndexEntry *tail;  // Last document containing the term
};

//---------------------------------------------------------------------------------------------

// Boilerplate hashtable compare function
int MergeHashTable::Compare(const KHTableEntry *ent, const void *s, size_t n, uint32_t h) {
  mergedEntry *e = (mergedEntry *)ent;
  // 0 return value means "true"
  return !(e->head->hash == h && e->head->len == n && memcmp(e->head->term, s, n) == 0);
}

//---------------------------------------------------------------------------------------------

// Boilerplate hash retrieval function. Used for rebalancing the table
uint32_t MergeHashTable::Hash(const KHTableEntry *ent) {
  mergedEntry *e = (mergedEntry *)ent;
  return e->head->hash;
}

//---------------------------------------------------------------------------------------------

// Boilerplate dict entry allocator
KHTableEntry *MergeHashTable::Alloc(void *ctx) {
  BlkAlloc b = ctx;
  return b->Alloc(sizeof(mergedEntry), sizeof(mergedEntry) * TERMS_PER_BLOCK);
}

//---------------------------------------------------------------------------------------------

// This function used for debugging, and returns how many items are actually in the list
static size_t countMerged(mergedEntry *ent) {
  size_t n = 0;
  for (ForwardIndexEntry *cur = ent->head; cur; cur = cur->next) {
    n++;
  }
  return n;
}

//---------------------------------------------------------------------------------------------

// Merges all terms in the queue into a single hash table.
// parentMap is assumed to be a AddDocumentCtx*[] of capacity MAX_DOCID_ENTRIES
//
// This function returns the first aCtx which lacks its own document ID.
// This wil be used when actually assigning document IDs later on, so that we
// don't need to seek the document list again for it.

static AddDocumentCtx *doMerge(AddDocumentCtx *aCtx, KHTable *ht, AddDocumentCtx **parentMap) {
  // Counter is to make sure we don't block the CPU if there are many many items
  // in the queue, though in reality the number of iterations is also limited
  // by MAX_DOCID_ENTRIES
  size_t counter = 0;

  // Current index within the parentMap, this is assigned as the placeholder
  // doc ID value
  size_t curIdIdx = 0;

  AddDocumentCtx *cur = aCtx;
  AddDocumentCtx *firstZeroId = NULL;

  while (cur && ++counter < 1000 && curIdIdx < MAX_BULK_DOCS) {

    ForwardIndexIterator it = cur->fwIdx->Iterate();
    ForwardIndexEntry *entry = it.Next();

    while (entry) {
      // Because we don't have the actual document ID at this point, the document
      // ID field will be used here to point to an index in the parentMap
      // that will contain the parent. The parent itself will contain the
      // document ID when assigned (when the lock is held).
      entry->docId = curIdIdx;

      // Get the entry for it.
      int isNew = 0;
      mergedEntry *mergedEnt =
          (mergedEntry *)KHTable_GetEntry(ht, entry->term, entry->len, entry->hash, &isNew);

      if (isNew) {
        mergedEnt->head = mergedEnt->tail = entry;

      } else {
        mergedEnt->tail->next = entry;
        mergedEnt->tail = entry;
      }

      entry->next = NULL;
      entry = it.Next();
    }

    // Set the document's text status as indexed. This is not strictly true,
    // but it means that there is no more index interaction with this specific
    // document.
    cur->stateFlags |= ACTX_F_TEXTINDEXED;
    parentMap[curIdIdx++] = cur;
    if (firstZeroId == NULL && cur->doc.docId == 0) {
      firstZeroId = cur;
    }

    cur = cur->next;
  }
  return firstZeroId;
}

//---------------------------------------------------------------------------------------------

// Writes all the entries in the hash table to the inverted index.
// parentMap contains the actual mapping between the `docID` field and the actual
// AddDocumentCtx which contains the document itself, which by this time should
// have been assigned an ID via makeDocumentId()

int DocumentIndexer::writeMergedEntries(AddDocumentCtx *aCtx, RedisSearchCtx *ctx,
                                        KHTable *ht, AddDocumentCtx **parentMap) {

  IndexEncoder encoder = InvertedIndex::GetEncoder(ctx->spec->flags);
  const int isBlocked = aCtx->IsBlockable();

  // This is used as a cache layer, so that we don't need to derefernce the
  // AddDocumentCtx each time.
  uint32_t docIdMap[MAX_BULK_DOCS] = {0};

  // Iterate over all the entries
  for (uint32_t curBucketIdx = 0; curBucketIdx < ht->numBuckets; curBucketIdx++) {
    for (KHTableEntry *entp = ht->buckets[curBucketIdx]; entp; entp = entp->next) {
      mergedEntry *merged = (mergedEntry *)entp;

      // Open the inverted index:
      ForwardIndexEntry *fwent = merged->head;

      // Add the term to the prefix trie. This only needs to be done once per term
      ctx->spec->AddTerm(fwent->term, fwent->len);

      RedisModuleKey *idxKey = NULL;
      InvertedIndex *invidx = Redis_OpenInvertedIndexEx(ctx, fwent->term, fwent->len, 1, &idxKey);

      if (invidx == NULL) {
        continue;
      }

      for (; fwent != NULL; fwent = fwent->next) {
        // Get the Doc ID for this entry.
        // Note that we cache the lookup result itself, since accessing the
        // parent each time causes some memory access overhead. This saves
        // about 3% overall.
        uint32_t docId = docIdMap[fwent->docId];
        if (docId == 0) {
          // Meaning the entry is not yet in the cache.
          AddDocumentCtx *parent = parentMap[fwent->docId];
          if ((parent->stateFlags & ACTX_F_ERRORED) || parent->doc.docId == 0) {
            // Has an error, or for some reason it doesn't have a document ID(!? is this possible)
            continue;
          } else {
            // Place the entry in the cache, so we don't need a pointer dereference next time
            docId = docIdMap[fwent->docId] = parent->doc.docId;
          }
        }

        // Finally assign the document ID to the entry
        fwent->docId = docId;
        writeIndexEntry(ctx->spec, invidx, encoder, *fwent);
      }

      if (idxKey) {
        RedisModule_CloseKey(idxKey);
      }

      if (isBlocked && CONCURRENT_CTX_TICK(&concCtx) && ctx->spec == NULL) {
        aCtx->status.SetError(QUERY_ENOINDEX, NULL);
        return -1;
      }
    }
  }
  return 0;
}

//---------------------------------------------------------------------------------------------

// Simple implementation, writes all the entries for a single document.
// This function is used when there is only one item in the queue.
// In this case it's simpler to forego building the merged dictionary because there is
// nothing to merge.

void DocumentIndexer::writeCurEntries(AddDocumentCtx *aCtx, RedisSearchCtx *ctx) {
  RS_LOG_ASSERT(ctx, "ctx shound not be NULL");

  ForwardIndexIterator it = aCtx->fwIdx->Iterate();
  ForwardIndexEntry *entry = it.Next();
  IndexEncoder encoder = InvertedIndex::GetEncoder(aCtx->specFlags);
  const int isBlocked = aCtx->IsBlockable();

  while (entry != NULL) {
    RedisModuleKey *idxKey = NULL;
    ctx->spec->AddTerm(entry->term, entry->len);

    InvertedIndex *invidx = Redis_OpenInvertedIndexEx(ctx, entry->term, entry->len, 1, &idxKey);
    if (invidx) {
      entry->docId = aCtx->doc.docId;
      RS_LOG_ASSERT(entry->docId, "docId should not be 0");
      writeIndexEntry(ctx->spec, invidx, encoder, *entry);
    }
    if (idxKey) {
      RedisModule_CloseKey(idxKey);
    }

    entry = it.Next();
    if (isBlocked && CONCURRENT_CTX_TICK(&concCtx) && ctx->spec == NULL) {
      aCtx->status.SetError(QUERY_ENOINDEX, NULL);
      return;
    }
  }
}

//---------------------------------------------------------------------------------------------

static void handleReplaceDelete(RedisSearchCtx *sctx, t_docId did) {
  IndexSpec *sp = sctx->spec;
  for (size_t ii = 0; ii < sp->numFields; ++ii) {
    const FieldSpec *fs = sp->fields + ii;
    if (!fs->IsFieldType(INDEXFLD_T_GEO)) {
      continue;
    }
    // Open the key:
    RedisModuleString *fmtkey = sp->GetFormattedKey(fs, INDEXFLD_T_GEO);
    GeoIndex gi{sctx, fs};
    gi.RemoveEntries(did);
  }
}

//---------------------------------------------------------------------------------------------

// Assigns a document ID to a single document

static int makeDocumentId(AddDocumentCtx *aCtx, RedisSearchCtx *sctx, int replace,
                          QueryError *status) {
  IndexSpec *spec = sctx->spec;
  DocTable *table = &spec->docs;
  Document *doc = &aCtx->doc;
  if (replace) {
    RSDocumentMetadata *dmd = table->PopR(doc->docKey);
    if (dmd) {
      // decrease the number of documents in the index stats only if the document was there
      --spec->stats.numDocuments;
      aCtx->oldMd = dmd;
      if (dmd->flags & Document_HasOnDemandDeletable) {
        // Delete all on-demand fields.. this means geo,but could mean other things..
        handleReplaceDelete(sctx, dmd->id);
      }
      if (sctx->spec->gc) {
        sctx->spec->gc->OnDelete();
      }
    }
  }

  size_t n;
  const char *s = RedisModule_StringPtrLen(doc->docKey, &n);

  doc->docId =
    table->Put(s, n, doc->score, aCtx->docFlags, doc->payload, doc->payloadSize);
  if (doc->docId == 0) {
    status->SetError(QUERY_EDOCEXISTS, NULL);
    return -1;
  }
  ++spec->stats.numDocuments;

  return 0;
}

//---------------------------------------------------------------------------------------------

/**
 * Performs bulk document ID assignment to all items in the queue.
 * If one item cannot be assigned an ID, it is marked as being errored.
 *
 * This function also sets the document's sorting vector, if present.
 */
static void doAssignIds(AddDocumentCtx *cur, RedisSearchCtx *ctx) {
  IndexSpec *spec = ctx->spec;
  for (; cur; cur = cur->next) {
    if (cur->stateFlags & ACTX_F_ERRORED) {
      continue;
    }

    RS_LOG_ASSERT(!cur->doc.docId, "docId must be 0");
    int rv = makeDocumentId(cur, ctx, cur->options & DOCUMENT_ADD_REPLACE, &cur->status);
    if (rv != 0) {
      cur->stateFlags |= ACTX_F_ERRORED;
      continue;
    }

    RSDocumentMetadata *md = &spec->docs->Get(cur->doc.docId);
    md->maxFreq = cur->fwIdx->maxFreq;
    md->len = cur->fwIdx->totalFreq;

    if (cur->sv) {
      &spec->docs->SetSortingVector(cur->doc.docId, cur->sv);
      cur->sv = NULL;
    }

    if (cur->byteOffsets) {
      cur->offsetsWriter.Move(cur->byteOffsets);
      &spec->docs->SetByteOffsets(cur->doc.docId, cur->byteOffsets);
      cur->byteOffsets = NULL;
    }
  }
}

//---------------------------------------------------------------------------------------------

static void IndexBulkData::indexBulkFields(AddDocumentCtx *aCtx, RedisSearchCtx *sctx) {
  // Traverse all fields, seeing if there may be something which can be written!
  IndexBulkData bData[SPEC_MAX_FIELDS] = {{{NULL}}};
  IndexBulkData *activeBulks[SPEC_MAX_FIELDS];
  size_t numActiveBulks = 0;

  for (AddDocumentCtx *cur = aCtx; cur && cur->doc.docId; cur = cur->next) {
    if (cur->stateFlags & ACTX_F_ERRORED) {
      continue;
    }

    const Document *doc = &cur->doc;
    for (size_t ii = 0; ii < doc->numFields; ++ii) {
      const FieldSpec *fs = cur->fspecs + ii;
      FieldIndexerData *fdata = cur->fdatas + ii;
      if (fs->name == NULL || fs->types == INDEXFLD_T_FULLTEXT || !fs->IsIndexable()) {
        continue;
      }
      IndexBulkData *bulk = &bData[fs->index];
      if (!bulk->found) {
        bulk->found = 1;
        activeBulks[numActiveBulks++] = bulk;
      }

      if (IndexerBulkAdd(bulk, cur, sctx, doc->fields + ii, fs, fdata, &cur->status) != 0) {
        cur->stateFlags |= ACTX_F_ERRORED;
      }
      cur->stateFlags |= ACTX_F_OTHERINDEXED;
    }
  }

  // Flush it!
  for (size_t ii = 0; ii < numActiveBulks; ++ii) {
    IndexBulkData *cur = activeBulks[ii];
    IndexerBulkCleanup(cur, sctx);
  }
}

//---------------------------------------------------------------------------------------------

struct DocumentIndexerConcurrentKey : public ConcurrentKey {
  RedisSearchCtx sctx;

  DocumentIndexerConcurrentKey(RedisModuleKey *key, RedisModuleString *keyName) :
    ConcurrentKey(key, keyName, REDISMODULE_READ | REDISMODULE_WRITE) {
  }

  virtual void Reopen(RedisModuleKey *k) {
    // we do not allow empty indexes when loading an existing index
    if (k == NULL || RedisModule_KeyType(k) == REDISMODULE_KEYTYPE_EMPTY ||
        RedisModule_ModuleTypeGetType(k) != IndexSpecType) {
      sctx.spec = NULL;
      return;
    }

    sctx.spec = RedisModule_ModuleTypeGetValue(k);
    if (sctx.spec->uniqueId != sctx.specId) {
      sctx.spec = NULL;
    }
  }
};

//---------------------------------------------------------------------------------------------

// Routines for the merged hash table
#define ACTX_IS_INDEXED(actx)                                           \
  (((actx)->stateFlags & (ACTX_F_OTHERINDEXED | ACTX_F_TEXTINDEXED)) == \
   (ACTX_F_OTHERINDEXED | ACTX_F_TEXTINDEXED))

//---------------------------------------------------------------------------------------------

// Perform the processing chain on a single document entry, optionally merging
// the tokens of further entries in the queue

void DocumentIndexer::Process(AddDocumentCtx *aCtx) {
  AddDocumentCtx *parentMap[MAX_BULK_DOCS] = {0};
  AddDocumentCtx *firstZeroId = aCtx;

  if (ACTX_IS_INDEXED(aCtx) || aCtx->stateFlags & (ACTX_F_ERRORED)) {
    // Document is complete or errored. No need for further processing.
    if (!(aCtx->stateFlags & ACTX_F_EMPTY)) {
      return;
    }
  }

  bool useTermHt = size > 1 && !(aCtx->stateFlags & ACTX_F_TEXTINDEXED);
  if (useTermHt) {
    firstZeroId = doMerge(aCtx, &mergeHt, parentMap);
    if (firstZeroId && firstZeroId->stateFlags & ACTX_F_ERRORED) {
      // Don't treat an errored ctx as being the head of a new ID chain. It's
      // likely that subsequent entries do indeed have IDs.
      firstZeroId = NULL;
    }
  }

  const int isBlocked = aCtx->IsBlockable();

  RedisSearchCtx sctx;
  if (isBlocked) {
    // Force a context at this point:
    if (!isDbSelected) {
      RedisModuleCtx *thCtx = RedisModule_GetThreadSafeContext(aCtx->client.bc);
      RedisModule_SelectDb(redisCtx, RedisModule_GetSelectedDb(thCtx));
      RedisModule_FreeThreadSafeContext(thCtx);
      isDbSelected = true;
    }

    sctx.redisCtx = redisCtx;
    sctx.specId = specId;
    concCtx.SetKey(specKeyName, &sctx);
    concCtx.ResetClock();
    concCtx.Lock();
  } else {
    sctx = *aCtx->client.sctx;
  }

  if (!sctx.spec) {
    aCtx->status.SetCode(QUERY_ENOINDEX);
    aCtx->stateFlags |= ACTX_F_ERRORED;
    goto cleanup;
  }

  Document *doc = &aCtx->doc;

  /**
   * Document ID assignment:
   * In order to hold the GIL for as short a time as possible, we assign
   * document IDs in bulk. We begin using the first document ID that is assumed
   * to be zero.
   *
   * When merging multiple document IDs, the merge stage scans through the chain
   * of proposed documents and selects the first document in the chain missing an
   * ID - the subsequent documents should also all be missing IDs. If none of
   * the documents are missing IDs then the firstZeroId document is NULL and
   * no ID assignment takes place.
   *
   * Assigning IDs in bulk speeds up indexing of smaller documents by about 10% overall.
   */

  if (firstZeroId != NULL && firstZeroId->doc.docId == 0) {
    doAssignIds(firstZeroId, &sctx);
  }

  // Handle FULLTEXT indexes
  if (useTermHt) {
    writeMergedEntries(aCtx, &sctx, &mergeHt, parentMap);
  } else if (aCtx->fwIdx && !(aCtx->stateFlags & ACTX_F_ERRORED)) {
    writeCurEntries(aCtx, &sctx);
  }

  if (!(aCtx->stateFlags & ACTX_F_OTHERINDEXED)) {
    IndexBulkData::indexBulkFields(aCtx, &sctx);
  }

cleanup:
  if (isBlocked) {
    concCtx.Unlock();
  }
  if (useTermHt) {
    alloc.Clear(NULL, NULL, 0);
    mergeHt.Clear();
  }
}

//---------------------------------------------------------------------------------------------

void DocumentIndexer::main() {
  pthread_mutex_lock(&lock);
  while (!ShouldStop()) {
    while (head == NULL && !ShouldStop()) {
      pthread_cond_wait(&cond, &lock);
    }

    AddDocumentCtx *cur = head;
    if (cur == NULL) {
      RS_LOG_ASSERT(ShouldStop(), "indexer was stopped");
      pthread_mutex_unlock(&lock);
      break;
    }

    size--;

    if ((head = cur->next) == NULL) {
      tail = NULL;
    }
    pthread_mutex_unlock(&lock);
    Process(cur);
    cur->Finish();
    pthread_mutex_lock(&lock);
  }

  delete this;
}

//---------------------------------------------------------------------------------------------

static void *DocumentIndexer::_main(void *self_) {
  auto self = (DocumentIndexer *)self_;
  try {
    self->main();
  } catch (Error &x) {
    RS_LOG_ASSERT(0, "DocumentIndexer thread exception: %s", x.what());
  } catch (...) {
    RS_LOG_ASSERT(0, "DocumentIndexer thread exception");
  }
}

//---------------------------------------------------------------------------------------------

// Add a document to the indexing queue. If successful, the indexer now takes
// ownership of the document context (until it DocumentAddCtx_Finish).

int DocumentIndexer::Add(AddDocumentCtx *aCtx) {
  if (!aCtx->IsBlockable() || !!(options & INDEXER_THREADLESS)) {
    Process(aCtx);
    aCtx->Finish();
    return 0;
  }

  pthread_mutex_lock(&lock);

  if (tail) {
    tail->next = aCtx;
    tail = aCtx;
  } else {
    head = tail = aCtx;
  }

  pthread_cond_signal(&cond);
  pthread_mutex_unlock(&lock);

  size++;
  return 0;
}

///////////////////////////////////////////////////////////////////////////////////////////////
// Multiple Indexers

/**
 * Each index (i.e. IndexSpec) will have its own dedicated indexing thread.
 * This is because documents only need to be indexed in order with respect
 * to their document IDs, and the ID namespace is only unique among a given
 * index.
 *
 * Separating background threads also greatly simplifies the work of merging
 * or folding indexing and document ID assignment, as it can be assumed that
 * every item within the document ID belongs to the same index.
 */

// Creates a new DocumentIndexer. This initializes the structure and starts the
// thread. This does not insert it into the list of threads, though
// todo: remove the withIndexThread var once we switch to threadpool

DocumentIndexer::DocumentIndexer(IndexSpec *spec) {
  mergeHt = new MergeHashTable(alloc, 4096);
  size = 0;
  isDbSelected = false;
  refcount = 1;
  head = tail = NULL;
  options = 0;
  if (!!(spec->flags & Index_Temporary) || !RSGlobalConfig.concurrentMode) {
    options |= INDEXER_THREADLESS;
  }

  if (!(options & INDEXER_THREADLESS)) {
    pthread_cond_init(&cond, NULL);
    pthread_mutex_init(&lock, NULL);
    pthread_create(&thr, NULL, _main, this);
    pthread_detach(thr);
  }

  next = NULL;
  redisCtx = RedisModule_GetThreadSafeContext(NULL);
  specId = spec->uniqueId;
  specKeyName = RedisModule_CreateStringPrintf(redisCtx, INDEX_SPEC_KEY_FMT, spec->name);

  concCtx = *new ConcurrentSearchCtx(redisCtx, REDISMODULE_READ | REDISMODULE_WRITE);
}

//---------------------------------------------------------------------------------------------

DocumentIndexer::~DocumentIndexer() {
  if (!(options & INDEXER_THREADLESS)) {
    pthread_cond_destroy(&cond);
    pthread_mutex_destroy(&lock);
  }
  delete &concCtx;
  RedisModule_FreeString(redisCtx, specKeyName);
  mergeHt.Clear();
  // KHTable_Free(&mergeHt);
  alloc.FreeAll(NULL, 0, 0);
  RedisModule_FreeThreadSafeContext(redisCtx);
}

//---------------------------------------------------------------------------------------------

size_t DocumentIndexer::Decref() {
  size_t ret = __sync_sub_and_fetch(&refcount, 1);
  if (!ret) {
    pthread_mutex_lock(&lock);
    options |= INDEXER_STOPPED;
    pthread_cond_signal(&cond);
    pthread_mutex_unlock(&lock);
  }
  return ret;
}

//---------------------------------------------------------------------------------------------

size_t DocumentIndexer::Incref() {
  return ++refcount;
}

//---------------------------------------------------------------------------------------------

void DocumentIndexer::Free() {
  if (options & INDEXER_THREADLESS) {
    delete this;
  } else {
    Decref();
  }
}

///////////////////////////////////////////////////////////////////////////////////////////////
