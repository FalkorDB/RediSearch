
#include "geo_index.h"
#include "index.h"
#include "query.h"
#include "config.h"
#include "query_parser/parser.h"
#include "redis_index.h"
#include "tokenize.h"
#include "tag_index.h"
#include "err.h"
#include "concurrent_ctx.h"
#include "numeric_index.h"
#include "numeric_filter.h"
#include "module.h"

#include "extension.h"
#include "ext/default.h"

#include "rmutil/sds.h"

#include "util/logging.h"
#include "util/strconv.h"
#include "util/arr.h"
#include "rmutil/rm_assert.h"

#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/param.h>

///////////////////////////////////////////////////////////////////////////////////////////////

#define EFFECTIVE_FIELDMASK(q_, qn_) ((qn_)->opts.fieldMask & (q)->opts->fieldmask)

//---------------------------------------------------------------------------------------------

QueryTokenNode:~QueryTokenNode() {
  if (str) rm_free(str);
}

//---------------------------------------------------------------------------------------------

QueryTagNode::~QueryTagNode() {
  rm_free((char *)fieldName);
}

//---------------------------------------------------------------------------------------------

QueryLexRangeNode::~QueryLexRangeNode() {
  if (begin) rm_free(begin);
  if (end) rm_free(end);
}

//---------------------------------------------------------------------------------------------

// void _queryNumericNode_Free(QueryNumericNode *nn) { free(nn->nf); }

QueryNode::~QueryNode() {
  if (children) {
    for (size_t ii = 0; ii < NumChildren(); ++ii) {
      delete children[ii];
    }
    array_free(children);
    children = NULL;
  }

  switch (type) {
    case QN_TOKEN:
      delete &tn;
      break;
    case QN_NUMERIC:
      NumericFilter_Free((void *)nn.nf);

      break;  //
    case QN_PREFX:
      deletet &pfx;
      break;
    case QN_GEO:
      if (gn.gf) {
        GeoFilter_Free((void *)gn.gf);
      }
      break;
    case QN_FUZZY:
      delete &fz.tok;
      break;
    case QN_LEXRANGE:
      delete &lxrng;
      break;
    case QN_WILDCARD:
    case QN_IDS:
      break;

    case QN_TAG:
      deletet &tag;
      break;

    case QN_UNION:
    case QN_NOT:
    case QN_OPTIONAL:
    case QN_NULL:
    case QN_PHRASE:
      break;
  }
}

//---------------------------------------------------------------------------------------------

QueryNode::ctor(QueryNodeType type_) {
  type = type_;
  opts = (QueryNodeOptions){
      .fieldMask = RS_FIELDMASK_ALL,
      .flags = 0,
      .maxSlop = -1,
      .inOrder = 0,
      .weight = 1,
  };
}

//---------------------------------------------------------------------------------------------

QueryNode *QueryAST::NewTokenNodeExpanded(const char *s, size_t len, RSTokenFlags flags) {
  QueryNode *ret = new QueryNode(QN_TOKEN);
  numTokens++;
  ret->tn = (QueryTokenNode){.str = (char *)s, .len = len, .expanded = 1, .flags = flags};
  return ret;
}

//---------------------------------------------------------------------------------------------

QueryNode *NewTokenNode(QueryParse *q, const char *s, size_t len) {
  if (len == (size_t)-1) {
    len = strlen(s);
  }

  QueryNode *ret = new QueryNode(QN_TOKEN);
  q->numTokens++;

  ret->tn = (QueryTokenNode){.str = (char *)s, .len = len, .expanded = 0, .flags = 0};
  return ret;
}

//---------------------------------------------------------------------------------------------

QueryNode *NewPrefixNode(QueryParse *q, const char *s, size_t len) {
  QueryNode *ret = new QueryNode(QN_PREFX);
  q->numTokens++;

  ret->pfx = (QueryPrefixNode){.str = (char *)s, .len = len, .expanded = 0, .flags = 0};
  return ret;
}

//---------------------------------------------------------------------------------------------

QueryNode *NewFuzzyNode(QueryParse *q, const char *s, size_t len, int maxDist) {
  QueryNode *ret = new QueryNode(QN_FUZZY);
  q->numTokens++;

  ret->fz = (QueryFuzzyNode){
      .tok =
          (RSToken){
              .str = (char *)s,
              .len = len,
              .expanded = 0,
              .flags = 0,
          },
      .maxDist = maxDist,
  };
  return ret;
}

//---------------------------------------------------------------------------------------------

QueryNode *NewPhraseNode(int exact) {
  QueryNode *ret = new QueryNode(QN_PHRASE);
  ret->pn.exact = exact;
  return ret;
}

//---------------------------------------------------------------------------------------------

QueryNode *NewTagNode(const char *field, size_t len) {

  QueryNode *ret = new QueryNode(QN_TAG);
  ret->tag.fieldName = field;
  ret->tag.len = len;
  return ret;
}

//---------------------------------------------------------------------------------------------

QueryNode *NewNumericNode(const NumericFilter *flt) {
  QueryNode *ret = new QueryNode(QN_NUMERIC);
  ret->nn = (QueryNumericNode){.nf = (NumericFilter *)flt};

  return ret;
}

//---------------------------------------------------------------------------------------------

QueryNode *NewGeofilterNode(const GeoFilter *flt) {
  QueryNode *ret = new QueryNode(QN_GEO);
  ret->gn.gf = flt;
  return ret;
}

//---------------------------------------------------------------------------------------------

void QueryAST::setFilterNode(QueryNode *n) {
  if (root == NULL || n == NULL) return;

  // for a simple phrase node we just add the numeric node
  if (root->type == QN_PHRASE) {
    // we usually want the numeric range as the "leader" iterator.
    root->children = array_ensure_prepend(root->children, &n, 1, QueryNode *);
    numTokens++;
  } else {  // for other types, we need to create a new phrase node
    QueryNode *nr = NewPhraseNode(0);
    nr->AddChild(n);
    nr->AddChild(root);
    numTokens++;
    root = nr;
  }
}

//---------------------------------------------------------------------------------------------

// Used only to support legacy FILTER keyword. Should not be used by newer code
void QueryAST::SetGlobalFilters(const NumericFilter *numeric) {
  QueryNode *n = new QueryNode(QN_NUMERIC);
  n->nn.nf = (NumericFilter *)options.numeric;
  setFilterNode(n);
}

// Used only to support legacy GEOFILTER keyword. Should not be used by newer code
void QueryAST::SetGlobalFilters(const GeoFilter *geo) {
  QueryNode *n = new QueryNode(QN_GEO);
  n->gn.gf = options.geo;
  setFilterNode(n);
}

// List of IDs to limit to, and the length of that array
void QueryAST::SetGlobalFilters(t_docId *ids, size_t nids) {
  QueryNode *n = new QueryNode(QN_IDS);
  n->fn.ids = options.ids;
  n->fn.len = options.nids;
  setFilterNode(n);
}

//---------------------------------------------------------------------------------------------

static void QueryNode_Expand(RSQueryTokenExpander expander, RSQueryExpanderCtx *expCtx,
                             QueryNode **pqn) {

  QueryNode *qn = *pqn;
  // Do not expand verbatim nodes
  if (qn->opts.flags & QueryNode_Verbatim) {
    return;
  }

  int expandChildren = 0;

  if (qn->type == QN_TOKEN) {
    expCtx->currentNode = pqn;
    expander(expCtx, &qn->tn);
  } else if (qn->type == QN_UNION ||
             (qn->type == QN_PHRASE && !qn->pn.exact)) {  // do not expand exact phrases
    expandChildren = 1;
  }
  if (expandChildren) {
    for (size_t ii = 0; ii < qn->NumChildren(); ++ii) {
      QueryNode_Expand(expander, expCtx, &qn->children[ii]);
    }
  }
}

//---------------------------------------------------------------------------------------------

IndexIterator *Query_EvalTokenNode(QueryEvalCtx *q, QueryNode *qn) {
  if (qn->type != QN_TOKEN) {
    return NULL;
  }

  // if there's only one word in the query and no special field filtering,
  // and we are not paging beyond MAX_SCOREINDEX_SIZE
  // we can just use the optimized score index
  int isSingleWord = q->numTokens == 1 && q->opts->fieldmask == RS_FIELDMASK_ALL;

  RSQueryTerm *term = new QueryTerm(&qn->tn, q->tokenId++);

  // printf("Opening reader.. `%s` FieldMask: %llx\n", term->str, EFFECTIVE_FIELDMASK(q, qn));

  IndexReader *ir = Redis_OpenReader(q->sctx, term, q->docTable, isSingleWord,
                                     EFFECTIVE_FIELDMASK(q, qn), q->conc, qn->opts.weight);
  if (ir == NULL) {
    Term_Free(term);
    return NULL;
  }

  return ir->NewReadIterator();
}

//---------------------------------------------------------------------------------------------

static IndexIterator *iterateExpandedTerms(QueryEvalCtx *q, Trie *terms, const char *str,
                                           size_t len, int maxDist, int prefixMode,
                                           QueryNodeOptions *opts) {
  TrieIterator *it = terms->Iterate(str, len, maxDist, prefixMode);
  if (!it) return NULL;

  size_t itsSz = 0, itsCap = 8;
  IndexIterator **its = rm_calloc(itsCap, sizeof(*its));

  rune *rstr = NULL;
  t_len slen = 0;
  float score = 0;
  int dist = 0;

  // an upper limit on the number of expansions is enforced to avoid stuff like "*"
  size_t maxExpansions = q->sctx->spec->maxPrefixExpansions;
  while (it->Next(&rstr, &slen, NULL, &score, &dist) &&
         (itsSz < maxExpansions || maxExpansions == -1)) {

    // Create a token for the reader
    RSToken tok = (RSToken){
        .expanded = 0,
        .flags = 0,
        .len = 0,
    };
    tok.str = runesToStr(rstr, slen, &tok.len);
    if (q->sctx && q->sctx->redisCtx) {
      RedisModule_Log(q->sctx->redisCtx, "debug", "Found fuzzy expansion: %s %f", tok.str, score);
    }

    RSQueryTerm *term = new QueryTerm(&tok, q->tokenId++);

    // Open an index reader
    IndexReader *ir = Redis_OpenReader(q->sctx, term, &q->sctx->spec->docs, 0,
                                       q->opts->fieldmask & opts->fieldMask, q->conc, 1);

    rm_free(tok.str);
    if (!ir) {
      Term_Free(term);
      continue;
    }

    // Add the reader to the iterator array
    its[itsSz++] = NewReadIterator(ir);
    if (itsSz == itsCap) {
      itsCap *= 2;
      its = rm_realloc(its, itsCap * sizeof(*its));
    }
  }

  DFAFilter_Free(it->ctx);
  rm_free(it->ctx);
  // printf("Expanded %d terms!\n", itsSz);
  if (itsSz == 0) {
    rm_free(its);
    return NULL;
  }
  return new UnionIterator(its, itsSz, q->docTable, 1, opts->weight);
}

//---------------------------------------------------------------------------------------------

/* Ealuate a prefix node by expanding all its possible matches and creating one big UNION on all
 * of them */
static IndexIterator *Query_EvalPrefixNode(QueryEvalCtx *q, QueryNode *qn) {
  RS_LOG_ASSERT(qn->type == QN_PREFX, "query node type should be prefix");

  // we allow a minimum of 2 letters in the prefx by default (configurable)
  if (qn->pfx.len < RSGlobalConfig.minTermPrefix) {
    return NULL;
  }
  Trie *terms = q->sctx->spec->terms;

  if (!terms) return NULL;

  return iterateExpandedTerms(q, terms, qn->pfx.str, qn->pfx.len, 0, 1, &qn->opts);
}

//---------------------------------------------------------------------------------------------

struct LexRangeCtx {
  IndexIterator **its;
  size_t nits;
  size_t cap;
  QueryEvalCtx *q;
  QueryNodeOptions *opts;
  double weight;
};

static void rangeItersAddIterator(LexRangeCtx *ctx, IndexReader *ir) {
  ctx->its[ctx->nits++] = ir->NewReadIterator();
  if (ctx->nits == ctx->cap) {
    ctx->cap *= 2;
    ctx->its = rm_realloc(ctx->its, ctx->cap * sizeof(*ctx->its));
  }
}

//---------------------------------------------------------------------------------------------

static void rangeIterCbStrs(const char *r, size_t n, void *p, void *invidx) {
  LexRangeCtx *ctx = p;
  QueryEvalCtx *q = ctx->q;
  RSToken tok = {0};
  tok.str = (char *)r;
  tok.len = n;
  RSQueryTerm *term = new QueryTerm(&tok, ctx->q->tokenId++);
  IndexReader *ir = new TermIndexReader(invidx, q->sctx->spec, RS_FIELDMASK_ALL, term, ctx->weight);
  if (!ir) {
    Term_Free(term);
    return;
  }

  rangeItersAddIterator(ctx, ir);
}

//---------------------------------------------------------------------------------------------

static void rangeIterCb(const rune *r, size_t n, void *p) {
  LexRangeCtx *ctx = p;
  QueryEvalCtx *q = ctx->q;
  RSToken tok = {0};
  tok.str = runesToStr(r, n, &tok.len);
  RSQueryTerm *term = new QueryTerm(&tok, ctx->q->tokenId++);
  IndexReader *ir = Redis_OpenReader(q->sctx, term, &q->sctx->spec->docs, 0,
                                     q->opts->fieldmask & ctx->opts->fieldMask, q->conc, 1);
  rm_free(tok.str);
  if (!ir) {
    Term_Free(term);
    return;
  }

  rangeItersAddIterator(ctx, ir);
}

//---------------------------------------------------------------------------------------------

static IndexIterator *Query_EvalLexRangeNode(QueryEvalCtx *q, QueryNode *lx) {
  Trie *t = q->sctx->spec->terms;
  LexRangeCtx ctx = {.q = q, .opts = &lx->opts};

  if (!t) {
    return NULL;
  }

  ctx.cap = 8;
  ctx.its = rm_malloc(sizeof(*ctx.its) * ctx.cap);
  ctx.nits = 0;

  rune *begin = NULL, *end = NULL;
  size_t nbegin, nend;
  if (lx->lxrng.begin) {
    begin = strToFoldedRunes(lx->lxrng.begin, &nbegin);
  }
  if (lx->lxrng.end) {
    end = strToFoldedRunes(lx->lxrng.end, &nend);
  }

  t->root->IterateRange(begin, begin ? nbegin : -1, lx->lxrng.includeBegin, end,
                        end ? nend : -1, lx->lxrng.includeEnd, rangeIterCb, &ctx);
  if (!ctx.its || ctx.nits == 0) {
    rm_free(ctx.its);
    return NULL;
  } else {
    return new UnionIterator(ctx.its, ctx.nits, q->docTable, 1, lx->opts.weight);
  }
}

//---------------------------------------------------------------------------------------------

static IndexIterator *Query_EvalFuzzyNode(QueryEvalCtx *q, QueryNode *qn) {
  RS_LOG_ASSERT(qn->type == QN_FUZZY, "query node type should be fuzzy");

  Trie *terms = q->sctx->spec->terms;

  if (!terms) return NULL;

  return iterateExpandedTerms(q, terms, qn->pfx.str, qn->pfx.len, qn->fz.maxDist, 0, &qn->opts);
}

//---------------------------------------------------------------------------------------------

static IndexIterator *Query_EvalPhraseNode(QueryEvalCtx *q, QueryNode *qn) {
  if (qn->type != QN_PHRASE) {
    // printf("Not a phrase node!\n");
    return NULL;
  }
  QueryPhraseNode *node = &qn->pn;
  // an intersect stage with one child is the same as the child, so we just
  // return it
  if (qn->NumChildren() == 1) {
    qn->children[0]->opts.fieldMask &= qn->opts.fieldMask;
    return Query_EvalNode(q, qn->children[0]);
  }

  // recursively eval the children
  IndexIterator **iters = rm_calloc(qn->NumChildren(), sizeof(IndexIterator *));
  for (size_t ii = 0; ii < qn->NumChildren(); ++ii) {
    qn->children[ii]->opts.fieldMask &= qn->opts.fieldMask;
    iters[ii] = Query_EvalNode(q, qn->children[ii]);
  }
  IndexIterator *ret;

  if (node->exact) {
    ret = NewIntersecIterator(iters, qn->NumChildren(), q->docTable,
                              EFFECTIVE_FIELDMASK(q, qn), 0, 1, qn->opts.weight);
  } else {
    // Let the query node override the slop/order parameters
    int slop = qn->opts.maxSlop;
    if (slop == -1) slop = q->opts->slop;

    // Let the query node override the inorder of the whole query
    int inOrder = q->opts->flags & Search_InOrder;
    if (qn->opts.inOrder) inOrder = 1;

    // If in order was specified and not slop, set slop to maximum possible value.
    // Otherwise we can't check if the results are in order
    if (inOrder && slop == -1) {
      slop = __INT_MAX__;
    }

    ret = NewIntersecIterator(iters, qn->NumChildren(), q->docTable,
                              EFFECTIVE_FIELDMASK(q, qn), slop, inOrder, qn->opts.weight);
  }
  return ret;
}

static IndexIterator *Query_EvalWildcardNode(QueryEvalCtx *q, QueryNode *qn) {
  if (qn->type != QN_WILDCARD || !q->docTable) {
    return NULL;
  }

  return NewWildcardIterator(q->docTable->maxDocId);
}

//---------------------------------------------------------------------------------------------

static IndexIterator *Query_EvalNotNode(QueryEvalCtx *q, QueryNode *qn) {
  if (qn->type != QN_NOT) {
    return NULL;
  }
  QueryNotNode *node = &qn->inverted;

  return NewNotIterator(qn->NumChildren() ? Query_EvalNode(q, qn->children[0]) : NULL,
                        q->docTable->maxDocId, qn->opts.weight);
}

//---------------------------------------------------------------------------------------------

static IndexIterator *Query_EvalOptionalNode(QueryEvalCtx *q, QueryNode *qn) {
  if (qn->type != QN_OPTIONAL) {
    return NULL;
  }
  QueryOptionalNode *node = &qn->opt;

  return NewOptionalIterator(qn->NumChildren() ? Query_EvalNode(q, qn->children[0]) : NULL,
                             q->docTable->maxDocId, qn->opts.weight);
}

//---------------------------------------------------------------------------------------------

static IndexIterator *Query_EvalNumericNode(QueryEvalCtx *q, QueryNumericNode *node) {

  const FieldSpec *fs =
      q->sctx->spec->GetField(node->nf->fieldName, strlen(node->nf->fieldName));
  if (!fs || !fs->IsFieldType(INDEXFLD_T_NUMERIC)) {
    return NULL;
  }

  return NewNumericFilterIterator(q->sctx, node->nf, q->conc);
}

//---------------------------------------------------------------------------------------------

static IndexIterator *Query_EvalGeofilterNode(QueryEvalCtx *q, QueryGeofilterNode *node,
                                              double weight) {

  const FieldSpec *fs =
      q->sctx->spec->GetField(node->gf->property, strlen(node->gf->property));
  if (!fs || !fs->IsFieldType(INDEXFLD_T_GEO)) {
    return NULL;
  }

  GeoIndex gi = {.ctx = q->sctx, .sp = fs};
  return NewGeoRangeIterator(&gi, node->gf, weight);
}

//---------------------------------------------------------------------------------------------

static IndexIterator *Query_EvalIdFilterNode(QueryEvalCtx *q, QueryIdFilterNode *node) {
  return NewIdListIterator(node->ids, node->len, 1);
}

//---------------------------------------------------------------------------------------------

static IndexIterator *Query_EvalUnionNode(QueryEvalCtx *q, QueryNode *qn) {
  if (qn->type != QN_UNION) {
    return NULL;
  }

  // a union stage with one child is the same as the child, so we just return it
  if (qn->NumChildren() == 1) {
    return Query_EvalNode(q, qn->children[0]);
  }

  // recursively eval the children
  IndexIterator **iters = rm_calloc(qn->NumChildren(), sizeof(IndexIterator *));
  int n = 0;
  for (size_t i = 0; i < qn->NumChildren(); ++i) {
    qn->children[i]->opts.fieldMask &= qn->opts.fieldMask;
    IndexIterator *it = Query_EvalNode(q, qn->children[i]);
    if (it) {
      iters[n++] = it;
    }
  }
  if (n == 0) {
    return NULL;
  }

  if (n == 1) {
    IndexIterator *ret = iters[0];
    return ret;
  }

  return new UnionIterator(iters, n, q->docTable, 0, qn->opts.weight);
}

//---------------------------------------------------------------------------------------------

typedef IndexIterator **IndexIteratorArray;

static IndexIterator *Query_EvalTagLexRangeNode(QueryEvalCtx *q, TagIndex *idx, QueryNode *qn,
                                                IndexIteratorArray *iterout, double weight) {
  TrieMap *t = idx->values;
  LexRangeCtx ctx = {.q = q, .opts = &qn->opts, .weight = weight};

  if (!t) {
    return NULL;
  }

  ctx.cap = 8;
  ctx.its = rm_malloc(sizeof(*ctx.its) * ctx.cap);
  ctx.nits = 0;

  const char *begin = qn->lxrng.begin, *end = qn->lxrng.end;
  int nbegin = begin ? strlen(begin) : -1, nend = end ? strlen(end) : -1;

  t->IterateRange(begin, nbegin, qn->lxrng.includeBegin, end, nend, qn->lxrng.includeEnd,
                  rangeIterCbStrs, &ctx);
  if (ctx.nits == 0) {
    rm_free(ctx.its);
    return NULL;
  } else {
    return new UnionIterator(ctx.its, ctx.nits, q->docTable, 1, qn->opts.weight);
  }
}

//---------------------------------------------------------------------------------------------

/* Evaluate a tag prefix by expanding it with a lookup on the tag index */
static IndexIterator *Query_EvalTagPrefixNode(QueryEvalCtx *q, TagIndex *idx, QueryNode *qn,
                                              IndexIteratorArray *iterout, double weight) {
  if (qn->type != QN_PREFX) {
    return NULL;
  }

  // we allow a minimum of 2 letters in the prefx by default (configurable)
  if (qn->pfx.len < q->sctx->spec->minPrefix) {
    return NULL;
  }
  if (!idx || !idx->values) return NULL;

  TrieMapIterator *it = idx->values->Iterate(qn->pfx.str, qn->pfx.len);
  if (!it) return NULL;

  size_t itsSz = 0, itsCap = 8;
  IndexIterator **its = rm_calloc(itsCap, sizeof(*its));

  // an upper limit on the number of expansions is enforced to avoid stuff like "*"
  char *s;
  tm_len_t sl;
  void *ptr;

  // Find all completions of the prefix
  size_t maxExpansions = q->sctx->spec->maxPrefixExpansions;
  while (it->Next(&s, &sl, &ptr) &&
         (itsSz < maxExpansions || maxExpansions == -1)) {
    IndexIterator *ret = idx->OpenReader(q->sctx->spec, s, sl, 1);
    if (!ret) continue;

    // Add the reader to the iterator array
    its[itsSz++] = ret;
    if (itsSz == itsCap) {
      itsCap *= 2;
      its = rm_realloc(its, itsCap * sizeof(*its));
    }
  }

  // printf("Expanded %d terms!\n", itsSz);
  if (itsSz == 0) {
    rm_free(its);
    return NULL;
  }

  *iterout = array_ensure_append(*iterout, its, itsSz, IndexIterator *);
  return new UnionIterator(its, itsSz, q->docTable, 1, weight);
}

//---------------------------------------------------------------------------------------------

static IndexIterator *query_EvalSingleTagNode(QueryEvalCtx *q, TagIndex *idx, QueryNode *n,
                                              IndexIteratorArray *iterout, double weight) {
  IndexIterator *ret = NULL;
  switch (n->type) {
    case QN_TOKEN: {
      ret = idx->OpenReader(q->sctx->spec, n->tn.str, n->tn.len, weight);
      break;
    }
    case QN_PREFX:
      return Query_EvalTagPrefixNode(q, idx, n, iterout, weight);

    case QN_LEXRANGE:
      return Query_EvalTagLexRangeNode(q, idx, n, iterout, weight);

    case QN_PHRASE: {
      char *terms[n->NumChildren()];
      for (size_t i = 0; i < n->NumChildren(); ++i) {
        if (n->children[i]->type == QN_TOKEN) {
          terms[i] = n->children[i]->tn.str;
        } else {
          terms[i] = "";
        }
      }

      sds s = sdsjoin(terms, n->NumChildren(), " ");

      ret = idx->OpenReader(q->sctx->spec, s, sdslen(s), weight);
      sdsfree(s);
      break;
    }

    default:
      return NULL;
  }

  if (ret) {
    *array_ensure_tail(iterout, IndexIterator *) = ret;
  }
  return ret;
}

//---------------------------------------------------------------------------------------------

static IndexIterator *Query_EvalTagNode(QueryEvalCtx *q, QueryNode *qn) {
  if (qn->type != QN_TAG) {
    return NULL;
  }
  QueryTagNode *node = &qn->tag;
  RedisModuleKey *k = NULL;
  const FieldSpec *fs =
      q->sctx->spec->GetFieldCase(node->fieldName, strlen(node->fieldName));
  if (!fs) {
    return NULL;
  }
  RedisModuleString *kstr = q->sctx->spec->GetFormattedKey(fs, INDEXFLD_T_TAG);
  TagIndex *idx = q->sctxOpen(kstr, 0, &k);

  IndexIterator **total_its = NULL;
  IndexIterator *ret = NULL;

  if (!idx) {
    goto done;
  }
  // a union stage with one child is the same as the child, so we just return it
  if (qn->NumChildren() == 1) {
    ret = query_EvalSingleTagNode(q, idx, qn->children[0], &total_its, qn->opts.weight);
    if (ret) {
      if (q->conc) {
        idx->RegisterConcurrentIterators(q->conc, k, kstr, (array_t *)total_its);
        k = NULL;  // we passed ownershit
      } else {
        array_free(total_its);
      }
    }
    goto done;
  }

  // recursively eval the children
  IndexIterator **iters = rm_calloc(qn->NumChildren(), sizeof(IndexIterator *));
  size_t n = 0;
  for (size_t i = 0; i < qn->NumChildren(); i++) {
    IndexIterator *it =
        query_EvalSingleTagNode(q, idx, qn->children[i], &total_its, qn->opts.weight);
    if (it) {
      iters[n++] = it;
    }
  }
  if (n == 0) {
    goto done;
  }

  if (total_its) {
    if (q->conc) {
      idx->RegisterConcurrentIterators(q->conc, k, kstr, (array_t *)total_its);
      k = NULL;  // we passed ownershit
    } else {
      array_free(total_its);
    }
  }

  ret = new UnionIterator(iters, n, q->docTable, 0, qn->opts.weight);

done:
  if (k) {
    RedisModule_CloseKey(k);
  }
  return ret;
}

//---------------------------------------------------------------------------------------------

IndexIterator *Query_EvalNode(QueryEvalCtx *q, QueryNode *n) {
  switch (n->type) {
    case QN_TOKEN:
      return Query_EvalTokenNode(q, n);
    case QN_PHRASE:
      return Query_EvalPhraseNode(q, n);
    case QN_UNION:
      return Query_EvalUnionNode(q, n);
    case QN_TAG:
      return Query_EvalTagNode(q, n);
    case QN_NOT:
      return Query_EvalNotNode(q, n);
    case QN_PREFX:
      return Query_EvalPrefixNode(q, n);
    case QN_LEXRANGE:
      return Query_EvalLexRangeNode(q, n);
    case QN_FUZZY:
      return Query_EvalFuzzyNode(q, n);
    case QN_NUMERIC:
      return Query_EvalNumericNode(q, &n->nn);
    case QN_OPTIONAL:
      return Query_EvalOptionalNode(q, n);
    case QN_GEO:
      return Query_EvalGeofilterNode(q, &n->gn, n->opts.weight);
    case QN_IDS:
      return Query_EvalIdFilterNode(q, &n->fn);
    case QN_WILDCARD:
      return Query_EvalWildcardNode(q, n);
    case QN_NULL:
      return NewEmptyIterator();
  }

  return NULL;
}

//---------------------------------------------------------------------------------------------

QueryParse::QueryParse(char *query, size_t nquery, const RedisSearchCtx &sctx_,
                       const RSSearchOptions &opts_, QueryError *status_) {
  raw =  query;
  len = nquery;
  sctx = (RedisSearchCtx *)&sctx_;
  opts = &opts_;
  status = status_;
}

//---------------------------------------------------------------------------------------------

QueryNode *RSQuery_ParseRaw(QueryParse *);

/**
 * Parse the query string into an AST.
 * @param dst the AST structure to populate
 * @param sctx the context - this is never written to or retained
 * @param sopts options modifying parsing behavior
 * @param qstr the query string
 * @param len the length of the query string
 * @param status error details set here.
 */

QueryAST::QueryAST(const RedisSearchCtx &sctx, const RSSearchOptions &opts,
                   const char *q, size_t n, QueryError *status) {
  query = rm_strndup(q, n);
  nquery = n;

  QueryParse qp(query, nquery, sctx, opts, status);

  root = RSQuery_ParseRaw(&qpCtx);
  // printf("Parsed %.*s. Error (Y/N): %d. Root: %p\n", (int)n, q, status->HasError(), root);
  if (!root) {
    if (status->HasError()) {
      throw Error(status);
    }
    root = new QueryNode(QN_NULL);
  }
  if (status->HasError()) {
    if (root) {
      delete root;
    }
    throw Error(status);
  }
  numTokens = qpCtx.numTokens;
}

//---------------------------------------------------------------------------------------------

Query::Query(QueryAST &ast, const RSSearchOptions *opts_, RedisSearchCtx *sctx_,
             ConcurrentSearchCtx *conc) {
  conc = conc_;
  opts = opts_;
  numTokens = qast.numTokens;
  docTable = &sctx_->spec->docs;
  sctx = sctx_;
}

//---------------------------------------------------------------------------------------------

/**
 * Open the result iterator on the filters. Returns the iterator for the root node.
 *
 * @param ast the parsed tree
 * @param opts options
 * @param sctx the search context. Note that this may be retained by the iterators
 *  for the remainder of the query.
 * @param conc Used to save state on the query
 * @return an iterator.
 */

IndexIterator *QueryAST::Iterate(const RSSearchOptions &opts, RedisSearchCtx &sctx,
                                 ConcurrentSearchCtx &conc) const {
  Query query(*this, &opts, &sctx, conc);
  IndexIterator *iter = query.eval(root);
  if (!iter) {
    // Return the dummy iterator
    iter = new EmptyIterator();
  }
  return iter;
}

//---------------------------------------------------------------------------------------------

QueryAST::~QueryAST() {
  delete root;
  numTokens = 0;
  rm_free(query);
  nquery = 0;
  query = NULL;
}

//---------------------------------------------------------------------------------------------

/**
 * Expand the query using a pre-registered expander. Query expansion possibly
 * modifies or adds additional search terms to the query.
 * @param q the query
 * @param expander the name of the expander
 * @param opts query options, passed to the expander function
 * @param status error detail
 * @return REDISMODULE_OK, or REDISMODULE_ERR with more detail in `status`
 */

int QueryAST::Expand(const char *expander, RSSearchOptions *opts, RedisSearchCtx &sctx,
                     QueryError *status) {
  if (!root) {
    return REDISMODULE_OK;
  }
  RSQueryExpanderCtx expCtx = {
      qast: = q, language: opts->language, handle: &sctx, status: status};

  ExtQueryExpanderCtx *xpc =
      Extensions_GetQueryExpander(&expCtx, expander ? expander : DEFAULT_EXPANDER_NAME);
  if (xpc && xpc->exp) {
    QueryNode_Expand(xpc->exp, &expCtx, &q->root);
    if (xpc->ff) {
      xpc->ff(expCtx.privdata);
    }
  }
  if (status->HasError()) {
    return REDISMODULE_ERR;
  }
  return REDISMODULE_OK;
}

//---------------------------------------------------------------------------------------------

// Set the field mask recursively on a query node. This is called by the parser to handle
// situations like @foo:(bar baz|gaz), where a complex tree is being applied a field mask.
void QueryNode::SetFieldMask(t_fieldMask mask) {
  if (!this) return;
  opts.fieldMask &= mask;
  for (size_t ii = 0; ii < NumChildren(); ++ii) {
    children[ii]->SetFieldMask(mask);
  }
}

//---------------------------------------------------------------------------------------------

void QueryNode::AddChildren(QueryNode **children_, size_t nchildren) {
  if (type == QN_TAG) {
    for (size_t ii = 0; ii < nchildren; ++ii) {
      if (children_[ii]->type == QN_TOKEN || children_[ii]->type == QN_PHRASE ||
          children_[ii]->type == QN_PREFX || children_[ii]->type == QN_LEXRANGE) {
        children = array_ensure_append(children, children_ + ii, 1, QueryNode *);
      }
    }
  } else {
    array_ensure_append(children, children_, nchildren, QueryNode *);
  }
}

//---------------------------------------------------------------------------------------------

void QueryNode::AddChild(QueryNode *ch) {
  AddChildren(&ch, 1);
}

//---------------------------------------------------------------------------------------------

void QueryNode::ClearChildren(bool shouldFree) {
  if (shouldFree) {
    for (size_t ii = 0; ii < NumChildren(); ++ii) {
      QueryNode_Free(children[ii]);
    }
  }
  if (NumChildren()) {
    array_clear(children);
  }
}

//---------------------------------------------------------------------------------------------

/* Set the concurrent mode of the query. By default it's on, setting here to 0 will turn it off,
 * resulting in the query not performing context switches */
// void Query_SetConcurrentMode(QueryPlan *q, int concurrent) {
//   q->concurrentMode = concurrent;
// }

static sds doPad(sds s, int len) {
  if (!len) return s;

  char buf[len * 2 + 1];
  memset(buf, ' ', len * 2);
  buf[len * 2] = 0;
  return sdscat(s, buf);
}

//---------------------------------------------------------------------------------------------

sds QueryNode::DumpSds(sds s, const IndexSpec *spec, int depth) const {
  s = doPad(s, depth);

  if (opts.fieldMask == 0) {
    s = sdscat(s, "@NULL:");
  }

  if (opts.fieldMask && opts.fieldMask != RS_FIELDMASK_ALL && type != QN_NUMERIC &&
      type != QN_GEO && type != QN_IDS) {
    if (!spec) {
      s = sdscatprintf(s, "@%" PRIu64, (uint64_t)opts.fieldMask);
    } else {
      s = sdscat(s, "@");
      t_fieldMask fm = opts.fieldMask;
      int i = 0, n = 0;
      while (fm) {
        t_fieldMask bit = (fm & 1) << i;
        if (bit) {
          const char *f = spec->GetFieldNameByBit(bit);
          s = sdscatprintf(s, "%s%s", n ? "|" : "", f ? f : "n/a");
          n++;
        }
        fm = fm >> 1;
        i++;
      }
    }
    s = sdscat(s, ":");
  }

  switch (type) {
    case QN_PHRASE:
      s = sdscatprintf(s, "%s {\n", pn.exact ? "EXACT" : "INTERSECT");
      for (size_t ii = 0; ii < NumChildren(); ++ii) {
        s = children[ii]->DumpSds(s, spec, depth + 1);
      }
      s = doPad(s, depth);

      break;
    case QN_TOKEN:
      s = sdscatprintf(s, "%s%s", (char *)tn.str, tn.expanded ? "(expanded)" : "");
      if (opts.weight != 1) {
        s = sdscatprintf(s, " => {$weight: %g;}", opts.weight);
      }
      s = sdscat(s, "\n");
      return s;

    case QN_PREFX:
      s = sdscatprintf(s, "PREFIX{%s*", (char *)pfx.str);
      break;

    case QN_LEXRANGE:
      s = sdscatprintf(s, "LEXRANGE{%s...%s", lxrng.begin ? lxrng.begin : "",
                       lxrng.end ? lxrng.end : "");
      break;

    case QN_NOT:
      s = sdscat(s, "NOT{\n");
      s = DumpChildren(s, spec, depth + 1);
      s = doPad(s, depth);
      break;

    case QN_OPTIONAL:
      s = sdscat(s, "OPTIONAL{\n");
      s = DumpChildren(s, spec, depth + 1);
      s = doPad(s, depth);
      break;

    case QN_NUMERIC: {
      const NumericFilter *f = nn.nf;
      s = sdscatprintf(s, "NUMERIC {%f %s @%s %s %f", f->min, f->inclusiveMin ? "<=" : "<",
                       f->fieldName, f->inclusiveMax ? "<=" : "<", f->max);
    } break;
    case QN_UNION:
      s = sdscat(s, "UNION {\n");
      s = DumpChildren(s, spec, depth + 1);
      s = doPad(s, depth);
      break;
    case QN_TAG:
      s = sdscatprintf(s, "TAG:@%.*s {\n", (int)tag.len, tag.fieldName);
      s = DumpChildren(s, spec, depth + 1);
      s = doPad(s, depth);
      break;
    case QN_GEO:

      s = sdscatprintf(s, "GEO %s:{%f,%f --> %f %s", gn.gf->property, gn.gf->lon,
                       gn.gf->lat, gn.gf->radius,
                       GeoDistance_ToString(gn.gf->unitType));
      break;
    case QN_IDS:

      s = sdscat(s, "IDS { ");
      for (int i = 0; i < fn.len; i++) {
        s = sdscatprintf(s, "%llu,", (unsigned long long)fn.ids[i]);
      }
      break;
    case QN_WILDCARD:

      s = sdscat(s, "<WILDCARD>");
      break;
    case QN_FUZZY:
      s = sdscatprintf(s, "FUZZY{%s}\n", fz.tok.str);
      return s;

    case QN_NULL:
      s = sdscat(s, "<empty>");
  }

  s = sdscat(s, "}");
  // print attributes if not the default
  if (opts.weight != 1 || opts.maxSlop != -1 || opts.inOrder) {
    s = sdscat(s, " => {");
    if (opts.weight != 1) {
      s = sdscatprintf(s, " $weight: %g;", opts.weight);
    }
    if (opts.maxSlop != -1) {
      s = sdscatprintf(s, " $slop: %d;", opts.maxSlop);
    }
    if (opts.inOrder || opts.maxSlop != -1) {
      s = sdscatprintf(s, " $inorder: %s;", opts.inOrder ? "true" : "false");
    }
    s = sdscat(s, " }");
  }
  s = sdscat(s, "\n");
  return s;
}

//---------------------------------------------------------------------------------------------

static sds DumpChildren(sds s, const IndexSpec *spec, int depth) const {
  for (size_t ii = 0; ii < NumChildren(); ++ii) {
    s = children[ii]->DumpSds(s, spec, depth);
  }
  return s;
}

//---------------------------------------------------------------------------------------------

/* Return a string representation of the query parse tree. The string should be freed by the
 * caller
 */
char *QueryAST::DumpExplain(const IndexSpec *spec) const {
  // empty query
  if (!root) {
    return rm_strdup("NULL");
  }

  sds s = root->DumpSds(sdsnew(""), spec, 0);
  char *ret = rm_strndup(s, sdslen(s));
  sdsfree(s);
  return ret;
}

//---------------------------------------------------------------------------------------------

void QueryAST::Print(const IndexSpec *spec) const {
  sds s = root->DumpSds(sdsnew(""), spec, 0);
  printf("%s\n", s);
  sdsfree(s);
}

//---------------------------------------------------------------------------------------------

int QueryNode::ForEach(QueryNode_ForEachCallback callback, void *ctx, int reverse) {
#define INITIAL_ARRAY_NODE_SIZE 5
  QueryNode **nodes = array_new(QueryNode *, INITIAL_ARRAY_NODE_SIZE);
  nodes = array_append(nodes, this);
  int retVal = 1;
  while (array_len(nodes) > 0) {
    QueryNode *curr = array_pop(nodes);
    if (!callback(curr, this, ctx)) {
      retVal = 0;
      break;
    }
    if (reverse) {
      for (size_t ii = curr->NumChildren(); ii; --ii) {
        nodes = array_append(nodes, curr->children[ii - 1]);
      }
    } else {
      for (size_t ii = 0; ii < curr->NumChildren(); ++ii) {
        nodes = array_append(nodes, curr->children[ii]);
      }
    }
  }

  array_free(nodes);
  return retVal;
}

//---------------------------------------------------------------------------------------------

int QueryNode::ApplyAttribute(QueryAttribute *attr, QueryError *status) {

#define MK_INVALID_VALUE()                                                         \
  status->SetErrorFmt(QUERY_ESYNTAX, "Invalid value (%.*s) for `%.*s`", \
                         (int)attr->vallen, attr->value, (int)attr->namelen, attr->name)

  // Apply slop: [-1 ... INF]
  if (STR_EQCASE(attr->name, attr->namelen, "slop")) {
    long long n;
    if (!ParseInteger(attr->value, &n) || n < -1) {
      MK_INVALID_VALUE();
      return 0;
    }
    opts.maxSlop = n;

  } else if (STR_EQCASE(attr->name, attr->namelen, "inorder")) {
    // Apply inorder: true|false
    int b;
    if (!ParseBoolean(attr->value, &b)) {
      MK_INVALID_VALUE();
      return 0;
    }
    opts.inOrder = b;

  } else if (STR_EQCASE(attr->name, attr->namelen, "weight")) {
    // Apply weight: [0  ... INF]
    double d;
    if (!ParseDouble(attr->value, &d) || d < 0) {
      MK_INVALID_VALUE();
      return 0;
    }
    opts.weight = d;

  } else if (STR_EQCASE(attr->name, attr->namelen, "phonetic")) {
    // Apply phonetic: true|false
    int b;
    if (!ParseBoolean(attr->value, &b)) {
      MK_INVALID_VALUE();
      return 0;
    }
    if (b) {
      opts.phonetic = PHONETIC_ENABLED;  // means we specifically asked for phonetic matching
    } else {
      opts.phonetic =
          PHONETIC_DESABLED;  // means we specifically asked no for phonetic matching
    }
    // opts.noPhonetic = PHONETIC_DEFAULT -> means no special asks regarding phonetics
    //                                          will be enable if field was declared phonetic

  } else {
    status->SetErrorFmt(QUERY_ENOOPTION, "Invalid attribute %.*s", (int)attr->namelen,
                           attr->name);
    return 0;
  }

  return 1;
}

//---------------------------------------------------------------------------------------------

int QueryNode::ApplyAttributes(QueryAttribute *attrs, size_t len, QueryError *status) {
  for (size_t i = 0; i < len; i++) {
    if (!ApplyAttribute(&attrs[i], status)) {
      return 0;
    }
  }
  return 1;
}

///////////////////////////////////////////////////////////////////////////////////////////////
