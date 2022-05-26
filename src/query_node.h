#pragma once

#include "redisearch.h"
#include "query_error.h"

#include <stdlib.h>

///////////////////////////////////////////////////////////////////////////////////////////////

struct numericFilter;
struct geoFilter;
struct idFilter;

//---------------------------------------------------------------------------------------------

// The types of query nodes
enum QueryNodeType {
  QN_PHRASE = 1,  // Phrase (AND) node, exact or not
  QN_UNION,       // Union (OR) Node
  QN_TOKEN,       // Single token node
  QN_NUMERIC,     // Numeric filter node
  QN_NOT,         // NOT operator node
  QN_OPTIONAL,    // OPTIONAL (should match) node
  QN_GEO,         // OPTIONAL (should match) node
  QN_PREFX,       // Prefix selection node
  QN_IDS,         // Id Filter node
  QN_WILDCARD,    // Wildcard node, used only in conjunction with negative root node to allow negative queries
  QN_TAG,         // Tag node, a list of tags for a specific tag field
  QN_FUZZY,       // Fuzzy term - expand with levenshtein distance
  QN_LEXRANGE,    // Lexical range
  QN_NULL         // Null term - take no action
};

//---------------------------------------------------------------------------------------------

enum QueryNodeFlags {
  QueryNode_Verbatim = 0x01,
};

//---------------------------------------------------------------------------------------------

#define PHONETIC_ENABLED 1
#define PHONETIC_DESABLED 2
#define PHONETIC_DEFAULT 0

// Various modifiers and options that can apply to the entire query or any sub-query of it

struct QueryNodeOptions {
  QueryNodeFlags flags;
  t_fieldMask fieldMask;
  int maxSlop;
  int inOrder;
  double weight;
  int phonetic;
};

//---------------------------------------------------------------------------------------------

// Query attribute is a dynamic attribute that can be applied to any query node.
// Currently supported are weight, slop, and inorder

struct QueryAttribute {
  const char *name;
  size_t namelen;
  const char *value;
  size_t vallen;
};

//---------------------------------------------------------------------------------------------

// QueryNode reqresents any query node in the query tree.
// It has a type to resolve which node it is, and a union of all possible nodes.

struct QueryNode {
  //@@ make derive classes
  /*union {
    QueryPhraseNode pn;
    QueryTokenNode tn;
    QueryUnionNode un;
    QueryNumericNode nn;
    QueryGeofilterNode gn;
    QueryIdFilterNode fn;
    QueryNotNode inverted;
    QueryOptionalNode opt;
    QueryPrefixNode pfx;
    QueryTagNode tag;
    QueryFuzzyNode fz;
    QueryLexRangeNode lxrng;
  };*/

  void ctor(QueryNodeType type_);
  QueryNode(QueryNodeType type_) { ctor(type_); }
  QueryNode(QueryNodeType type_, QueryNode **children_, size_t n) {
    ctor(type_);
    children = array_ensure_append(children, children_, n, QueryNode *);
  }
  virtual ~QueryNode();

  // The node type, for resolving the union access
  QueryNodeType type;
  QueryNodeOptions opts;
  struct QueryNode **children;

  int ApplyAttributes(QueryAttribute *attr, size_t len, QueryError *status);
  int ApplyAttributes(QueryAttribute *attrs, size_t len, QueryError *status);

  void AddChildren(QueryNode **children_, size_t n);
  void AddChild(QueryNode *child);
  void ClearChildren(bool shouldFree);

  size_t NumChildren() const { return children ? array_len(children) : 0; }
  QueryNode *GetChild(int ix) { return NumChildren() > ix ? children[ix] : NULL; }

  typedef int (*ForEachCallback)(QueryNode *node, QueryNode *q, void *ctx);
  int ForEach(ForEachCallback callback, void *ctx, bool reverse);

  void SetFieldMask(t_fieldMask mask);

  sds DumpSds(sds s, const IndexSpec *spec, int depth) const;
  sds DumpChildren(sds s, const IndexSpec *spec, int depth) const;
};

//---------------------------------------------------------------------------------------------

// A prhase node represents a list of nodes with intersection between them, or a phrase in the case
// of several token nodes.

struct QueryPhraseNode : QueryNode {
  int exact;
};

//---------------------------------------------------------------------------------------------

// Query node used when the query is effectively null but not invalid.
// This might happen as a result of a query containing only stopwords.

struct QueryNullNode : QueryNode {
  //int dummy;
};

//---------------------------------------------------------------------------------------------

struct QueryTagNode : QueryNode {
  const char *fieldName;
  size_t len;
};

//---------------------------------------------------------------------------------------------

// A token node is a terminal, single term/token node.
// An expansion of synonyms is represented by a Union node with several token nodes.
// A token can have private metadata written by expanders or tokenizers.
// Later this gets passed to scoring functions in a Term object. See RSIndexRecord.

// typedef RSToken QueryTokenNode;
// typedef RSToken QueryPrefixNode;

struct QueryTokenNode : QueryNode {
  RSToken tok;
};

struct QueryPrefixNode : QueryNode {
  RSToken tok;
};

struct QueryFuzzyNode : QueryNode {
  RSToken tok;
  int maxDist;
};

//---------------------------------------------------------------------------------------------

// A node with a numeric filter

struct QueryNumericNode : QueryNode {
  struct NumericFilter *nf;
};

//---------------------------------------------------------------------------------------------

struct QueryGeofilterNode : QueryNode {
  const struct GeoFilter *gf;
};

//---------------------------------------------------------------------------------------------

struct QueryIdFilterNode : QueryNode {
  t_docId *ids;
  size_t len;
};

//---------------------------------------------------------------------------------------------

struct QueryLexRangeNode : QueryNode {
  char *begin;
  bool includeBegin;
  char *end;
  bool includeEnd;
};

//---------------------------------------------------------------------------------------------

typedef QueryNullNode QueryUnionNode, QueryNotNode, QueryOptionalNode;

///////////////////////////////////////////////////////////////////////////////////////////////
