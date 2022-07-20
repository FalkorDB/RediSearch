#pragma once

#include "value.h"
#include "query.h"
#include "reducer.h"
#include "result_processor.h"
#include "expr/expression.h"
#include "aggregate_plan.h"

#include "util/map.h"
#include "rmutil/rm_assert.h"

///////////////////////////////////////////////////////////////////////////////////////////////

enum class CommandType { Aggregate, Search, Explain };

//---------------------------------------------------------------------------------------------

enum QEXECFlags : uint32_t {
  QEXEC_F_IS_EXTENDED = 0x01,    // Contains aggregations or projections
  QEXEC_F_SEND_SCORES = 0x02,    // Output: Send scores with each result
  QEXEC_F_SEND_SORTKEYS = 0x04,  // Sent the key used for sorting, for each result
  QEXEC_F_SEND_NOFIELDS = 0x08,  // Don't send the contents of the fields
  QEXEC_F_SEND_PAYLOADS = 0x10,  // Sent the payload set with ADD
  QEXEC_F_IS_CURSOR = 0x20,      // Is a cursor-type query

  // Don't use concurrent execution
  QEXEC_F_SAFEMODE = 0x100,

  // The inverse of IS_EXTENDED. The two cannot coexist together
  QEXEC_F_IS_SEARCH = 0x200,

  // Highlight/summarize options are active
  QEXEC_F_SEND_HIGHLIGHT = 0x400,

  // Do not emit any rows, only the number of query results
  QEXEC_F_NOROWS = 0x800,

  // Do not stringify result values. Send them in their proper types
  QEXEC_F_TYPED = 0x1000,

  // Send raw document IDs alongside key names. Used for debugging
  QEXEC_F_SENDRAWIDS = 0x2000,

  // Flag for scorer function to create explanation strings
  QEXEC_F_SEND_SCOREEXPLAIN = 0x4000
};

//---------------------------------------------------------------------------------------------

enum QEStateFlags {
  // Received EOF from iterator
  QEXEC_S_ITERDONE = 0x02,
};

//---------------------------------------------------------------------------------------------

/**
 * Do not create the root result processor. Only process those components
 * which process fully-formed, fully-scored results. This also means
 * that a scorer is not created. It will also not initialize the
 * first step or the initial lookup table.
 */
enum BuildPipelineOptions {
  AREQ_BUILDPIPELINE_NO_ROOT = 0x01
};

//---------------------------------------------------------------------------------------------

struct AREQ : public Object {
  // plan containing the logical sequence of steps
  AGGPlan ap;

  // Arguments converted to sds. Received on input.
  sds *args;
  size_t nargs;

  // Search query string
  const char *query;

  // Fields to be output and otherwise processed
  FieldList outFields;

  // Options controlling search behavior
  RSSearchOptions searchopts;

  // Parsed query tree
  std::unique_ptr<QueryAST> ast;

  // Root iterator. This is owned by the request
  IndexIterator *rootiter;

  // Context, owned by request
  std::unique_ptr<RedisSearchCtx> sctx;

  // Resumable context
  std::unique_ptr<ConcurrentSearch> conc;

  // Context for iterating over the queries themselves
  std::unique_ptr<QueryIterator> qiter;

  // Used for identifying unique objects across this request
  uint32_t serial;

  // Flags controlling query output
  QEXECFlags reqflags;

  // Flags indicating current execution state
  uint32_t stateflags;

  // Query timeout in milliseconds
  uint32_t tmoMS;
  uint32_t tmoPolicy;

  // Cursor settings
  unsigned cursorMaxIdle;
  unsigned cursorChunkSize;

  AREQ(RedisModuleCtx *ctx, RedisModuleString **argv, int argc, CommandType type, QueryError *status);
  ~AREQ();

  int Compile(RedisModuleString **argv, int argc, QueryError *status);
  int ApplyContext(QueryError *status);
  int BuildPipeline(BuildPipelineOptions options, QueryError *status);

  void Execute(RedisModuleCtx *outctx);
  int StartCursor(RedisModuleCtx *outctx, const char *lookupName, QueryError *status);

  // Cached variables to avoid serializeResult retrieving these each time
  struct CachedVars {
    const RLookup *lastLk;
    const PLN_ArrangeStep *lastAstp;
  };

  size_t serializeResult(RedisModuleCtx *outctx, const SearchResult *r, const CachedVars &cv);
  int sendChunk(RedisModuleCtx *outctx, size_t limit);
  const RSValue *getSortKey(const SearchResult *r, const PLN_ArrangeStep *astp);

  void ensureSimpleMode();
  int ensureExtendedMode(const char *name, QueryError *status);
  int parseCursorSettings(ArgsCursor *ac, QueryError *status);
  int handleCommonArgs(ArgsCursor *ac, bool allowLegacy, QueryError *status);
  int parseQueryArgs(ArgsCursor *ac, RSSearchOptions *searchOpts, AggregatePlan *plan, QueryError *status);
  int parseGroupby(ArgsCursor *ac, QueryError *status);
  int handleApplyOrFilter(ArgsCursor *ac, bool isApply, QueryError *status);
  int handleLoad(ArgsCursor *ac, QueryError *status);

  ResultProcessor *RP() { return qiter->endProc; }
  ResultProcessor *pushRP(ResultProcessor *rp, ResultProcessor *rpUpstream);
  ResultProcessor *getGroupRP(PLN_GroupStep *gstp, ResultProcessor *rpUpstream, QueryError *status);
  ResultProcessor *getArrangeRP(AGGPlan *pln, const PLN_BaseStep *stp, ResultProcessor *up, QueryError *status);
  ResultProcessor *getScorerRP();

  void buildImplicitPipeline(QueryError *status);
  int buildOutputPipeline(QueryError *status);
};

///////////////////////////////////////////////////////////////////////////////////////////////

// Grouper Functions

struct Grouper;

//---------------------------------------------------------------------------------------------

// A group represents the allocated context of all reducers in a group, and the
// selected values of that group.
// Because one of these is created for every single group (i.e. every single
// unique key) we want to keep this quite small!

struct Group {
  // Contains the selected 'out' values used by the reducers output functions
  RLookupRow rowdata;

  Group(Grouper &grouper, const RSValue **groupvals, size_t ngrpvals);

  void invokeReducers(RLookupRow *srcrow);
  void writeValues(SearchResult *r) const;
};

//---------------------------------------------------------------------------------------------

// KHASH_MAP_INIT_INT64(khid, Group *);

struct Grouper : ResultProcessor {
  // Map of group_name => `Group` structure
  // static const int khid;
  // khash_t(khid) *groups;
  typedef UnorderedMap<uint64_t, Group*> GroupsMap;
  GroupsMap groups;

  // Backing store for the groups themselves
  BlkAlloc groupsAlloc;

  // Keys to group by. Both srckeys and dstkeys are used because different lookups
  // are employed. The srckeys are the lookup keys for the properties as they
  // appear in the row received from the upstream processor, and the dstkeys are
  // the keys as they are expected in the output row.

  const RLookupKey **srckeys;
  const RLookupKey **dstkeys;
  size_t nkeys;

  arrayof(Reducer*) reducers;

  // Used for maintaining state when yielding groups
  GroupsMap::iterator iter;
  bool _yield;

  Grouper(const RLookupKey **srckeys_, const RLookupKey **dstkeys, size_t nkeys);
  virtual ~Grouper();

  void extractGroups(const RSValue **xarr, size_t xpos, size_t xlen, size_t arridx,
                     uint64_t hval, RLookupRow *res);

  size_t numReducers() const;

  virtual int Next(SearchResult &res);
  int Yield(SearchResult &res);

  void AddReducer(Reducer *r, RLookupKey *dstkey);
  ResultProcessor *GetRP();

  void writeGroupValues(const Group *gr, SearchResult &r) const;

  void invokeReducers(RLookupRow &srcrow);
  void invokeGroupReducers(Group *gr, RLookupRow &srcrow);
};

//---------------------------------------------------------------------------------------------

int RSCursorCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);

///////////////////////////////////////////////////////////////////////////////////////////////
