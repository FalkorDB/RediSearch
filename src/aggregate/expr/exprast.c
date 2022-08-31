
#include "exprast.h"

#include <ctype.h>

///////////////////////////////////////////////////////////////////////////////////////////////

RSArgList::RSArgList(RSExpr *e) {
  if (e) args.push_back(e);
}

//---------------------------------------------------------------------------------------------

RSArgList *RSArgList::Append(RSExpr *e) {
  args.push_back(e);
  return this;
}

///////////////////////////////////////////////////////////////////////////////////////////////

#if 0
static RSExpr *newExpr(RSExprType t) {
  RSExpr *e = rm_calloc(1, sizeof(*e));
  e->t = t;
  return e;
}
#endif

//---------------------------------------------------------------------------------------------

// unquote and unescape a stirng literal, and return a cleaned copy of it
static char *unescpeStringDup(const char *s, size_t sz) {

  char *dst = rm_malloc(sz);
  char *dstStart = dst;
  char *src = (char *)s + 1;       // we start after the first quote
  char *end = (char *)s + sz - 1;  // we end at the last quote
  while (src < end) {
    // unescape
    if (*src == '\\' && src + 1 < end && (ispunct(*(src + 1)) || isspace(*(src + 1)))) {
      ++src;
      continue;
    }
    *dst++ = *src++;
  }
  *dst = '\0';
  return dstStart;
}

//---------------------------------------------------------------------------------------------

RSStringLiteral::RSStringLiteral(const char *str, size_t len) {
  literal = RS_StaticValue(RSValue_String);
  literal.strval.str = unescpeStringDup(str, len);
  literal.strval.len = strlen(literal.strval.str);
  literal.strval.stype = RSString_Malloc;
}

//---------------------------------------------------------------------------------------------

RSNullLiteral::RSNullLiteral() {
  literal.MakeReference(RS_NullVal());
}

//---------------------------------------------------------------------------------------------

RSNumberLiteral::RSNumberLiteral(double n) {
  literal = RS_StaticValue(RSValue_Number);
  literal.numval = n;
}

//---------------------------------------------------------------------------------------------

RSExprOp::RSExprOp(unsigned char op_, RSExpr *left, RSExpr *right) {
  op = op_;
  left = left;
  right = right;
}

//---------------------------------------------------------------------------------------------

RSPredicate::RSPredicate(RSCondition cond, RSExpr *left, RSExpr *right) {
  cond = cond;
  left = left;
  right = right;
}

//---------------------------------------------------------------------------------------------

RSFunctionExpr::RSFunctionExpr(const char *str, size_t len, RSArgList *args, RSFunction cb) {
  _args = args; // @@ ownership
  name = rm_strndup(str, len);
  Call = cb;
}

//---------------------------------------------------------------------------------------------

RSLookupExpr::RSLookupExpr(const char *str, size_t len) {
  key = rm_strndup(str, len);
  lookupKey = NULL;
}

//---------------------------------------------------------------------------------------------

RSInverted::RSInverted(RSExpr *child) {
  child = child;
}

//---------------------------------------------------------------------------------------------

RSArgList::~RSArgList() {
  for (auto arg: args) {
    delete arg;
  }
}

///////////////////////////////////////////////////////////////////////////////////////////////

RSExprOp::~RSExprOp() {
  delete left;
  delete right;
}

RSPredicate::~RSPredicate() {
  delete left;
  delete right;
}

RSInverted::~RSInverted() {
  delete child;
}

RSFunctionExpr::~RSFunctionExpr() {
  rm_free((char *)name);
  delete _args;
}

RSLookupExpr::~RSLookupExpr() {
  rm_free((char *)key);
}

RSLiteral::~RSLiteral() {
  literal.Clear();
}

//---------------------------------------------------------------------------------------------

void RSExprOp::Print() const {
  printf("(");
  left->Print();
  printf(" %c ", op);
  right->Print();
  printf(")");
}

void RSPredicate::Print() const {
  printf("(");
  left->Print();
  printf(" %s ", RSConditionStrings[cond]);
  right->Print();
  printf(")");
}

void RSInverted::Print() const {
  printf("!");
  child->Print();
}

void RSFunctionExpr::Print() const {
  printf("%s(", name);
  for (size_t i = 0; _args != NULL && i < _args->length(); i++) {
    (*_args)[i]->Print();
    if (i < _args->length() - 1) printf(", ");
  }
  printf(")");
}

void RSLookupExpr::Print() const {
  printf("@%s", key);
}

void RSLiteral::Print() const {
  literal.Print();
}

//---------------------------------------------------------------------------------------------

#if 0
// @@ used in unit tests

void ExprAST_Free(RSExpr *e) {
  delete e;
}

void ExprAST_Print(const RSExpr *e) {
  e->Print(e);
}

#endif // 0

//---------------------------------------------------------------------------------------------

RSExpr *RSExpr::ParseAST(const char *e, size_t n, QueryError *status) {
  char *errtmp = NULL;
  RS_LOG_ASSERT(!status->HasError(), "Query has error")

  RSExpr *ret = RSExpr::Parse(e, n, &errtmp);
  if (!ret) {
    status->SetError(QUERY_EEXPR, errtmp);
  }
  rm_free(errtmp);
  return ret;
}

///////////////////////////////////////////////////////////////////////////////////////////////
