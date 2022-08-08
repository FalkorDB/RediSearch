#include "aggregate/expr/expression.h"
#include "aggregate/functions/function.h"

#include "util/minmax.h"
#include "util/block_alloc.h"
#include "util/arr.h"
#include "rmutil/sds.h"

#include <ctype.h>
#include <err.h>

///////////////////////////////////////////////////////////////////////////////////////////////

#define STRING_BLOCK_SIZE 512

//---------------------------------------------------------------------------------------------

static int func_matchedTerms(ExprEval *ctx, RSValue *result, RSValue **argv, size_t argc,
                             QueryError *err) {
  int maxTerms = 0;
  if (argc == 1) {
    double d;
    if (argv[0]->ToNumber(&d)) {
      if (d > 0) {
        maxTerms = (int)d;
      }
    }
  }

  if (maxTerms == 0) maxTerms = 100;
  maxTerms = MIN(100, maxTerms);
  const SearchResult *res = ctx->res;

  if (res && res->indexResult) {
    RSQueryTerm *terms[maxTerms];
    size_t n = ctx->res->indexResult->GetMatchedTerms(terms, maxTerms);
    if (n) {
      RSValue **arr = rm_calloc(n, sizeof(RSValue *));
      for (size_t i = 0; i < n; i++) {
        arr[i] = RS_ConstStringVal(terms[i]->str.c_str(), terms[i]->str.length());
      }
      RSValue *v = RSValue::NewArray(arr, n, RSVAL_ARRAY_ALLOC | RSVAL_ARRAY_NOINCREF);
      result->MakeOwnReference(v);
      return EXPR_EVAL_OK;
    }
  }
  result->MakeReference(RS_NullVal());
  return EXPR_EVAL_OK;
}

//---------------------------------------------------------------------------------------------

/* lower(str) */
static int stringfunc_tolower(ExprEval *ctx, RSValue *result, RSValue **argv, size_t argc, QueryError *err) {
  VALIDATE_ARGS("lower", 1, 1, err);
  RSValue *val = argv[0]->Dereference();
  if (!val->IsString()) {
    result->MakeReference(RS_NullVal());
    return EXPR_EVAL_OK;
  }

  size_t sz = 0;
  char *p = (char *)val->StringPtrLen(&sz);
  char *np = ctx->Strndup(p, sz);
  for (size_t i = 0; i < sz; i++) {
    np[i] = tolower(np[i]);
  }
  result->SetConstString(np, sz);
  return EXPR_EVAL_OK;
}

//---------------------------------------------------------------------------------------------

/* upper(str) */
static int stringfunc_toupper(ExprEval *ctx, RSValue *result, RSValue **argv, size_t argc,
                              QueryError *err) {
  VALIDATE_ARGS("upper", 1, 1, err);

  RSValue *val = argv[0]->Dereference();
  if (!val->IsString()) {
    result->MakeReference(RS_NullVal());
    return EXPR_EVAL_OK;
  }

  size_t sz = 0;
  char *p = (char *)val->StringPtrLen(&sz);
  char *np = ctx->Strndup(p, sz);
  for (size_t i = 0; i < sz; i++) {
    np[i] = toupper(np[i]);
  }
  result->SetConstString(np, sz);
  return EXPR_EVAL_OK;
}

//---------------------------------------------------------------------------------------------

/* substr(str, offset, len) */
static int stringfunc_substr(ExprEval *ctx, RSValue *result, RSValue **argv, size_t argc,
                             QueryError *err) {
  VALIDATE_ARGS("substr", 3, 3, err);

  VALIDATE_ARG_TYPE("substr", argv, 1, RSValue_Number);
  VALIDATE_ARG_TYPE("substr", argv, 2, RSValue_Number);

  size_t sz;
  const char *str = argv[0]->StringPtrLen(&sz);
  if (!str) {
    err->SetError(QUERY_EPARSEARGS, "Invalid type for substr. Expected string");
    return EXPR_EVAL_ERR;
  }

  int offset = (int)(argv[1]->Dereference())->numval;
  int len = (int)(argv[2]->Dereference())->numval;

  // for negative offsets we count from the end of the string
  if (offset < 0) {
    offset = (int)sz + offset;
  }
  offset = MAX(0, MIN(offset, sz));
  // len < 0 means read until the end of the string
  if (len < 0) {
    len = MAX(0, ((int)sz - offset) + len);
  }
  if (offset + len > sz) {
    len = sz - offset;
  }

  char *dup = ctx->Strndup(&str[offset], len);
  result->SetConstString(dup, len);
  return EXPR_EVAL_OK;
}

//---------------------------------------------------------------------------------------------

int func_to_number(ExprEval *ctx, RSValue *result, RSValue **argv, size_t argc, QueryError *err) {
  VALIDATE_ARGS("to_number", 1, 1, err);

  double n;
  if (!argv[0]->ToNumber(&n)) {
    size_t sz = 0;
    const char *p = argv[0]->StringPtrLen(&sz);
    err->SetErrorFmt(QUERY_EPARSEARGS, "to_number: cannot convert string '%s'", p);
    return EXPR_EVAL_ERR;
  }

  result->SetNumber(n);
  return EXPR_EVAL_OK;
}

//---------------------------------------------------------------------------------------------

int func_to_str(ExprEval *ctx, RSValue *result, RSValue **argv, size_t argc, QueryError *err) {
  VALIDATE_ARGS("to_str", 1, 1, err);

  result->ToString(argv[0]);
  return EXPR_EVAL_OK;
}

//---------------------------------------------------------------------------------------------

static int stringfunc_format(ExprEval *ctx, RSValue *result, RSValue **argv, size_t argc,
                             QueryError *err) {
  if (argc < 1) {
    QERR_MKBADARGS_FMT(err, "Need at least one argument for format");
    return EXPR_EVAL_ERR;
  }
  VALIDATE_ARG_ISSTRING("format", argv, 0);

  size_t argix = 1;
  size_t fmtsz = 0;
  const char *fmt = argv[0]->StringPtrLen(&fmtsz);
  const char *last = fmt, *end = fmt + fmtsz;
  sds out = sdsMakeRoomFor(sdsnew(""), fmtsz);

  for (size_t ii = 0; ii < fmtsz; ++ii) {
    if (fmt[ii] != '%') {
      continue;
    }

    if (ii == fmtsz - 1) {
      // ... %"
      QERR_MKBADARGS_FMT(err, "Bad format string!");
      goto error;
    }

    // Detected a format string. Write from 'last' up to 'fmt'
    out = sdscatlen(out, last, (fmt + ii) - last);
    last = fmt + ii + 2;

    char type = fmt[++ii];
    if (type == '%') {
      // Append literal '%'
      out = sdscat(out, "%");
      continue;
    }

    if (argix == argc) {
      QERR_MKBADARGS_FMT(err, "Not enough arguments for format");
      goto error;
    }

    RSValue *arg = argv[argix++]->Dereference();
    if (type == 's') {
      if (arg->t == RSValue_Null) {
        // write null value
        out = sdscat(out, "(null)");
        continue;
      } else if (!arg->IsString()) {

        RSValue strval;
        strval.ToString(arg);
        size_t sz;
        const char *str = strval.StringPtrLen(&sz);
        if (!str) {
          out = sdscat(out, "(null)");
        } else {
          out = sdscatlen(out, str, sz);
        }
      } else {
        size_t sz;
        const char *str = arg->StringPtrLen(&sz);
        out = sdscatlen(out, str, sz);
      }
    } else {
      QERR_MKBADARGS_FMT(err, "Unknown format specifier passed");
      goto error;
    }
  }

  if (last && last < end) {
    out = sdscatlen(out, last, end - last);
  }

  result->SetSDS(out);
  return EXPR_EVAL_OK;

error:
  assert(err->HasError());
  sdsfree(out);
  result->MakeReference(RS_NullVal());
  return EXPR_EVAL_ERR;
}

//---------------------------------------------------------------------------------------------

char *strtrim(char *s, size_t sl, size_t *outlen, const char *cset) {
  char *start, *end, *sp, *ep;

  sp = start = s;
  ep = end = s + sl - 1;
  while (sp <= end && strchr(cset, *sp)) sp++;
  while (ep > sp && strchr(cset, *ep)) ep--;
  *outlen = (sp > ep) ? 0 : ((ep - sp) + 1);

  return sp;
}

//---------------------------------------------------------------------------------------------

static int stringfunc_split(ExprEval *ctx, RSValue *result, RSValue **argv, size_t argc,
                            QueryError *err) {
  if (argc < 1 || argc > 3) {
    QERR_MKBADARGS_FMT(err, "Invalid number of arguments for split");
    return EXPR_EVAL_ERR;
  }
  VALIDATE_ARG_ISSTRING("format", argv, 0);
  const char *sep = ",";
  const char *strp = " ";
  if (argc >= 2) {
    VALIDATE_ARG_ISSTRING("format", argv, 1);
    sep = argv[1]->StringPtrLen();
  }
  if (argc == 3) {
    VALIDATE_ARG_ISSTRING("format", argv, 2);
    strp = argv[2]->StringPtrLen();
  }

  size_t len;
  char *str = (char *)argv[0]->StringPtrLen(&len);
  char *ep = str + len;
  size_t l = 0;
  char *next;
  char *tok = str;

  // extract at most 1024 values
  static RSValue *tmp[1024];
  while (l < 1024 && tok < ep) {
    next = strpbrk(tok, sep);
    size_t sl = next ? (next - tok) : ep - tok;

    if (sl > 0) {
      size_t outlen;
      // trim the strip set
      char *s = strtrim(tok, sl, &outlen, strp);
      if (outlen) {
        tmp[l++] = RS_NewCopiedString(s, outlen);
      }
    }

    // advance tok while it's not in the sep
    if (!next) break;

    tok = next + 1;
  }

  // if (len > 0) {
  //   tmp[l++] = RS_ConstStringVal(tok, len);
  // }

  RSValue **vals = rm_calloc(l, sizeof(*vals));
  for (size_t i = 0; i < l; i++) {
    vals[i] = tmp[i];
  }
  RSValue *ret = RSValue::NewArray(vals, l, RSVAL_ARRAY_ALLOC | RSVAL_ARRAY_NOINCREF);
  result->MakeOwnReference(ret);
  return EXPR_EVAL_OK;
}

//---------------------------------------------------------------------------------------------

int func_exists(ExprEval *ctx, RSValue *result, RSValue **argv, size_t argc, QueryError *err) {
  VALIDATE_ARGS("exists", 1, 1, err);

  result->t = RSValue_Number;
  if (argv[0]->t != RSValue_Null) {
    result->numval = 1;
  } else {
    ctx->err->ClearError();
    result->numval = 0;
  }
  return EXPR_EVAL_OK;
}

//---------------------------------------------------------------------------------------------

void RegisterStringFunctions() {
  RSFunctionRegistry_RegisterFunction("lower", stringfunc_tolower, RSValue_String);
  RSFunctionRegistry_RegisterFunction("upper", stringfunc_toupper, RSValue_String);
  RSFunctionRegistry_RegisterFunction("substr", stringfunc_substr, RSValue_String);
  RSFunctionRegistry_RegisterFunction("format", stringfunc_format, RSValue_String);
  RSFunctionRegistry_RegisterFunction("split", stringfunc_split, RSValue_Array);
  RSFunctionRegistry_RegisterFunction("matched_terms", func_matchedTerms, RSValue_Array);
  RSFunctionRegistry_RegisterFunction("to_number", func_to_number, RSValue_Number);
  RSFunctionRegistry_RegisterFunction("to_str", func_to_str, RSValue_String);
  RSFunctionRegistry_RegisterFunction("exists", func_exists, RSValue_Number);
}

///////////////////////////////////////////////////////////////////////////////////////////////
