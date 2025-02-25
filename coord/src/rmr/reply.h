/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */


#pragma once

#include "resp3.h"

#include "redismodule.h"
#include "hiredis/hiredis.h"

#include <stdlib.h>

#define MR_REPLY_STRING 1
#define MR_REPLY_ARRAY 2
#define MR_REPLY_INTEGER 3
#define MR_REPLY_NIL 4
#define MR_REPLY_STATUS 5
#define MR_REPLY_ERROR 6

#define MR_REPLY_DOUBLE 7
#define MR_REPLY_BOOL 8
#define MR_REPLY_MAP 9
#define MR_REPLY_SET 10

#define MR_REPLY_ATTR 11
#define MR_REPLY_PUSH 12
#define MR_REPLY_BIGNUM 13
#define MR_REPLY_VERB 14

typedef struct redisReply MRReply;

static inline void MRReply_Free(MRReply *reply) {
  freeReplyObject(reply);
}

static inline int MRReply_Type(MRReply *reply) {
  return reply->type;
}

static inline long long MRReply_Integer(MRReply *reply) {
  return reply->integer;
}

static inline double MRReply_Double(MRReply *reply) {
  return reply->dval;
}

static inline size_t MRReply_Length(MRReply *reply) {
  return reply ? reply->elements : 0;
}

/* Compare a string reply with a string, optionally case sensitive */
int MRReply_StringEquals(MRReply *r, const char *s, int caseSensitive);

static inline char *MRReply_String(MRReply *reply, size_t *len) {
  if (len) {
    *len = reply->len;
  }
  return reply->str;
}

static inline MRReply *MRReply_ArrayElement(MRReply *reply, size_t idx) {
  // TODO: check out of bounds
  return reply->element[idx];
}

static inline MRReply *MRReply_MapElement(MRReply *reply, const char *key) {
  if (reply->type != MR_REPLY_MAP) return NULL;
  for (int i = 0; i < reply->elements; i += 2) {
    if (MRReply_StringEquals(reply->element[i], key, false)) {
      ++i;
      return i < reply->elements ? reply->element[i] : NULL;
    }
  }
  return NULL;
}

void MRReply_Print(FILE *fp, MRReply *r);
int MRReply_ToInteger(MRReply *reply, long long *i);
int MRReply_ToDouble(MRReply *reply, double *d);

int MR_ReplyWithMRReply(RedisModule_Reply *reply, MRReply *rep);
int RedisModule_ReplyKV_MRReply(RedisModule_Reply *reply, const char *key, MRReply *rep);

void print_mr_reply(MRReply *r);
