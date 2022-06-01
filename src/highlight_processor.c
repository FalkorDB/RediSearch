#include "result_processor.h"
#include "fragmenter.h"
#include "value.h"
#include "toksep.h"

#include "util/minmax.h"

#include <ctype.h>

///////////////////////////////////////////////////////////////////////////////////////////////

/**
 * Common parameters passed around for highlighting one or more fields within
 * a document. This structure exists to avoid passing these four parameters
 * discreetly (as we did in previous versiosn)
 */
struct hlpDocContext {
  // Byte offsets, byte-wise
  const RSByteOffsets *byteOffsets;

  // Index result, which contains the term offsets (word-wise)
  const IndexResult *indexResult;

  // Array used for in/out when writing fields. Optimization cache
  Array *iovsArr;

  RLookupRow *row;
};

//---------------------------------------------------------------------------------------------

/**
 * Attempts to fragmentize a single field from its offset entries. This takes
 * the field name, gets the matching field ID, retrieves the offset iterator
 * for the field ID, and fragments the text based on the offsets. The fragmenter
 * itself is in fragmenter.{c,h}
 *
 * Returns true if the fragmentation succeeded, false otherwise.
 */
static int fragmentizeOffsets(IndexSpec *spec, const char *fieldName, const char *fieldText,
                              size_t fieldLen, const IndexResult *indexResult,
                              const RSByteOffsets *byteOffsets, FragmentList *fragList,
                              int options) {
  const FieldSpec *fs = IndexSpec_GetField(spec, fieldName, strlen(fieldName));
  if (!fs || !fs->IsFieldType(INDEXFLD_T_FULLTEXT)) {
    return 0;
  }

  int rc = 0;
  var offsIter = indexResult->IterateOffsets();
  FragmentTermIterator fragIter;
  RSByteOffsetIterator bytesIter;
  if (RSByteOffset_Iterate(byteOffsets, fs->ftId, &bytesIter) != REDISMODULE_OK) {
    goto done;
  }

  fragIter.InitOffsets(&bytesIter, &offsIter);
  fragList->FragmentizeIter(fieldText, fieldLen, &fragIter, options);
  if (fragList->numFrags == 0) {
    goto done;
  }
  rc = 1;

done:
  offsIter.Free(offsIter.ctx);
  return rc;
}

//---------------------------------------------------------------------------------------------

// Strip spaces from a buffer in place. Returns the new length of the text,
// with all duplicate spaces stripped and converted to a single ' '.
static size_t stripDuplicateSpaces(char *s, size_t n) {
  int isLastSpace = 0;
  size_t oix = 0;
  char *out = s;
  for (size_t ii = 0; ii < n; ++ii) {
    if (isspace(s[ii])) {
      if (isLastSpace) {
        continue;
      } else {
        isLastSpace = 1;
        out[oix++] = ' ';
      }
    } else {
      isLastSpace = 0;
      out[oix++] = s[ii];
    }
  }
  return oix;
}

//---------------------------------------------------------------------------------------------

// Returns the length of the buffer without trailing spaces

static size_t trimTrailingSpaces(const char *s, size_t input) {
  for (; input && isspace(s[input - 1]); --input) {
    // Nothing
  }
  return input;
}

//---------------------------------------------------------------------------------------------

static void normalizeSettings(const ReturnedField *srcField, const ReturnedField *defaults,
                              ReturnedField *out) {
  if (srcField == NULL) {
    // Global setting
    *out = *defaults;
    return;
  }

  // Otherwise it gets more complex
  if ((defaults->mode & SummarizeMode_Highlight) &&
      (srcField->mode & SummarizeMode_Highlight) == 0) {
    out->highlightSettings = defaults->highlightSettings;
  } else if (srcField->mode && SummarizeMode_Highlight) {
    out->highlightSettings = srcField->highlightSettings;
  }

  if ((defaults->mode & SummarizeMode_Synopsis) && (srcField->mode & SummarizeMode_Synopsis) == 0) {
    out->summarizeSettings = defaults->summarizeSettings;
  } else {
    out->summarizeSettings = srcField->summarizeSettings;
  }

  out->mode |= defaults->mode | srcField->mode;
  out->name = srcField->name;
  out->lookupKey = srcField->lookupKey;
}

//---------------------------------------------------------------------------------------------

// Called when we cannot fragmentize based on byte offsets.
// docLen is an in/out parameter. On input it should contain the length of the
// field, and on output it contains the length of the trimmed summary.
// Returns a string which should be freed using free()
static char *trimField(const ReturnedField *fieldInfo, const char *docStr, size_t *docLen,
                       size_t estWordSize) {

  // Number of desired fragments times the number of context words in each fragments,
  // in characters (estWordSize)
  size_t headLen =
      fieldInfo->summarizeSettings.contextLen * fieldInfo->summarizeSettings.numFrags * estWordSize;
  headLen += estWordSize;  // Because we trim off a word when finding the toksep
  headLen = Min(headLen, *docLen);

  Array bufTmp = new Array(ArrayAlloc_RM);

  bufTmp.Write(docStr, headLen);
  headLen = stripDuplicateSpaces(bufTmp.data, headLen);
  bufTmp.Resize(headLen);

  while (bufTmp.len > 1) {
    if (istoksep(bufTmp.data[bufTmp.len - 1])) {
      break;
    }
    bufTmp.len--;
  }

  bufTmp.len = trimTrailingSpaces(bufTmp.data, bufTmp.len);
  char *ret = bufTmp.Steal(docLen);
  return ret;
}

//---------------------------------------------------------------------------------------------

static RSValue *summarizeField(IndexSpec *spec, const ReturnedField *fieldInfo,
                               const char *fieldName, const RSValue *returnedField,
                               hlpDocContext *docParams, int options) {

  FragmentList frags(8, 6);

  // Start gathering the terms
  HighlightTags tags = fieldInfo->highlightSettings;

  // First actually generate the fragments
  size_t docLen;
  const char *docStr = RSValue_StringPtrLen(returnedField, &docLen);
  if (docParams->byteOffsets == NULL ||
      !fragmentizeOffsets(spec, fieldName, docStr, docLen, docParams->indexResult,
                          docParams->byteOffsets, &frags, options)) {
    if (fieldInfo->mode == SummarizeMode_Synopsis) {
      // If summarizing is requested then trim the field so that the user isn't
      // spammed with a large blob of text
      char *summarized = trimField(fieldInfo, docStr, &docLen, frags.estAvgWordSize);
      return RS_StringVal(summarized, docLen);
    } else {
      // Otherwise, just return the whole field, but without highlighting
    }
    return NULL;
  }

  // Highlight only
  if (fieldInfo->mode == SummarizeMode_Highlight) {
    // No need to return snippets; just return the entire doc with relevant tags
    // highlighted.
    char *hlDoc = frags.HighlightWholeDocS(&tags);
    return RS_StringValC(hlDoc);
  }

  size_t numIovArr = Min(fieldInfo->summarizeSettings.numFrags, &frags->GetNumFrags());
  for (size_t i = 0; i < numIovArr; ++i) {
    docParams->iovsArr[i].Resize(0);
  }

  frags.HighlightFragments(&tags, fieldInfo->summarizeSettings.contextLen,
                           docParams->iovsArr, numIovArr, HIGHLIGHT_ORDER_SCOREPOS);

  // Buffer to store concatenated fragments
  Array bufTmp = new Array(ArrayAlloc_RM);

  for (size_t i = 0; i < numIovArr; ++i) {
    Array *curIovs = docParams->iovsArr + i;
    struct iovec *iovs = curIovs->ARRAY_GETARRAY_AS(struct iovec *);
    size_t numIovs = curIovs->ARRAY_GETSIZE_AS(struct iovec);
    size_t lastSize = bufTmp.len;

    for (size_t j = 0; j < numIovs; ++j) {
      bufTmp.Write(iovs[j].iov_base, iovs[j].iov_len);
    }

    // Duplicate spaces for the current snippet are eliminated here. We shouldn't
    // move it to the end because the delimiter itself may contain a special kind
    // of whitespace.
    size_t newSize = stripDuplicateSpaces(bufTmp.data + lastSize, bufTmp.len - lastSize);
    bufTmp.Resize(lastSize + newSize);
    bufTmp.Write(fieldInfo->summarizeSettings.separator,
                 strlen(fieldInfo->summarizeSettings.separator));
  }

  // Set the string value to the contents of the array. It might be nice if we didn't
  // need to strndup it.
  size_t hlLen;
  char *hlText = bufTmp.Steal(&hlLen);
  return RS_StringVal(hlText, hlLen);
}

//---------------------------------------------------------------------------------------------

static void resetIovsArr(Array **iovsArrp, size_t *curSize, size_t newSize) {
  if (*curSize < newSize) {
    *iovsArrp = rm_realloc(*iovsArrp, sizeof(**iovsArrp) * newSize);
  }
  for (size_t ii = 0; ii < *curSize; ++ii) {
    Array_Resize((*iovsArrp) + ii, 0);
  }
  for (size_t ii = *curSize; ii < newSize; ++ii) {
    Array_Init((*iovsArrp) + ii);
  }
  *curSize = newSize;
}

//---------------------------------------------------------------------------------------------

static void Highlighter::processField(hlpDocContext *docParams, ReturnedField *spec) {
  const char *fName = spec->name;
  const RSValue *fieldValue = RLookup_GetItem(spec->lookupKey, docParams->row);

  if (fieldValue == NULL || !RSValue_IsString(fieldValue)) {
    return;
  }
  RSValue *v = summarizeField(RP_SPEC(&hlpCtx->base), spec, fName, fieldValue, docParams,
                              hlpCtx->fragmentizeOptions);
  if (v) {
    RLookup_WriteOwnKey(spec->lookupKey, docParams->row, v);
  }
}

//---------------------------------------------------------------------------------------------

const IndexResult *Highlighter::getIndexResult(t_docId docId) {
  IndexIterator *it = QITR_GetRootFilter(parent);
  IndexResult *ir;
  it->Rewind();
  if (INDEXREAD_OK != it->SkipTo(docId, &ir)) {
    return NULL;
  }
  return ir;
}

//---------------------------------------------------------------------------------------------

int Highlighter::Next(SearchResult *r) {
  int rc = upstream->Next(r);
  if (rc != RS_RESULT_OK) {
    return rc;
  }

  // Get the index result for the current document from the root iterator.
  // The current result should not contain an index result
  const IndexResult *ir = r->indexResult ? r->indexResult : getIndexResult(rbase, r->docId);

  // we can't work withot the inex result, just return QUEUED
  if (!ir) {
    return RS_RESULT_OK;
  }

  size_t numIovsArr = 0;
  RSDocumentMetadata *dmd = r->dmd;
  if (!dmd) {
    return RS_RESULT_OK;
  }

  hlpDocContext docParams = {byteOffsets: dmd->byteOffsets,  // nl
                             indexResult: ir,
                             iovsArr: NULL,
                             row: &r->rowdata};

  if (fields->numFields) {
    for (size_t i = 0; i < fields->numFields; ++i) {
      const ReturnedField *ff = &fields->fields[i];
      if (ff->mode == SummarizeMode_None && fields->defaultField.mode == SummarizeMode_None) {
        // Ignore - this is a field for `RETURN`, not `SUMMARIZE`
        continue;
      }
      ReturnedField combinedSpec;
      normalizeSettings(ff, &fields->defaultField, &combinedSpec);
      resetIovsArr(&docParams.iovsArr, &numIovsArr, combinedSpec.summarizeSettings.numFrags);
      processField(hlp, &docParams, &combinedSpec);
    }
  } else if (fields->defaultField.mode != SummarizeMode_None) {
    for (const RLookupKey *k = hlp->lookup->head; k; k = k->next) {
      if (k->flags & RLOOKUP_F_HIDDEN) {
        continue;
      }
      ReturnedField spec;
      normalizeSettings(NULL, &fields->defaultField, &spec);
      spec.lookupKey = k;
      spec.name = k->name;
      resetIovsArr(&docParams.iovsArr, &numIovsArr, spec.summarizeSettings.numFrags);
      processField(&docParams, &spec);
    }
  }
  for (size_t i = 0; i < numIovsArr; ++i) {
    delete &docParams.iovsArr[i];
  }
  rm_free(docParams.iovsArr);
  return RS_RESULT_OK;
}

//---------------------------------------------------------------------------------------------

Highlighter::Highlighter(const RSSearchOptions *searchopts, const FieldList *fields_,
                           const RLookup *lookup_) {
  if (searchopts->language == RS_LANG_CHINESE) {
    fragmentizeOptions = FRAGMENTIZE_TOKLEN_EXACT;
  }
  Next = hlpNext;
  fields = fields_;
  lookup = lookup_;
}

///////////////////////////////////////////////////////////////////////////////////////////////
