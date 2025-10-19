#include "hash.h"
#include <boost/uuid/detail/sha1.hpp>

void Sha1_Compute(const char *value, size_t len, Sha1* output) {
  boost::uuids::detail::sha1 sha1;
  sha1.process_bytes(value, len);
  sha1.get_digest(output->hash);
}

void Sha1_FormatIntoBuffer(const Sha1 *sha1, char *buffer) {
  for (int i = 0; i < 5; i++) {
    uint32_t word = (sha1->hash[i*4] << 24) | (sha1->hash[i*4+1] << 16) | (sha1->hash[i*4+2] << 8)| sha1->hash[i*4+3];
    sprintf(buffer + i * 8, "%08x", sha1->hash[i]);
  }
  buffer[40] = '\0';
}
