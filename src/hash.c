/******************************************************************************
 * hash.c
 *
 * The Hsieh hash function (see http://www.azillionmonkeys.com/qed/hash.html)
 *
 * The Jenkins One-at-a-Time hash function (see
 * http://burtleburtle.net/bob/hash/doobs.html)
 *****************************************************************************/

#include "hash.h"
#include "types.h"

#include <assert.h>
#include <stdlib.h>

#define get16bits(d) (*((const uint16*) (d)))
uint32 hsiehHash(const char * data, int len) {
    uint32 hash = len, tmp;
    int rem;

    assert(len > 0 && data != NULL);

    rem = len & 3;
    len >>= 2;

    /* Main loop */
    for (;len > 0; len--) {
        hash  += get16bits (data);
        tmp    = (get16bits (data+2) << 11) ^ hash;
        hash   = (hash << 16) ^ tmp;
        data  += 2*sizeof (uint16);
        hash  += hash >> 11;
    }

    /* Handle end cases */
    switch (rem) {
        case 3: hash += get16bits (data);
                hash ^= hash << 16;
                hash ^= data[sizeof (uint16)] << 18;
                hash += hash >> 11;
                break;
        case 2: hash += get16bits (data);
                hash ^= hash << 11;
                hash += hash >> 17;
                break;
        case 1: hash += *data;
                hash ^= hash << 10;
                hash += hash >> 1;
    }

    /* Force "avalanching" of final 127 bits */
    hash ^= hash << 3;
    hash += hash >> 5;
    hash ^= hash << 4;
    hash += hash >> 17;
    hash ^= hash << 25;
    hash += hash >> 6;

    return hash;
}

uint32 oaatHash(const char *key, uint32 len) {
    assert(len > 0 && key != NULL);

    uint32 hash, i;
    for (hash=0, i=0; i<len; ++i) {
        hash += key[i];
        hash += (hash << 10);
        hash ^= (hash >> 6);
    }

    hash += (hash << 3);
    hash ^= (hash >> 11);
    hash += (hash << 15);

    return hash;
} 
