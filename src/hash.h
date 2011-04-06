/******************************************************************************
 * hash.h
 *
 * The Hsieh hash function, from http://www.azillionmonkeys.com/qed/hash.html
 *****************************************************************************/

#ifndef _HASH_H
#define _HASH_H

#include "types.h"

uint32 hsiehHash(const char * data, int len);
uint32 oaatHash(const char *key, uint32 len);

#endif
