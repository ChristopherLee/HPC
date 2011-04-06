/******************************************************************************
 *  Copyright (C) 2009 Dan Kunkle (kunkle@gmail.com)
 *
 *  This program is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation, either version 3 of the License, or
 *  (at your option) any later version.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *****************************************************************************/

/******************************************************************************
 * HashTable.h
 *
 * An open addressing hash table. Uses linear probing and lazy deletion.
 *****************************************************************************/

#ifndef _HASHTABLE_H
#define _HASHTABLE_H

#include "Array.h"
#include "BitsArray.h"
#include "types.h"

/*************************************************************************
                             DATA TYPES 
*************************************************************************/

/* a hash table */
typedef struct {
    int keySize;        // number of bytes in a key
    int valueSize;      // number of bytes in a value
    int eltSize;        // combined size of key and value
    int capacity;       // total number of slots
    int size;           // current number of elements
    int nonOpenSlots;   // number of USED and DELETED slots
    double loadFactor;  // if nonOpenSlots / capacity > loadFactor, the size
                        // of the table is doubled, and all elements are rehased
    Array array;        // stores (key,value) pairs
    BitsArray state;    // two bis of meta-data per slot, specifying if the slot
                        // is OPEN, USED, or DELETED
    int mapped;         // 1 if array and state are mmap'ed from files
    char *filename;     // if mapped == 1, the filename being used
} HashTable;

/*************************************************************************
                          FUNCTION HEADERS 
*************************************************************************/

/* Public API */

HashTable HashTable_make(int keySize, int valueSize, int capacity);
void HashTable_fileSave(HashTable *htb, char* filename);
HashTable HashTable_makeFromFiles(char* filename);
HashTable HashTable_makeMapped(char* filename);
int HashTable_removeFiles(char* filename);
void HashTable_insert(HashTable *htb, void *key, void *value);
void* HashTable_get(HashTable *htb, void *key);
int HashTable_hasKey(HashTable *htb, void *key);
void HashTable_delete(HashTable *htb, void *key);
int HashTable_size(HashTable *htb);
int HashTable_capacity(HashTable *htb);
int64 HashTable_bytesUsed(HashTable *htb);
void HashTable_destroy(HashTable *htb);
void HashTable_map(HashTable *htb, void (*mapFunc)(void *key, void *value));
void HashTable_reduce(
        HashTable *htb,
        void* ansInOut,
        void (*mergeValAndAns)(void* ansInOut, void *key, void *value));

/* Private methods */

void HashTable_fileSaveMetaData(HashTable *htb, char *filename);
void HashTable_fileLoadMetaData(HashTable *htb, char *filename);
void HashTable_mapArrays(HashTable *htb, char *filename);
int HashTable_lookup(HashTable *htb, void *key, int *state);
int HashTable_nextGoodCapacity(int n);
void HashTable_doubleCapacity(HashTable *htb);

#endif
