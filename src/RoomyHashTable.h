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
 * RoomyHashTable.h
 *
 * A parallel disk-based hash table. Stores (key,value) pairs. It is basically
 * a RoomyArray that is indexed by an arbitrary key instead of an integer.
 *****************************************************************************/

#ifndef _ROOMYHASHTABLE_H
#define _ROOMYHASHTABLE_H

#include "types.h"
#include "WriteBuffer.h"

// put global defines from roomy.h here to avoid cirular includes
#define RGLO_STR_SIZE 1024
#define RGLO_MAX_REGISTERED_FUNC 1024
#define RGLO_MAX_PREDICATES  1024

/*************************************************************************
                           DATA STRUCTURES
*************************************************************************/

// The primary RoomyHashTable data structure

typedef struct {
    // Global description
    char name[RGLO_STR_SIZE]; // unique name (used primarily for file naming)
    uint64 keySize;           // number of bytes in keys
    uint64 valueSize;         // number of bytes in values
    uint64 subTables;         // number of HashTables composing this RoomyHashTable
    uint64 size;              // number of elements in table (updated after
                              // each sync)

    // Local description
    uint64 lSize;          // number of elements on this node
    uint64 lSubTables;     // number of sub tables on this node
    char lDirName[RGLO_STR_SIZE]; // directory storing all data

    // User supplied access/update functions.
    int numAccessTypes;
    void (*accessFuncs[RGLO_MAX_REGISTERED_FUNC])
                    (void* key, void* tableValue, void* passedValue);
    uint64 accessSizes[RGLO_MAX_REGISTERED_FUNC];

    int numUpdateTypes;
    void (*updateFuncs[RGLO_MAX_REGISTERED_FUNC])
                    (void* key, void* oldValue, void* passedValue, void* newValOut);
    uint64 updateSizes[RGLO_MAX_REGISTERED_FUNC];

    // Write buffers for accesses/updates. The first index is the function index,
    // the second is the subtable.
    WriteBuffer* accessWBS[RGLO_MAX_REGISTERED_FUNC];
    WriteBuffer* updateWBS[RGLO_MAX_REGISTERED_FUNC];

    // Write buffers for remove operations, indexed by subtable.
    WriteBuffer* removeWBS;

    // True if buffers are open for writing for a given function.
    int accessBuffersOpen[RGLO_MAX_REGISTERED_FUNC];
    int updateBuffersOpen[RGLO_MAX_REGISTERED_FUNC];
    int removeBuffersOpen;
    
    // User supplied predicate functions.
    uint64 numPredicates;
    uint8 (*predFuncs[RGLO_MAX_PREDICATES])(void* key, void* val);
    uint64 predicateVals[RGLO_MAX_PREDICATES];

    // The number of times this RoomyHashTable has been sync'ed
    uint64 numSyncs;

    // True if there are no outstanding delayed operations, false otherwise.
    int isSynced;

    int destroyed;  // set to 1 when the structure has been destroyed.
                    // a global copy of this struct is kept for statistics.
} RoomyHashTable;

/*************************************************************************
                          FUNCTION HEADERS 
*************************************************************************/

RoomyHashTable* RoomyHashTable_make(char* name, int keySize, int valueSize,
                                    uint64 capacity);

void RoomyHashTable_destroy(RoomyHashTable* rht);

uint64 RoomyHashTable_size(RoomyHashTable* rht);

void RoomyHashTable_map(RoomyHashTable* rht,
                        void (*mapFunc)(void* key, void* value));

void RoomyHashTable_reduce(
  RoomyHashTable *rht,
  void* ansInOut,
  uint64 bytesInAns,
  void (*mergeValAndAns)(void* ansInOut, void* key, void* value),
  void (*mergeAnsAndAns)(void* ansInOut, void* ansIn));

void RoomyHashTable_registerAccessFunc(
  RoomyHashTable* rht,
  void (*accessFunc)(void* key, void* tableValue, void* passedValue),
  uint64 passedValSize);

void RoomyHashTable_access(
  RoomyHashTable* rht, void* key, void* passedValue,
  void (*accessFunc)(void* key, void* tableValue, void* passedValue));

void RoomyHashTable_registerUpdateFunc(
  RoomyHashTable* rht,
  void (*updateFunc)(void* key, void* oldValue, void* passedValue, void* newValOut),
  uint64 passedValSize);

void RoomyHashTable_update(
  RoomyHashTable* rht, void* key, void* passedValue,
  void (*updateFunc)
    (void* key, void* oldValue, void* passedValue, void* newValOut));

void RoomyHashTable_insert(RoomyHashTable* rht, void* key, void* value);

void RoomyHashTable_remove(RoomyHashTable* rht, void* key);

void RoomyHashTable_attachPredicate(RoomyHashTable* rht,
                                    uint8 (*predFunc)(void* key, void* value));

uint64 RoomyHashTable_predicateCount(RoomyHashTable* rht,
                                     uint8 (*predFunc)(void* key, void* value));

void RoomyHashTable_sync(RoomyHashTable* rht);

int RoomyHashTable_isSynced(RoomyHashTable* rht);

#endif
