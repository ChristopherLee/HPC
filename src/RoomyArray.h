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
 * RoomyArray.h
 *
 * A disk-based array. The array contains a fixed number of elements, each
 * element being the same size. Each element can contain between 1 and 7 bits,
 * or any number of bytes.
 *****************************************************************************/

#ifndef _ROOMYARRAY_H
#define _ROOMYARRAY_H

#include "types.h"
#include "WriteBuffer.h"
#include "RoomyList.h"

// put global defines from roomy.h here to avoid cirular includes
#define RGLO_STR_SIZE 1024
#define RGLO_MAX_UPDATE_FUNC 1024
#define RGLO_MAX_PREDICATES  1024

/*************************************************************************
                           DATA STRUCTURES
*************************************************************************/

// The RoomyArray can store: 1 or more bytes; between 1 and 7 bits; or a single
// bit per element.

#define SIZE_BYTES 0
#define SIZE_BITS 1
#define SIZE_BIT 2

// The primary RoomyArray data structure

typedef struct {
    int eltType;    // one of SIZE_BYTES, SIZE_BITS, SIZE_BIT

    // Global description
    char name[RGLO_STR_SIZE]; // unique name (used primarily for file naming)
    uint64 elts;              // number of elements
    uint64 bytesPer;          // number of bytes per element (if SIZE_BYTES)
    uint64 bitsPer;           // number of bits per element (if SIZE_BITS or
                              // SIZE_BIT)
    uint64 bytes;             // number of total bytes in array

    // Local description
    uint64 lElts;          // number of elements on this node
    uint64 lBytes;         // number of bytes on this node
    uint64 lChunks;        // number of chunks on this node
    uint64 lChunkSize;     // size of normal chunk on this node (in bytes)
    uint64 lChunkElts;     // number of elements in a normal chunk
    uint64 lChunkEltsAllNodes; // lChunkElts * RGLO_NUM_SLAVES (pre-calculated to
                               // save ops in commonly used index transformations).
    uint64 lLastChunkSize; // size of the last chunk (can be smaller than others)
    char lDirName[RGLO_STR_SIZE]; // directory storing all data

    // User supplied update functions
    uint64 numUpdateTypes;
    void (*updateFuncs[RGLO_MAX_UPDATE_FUNC])
                    (uint64 i, void* oldVal, void* updateVal, void* newValOut);
    uint64 updateSizes[RGLO_MAX_UPDATE_FUNC];

    // Write buffers for updates. Two dimensional array: the first index is the
    // update type, the second is the destination node.
    WriteBuffer* updateWBS[RGLO_MAX_UPDATE_FUNC];
    // true if buffers are open for writing
    int updateBuffersOpen[RGLO_MAX_UPDATE_FUNC];       
    
    // User supplied access functions
    uint64 numAccessTypes;
    void (*accessFuncs[RGLO_MAX_UPDATE_FUNC])
                    (uint64 i, void* arrayVal, void* passedVal);
    uint64 accessSizes[RGLO_MAX_UPDATE_FUNC];

    // Write buffers for accesses. The first index is the access type, the
    // second is the destination node.
    WriteBuffer* accessWBS[RGLO_MAX_UPDATE_FUNC];
    // true if buffers are open for writing
    int accessBuffersOpen[RGLO_MAX_UPDATE_FUNC];

    // User supplied predicate functions
    uint64 numPredicates;
    uint8 (*predFuncs[RGLO_MAX_PREDICATES])(uint64 i, void* val);
    uint64 predicateVals[RGLO_MAX_PREDICATES];

    // The number of times this RoomyArray has been sync'ed
    uint64 numSyncs;

    // True if there are no outstanding delayed operations, false otherwise.
    int isSynced;

    int destroyed;  // set to 1 when the structure has been destroyed.
                    // a global copy of this struct is kept for statistics
} RoomyArray;

// Delayed updates to array

typedef struct {
    uint64 index;
    void* val;
} RA_Update;

/*************************************************************************
                          FUNCTION HEADERS 
*************************************************************************/

RoomyArray* RoomyArray_makeBytes(char* name, uint64 bytesPerElt, uint64 numElts);
RoomyArray* RoomyArray_makeBits(char* name, uint64 bitsPerElt, uint64 numElts);
RoomyArray* RoomyArray_makeFromList(char* name, RoomyList* rl);
void RoomyArray_destroy(RoomyArray* ra);
uint64 RoomyArray_size(RoomyArray* ra);
void RoomyArray_map(RoomyArray* ra, void (*mapFunc)(uint64 i, void* val));
void RoomyArray_mapAndModify(
    RoomyArray *ra,
    void (*mapFunc)(uint64 i, void* oldVal, void* newValOut));
void RoomyArray_reduce(
    RoomyArray *ra,
    void* ansInOut,
    uint64 bytesInAns,
    void (*mergeValAndAns)(void* ansInOut, uint64 i, void* val),
    void (*mergeAnsAndAns)(void* ansInOut, void* ansIn));
void RoomyArray_registerUpdateFunc(
    RoomyArray* ra,
    void (*updateFunc)(uint64 i, void* oldVal, void* updateVal, void* newValOut),
    uint64 updateValSize);
void RoomyArray_update(RoomyArray *ra, uint64 i, void* updateVal,
    void (*updateFunc)(uint64 i, void* oldVal, void* updateVal, void* newValOut));
void RoomyArray_registerAccessFunc(RoomyArray* ra,
    void (*accessFunc)(uint64 i, void* arrayVal, void* passedVal),
    uint64 passedValSize);
void RoomyArray_access(RoomyArray *ra, uint64 i, void* passedVal,
    void (*accessFunc)(uint64 i, void* arrayVal, void* passedVal));
void RoomyArray_attachPredicate(RoomyArray* ra,
                                uint8 (*predFunc)(uint64 i, void* val));
uint64 RoomyArray_predicateCount(RoomyArray* ra,
                                 uint8 (*predFunc)(uint64 i, void* val));
void RoomyArray_sync(RoomyArray* ra);
int RoomyArray_isSynced(RoomyArray* ra);

void RoomyArray_permute(RoomyArray* X, RoomyArray* Y, RoomyArray* Z);
RoomyArray* RoomyArray_makeRandomPerm(char* name, uint64 numElts);

// Private methods

RoomyArray* RoomyArray_makeType(int eltType, char* name, uint64 bytesOrBytesPerElt,
                                uint64 numElts);
void RoomyArray_processDelayedOps(RoomyArray* ra);
void RoomyArray_performAccess(RoomyArray* ra, void* chunk, int chunkNum);
int RoomyArray_mergeUpdates(RoomyArray* ra, void* chunk, int chunkNum);

#endif
