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
 * RoomyList.h
 *
 * A list with an arbitrary number of elements. Each element is the same size,
 * that size being an arbitrary number of bytes.
 *****************************************************************************/

#ifndef _ROOMYLIST_H
#define _ROOMYLIST_H

#include "types.h"
#include "WriteBuffer.h"

// put global defines from roomy.h here to avoid cirular includes
#define RGLO_STR_SIZE 1024
#define RGLO_MAX_PREDICATES  1024

/*************************************************************************
                           DATA STRUCTURES
*************************************************************************/

// The primary RoomyList data structure

typedef struct {
    // Global description
    char name[RGLO_STR_SIZE]; // unique name (used primarily for file naming)
    uint64 bytesPer;          // number of bytes per element
    uint64 updateSize;        // number of bytes in add/remove op

    // Local description
    char lDirName[RGLO_STR_SIZE]; // directory storing all data
    int isSorted;                 // true if the local chunk is in sorted order
    uint64 lSize;                 // number of elements on this node

    // Write buffers for adding and removing elements.
    WriteBuffer* addWBS;
    WriteBuffer* removeWBS;
    // true if write buffers are open
    int buffersOpen;
    
    // User supplied predicate functions
    uint64 numPredicates;
    uint8 (*predFuncs[RGLO_MAX_PREDICATES])(void* val);
    uint64 predicateVals[RGLO_MAX_PREDICATES];

    // User supplied key definition
    uint64 keyStart;
    uint64 keySize;

    // The number of times this RoomyList has been sync'ed
    uint64 numSyncs;

    // True if there are no outstanding delayed operations, false otherwise.
    int isSynced;

    int destroyed;  // set to 1 when the structure has been destroyed.
                    // a global copy of this struct is kept for statistics
} RoomyList;

/*************************************************************************
                          FUNCTION HEADERS 
*************************************************************************/

RoomyList* RoomyList_make(char* name, uint64 bytesPerElt);
RoomyList* RoomyList_makeWithKey(char* name, uint64 bytesPerElt, uint64 keyStart,
                                 uint64 keySize);
void RoomyList_destroy(RoomyList* rl);
void RoomyList_add(RoomyList* rl, void* elt);
void RoomyList_remove(RoomyList* rl, void* elt);
void RoomyList_map(RoomyList* rl, void (*mapFunc)(void* val));
void RoomyList_mapAndModify(RoomyList *rl,
                     void (*mapFunc)(void* oldVal, void* newValOut));
void RoomyList_reduce(RoomyList *rl,
                   void* ansInOut,
                   uint64 bytesInAns,
                   void (*mergeValAndAns)(void* ansInOut, void* val),
                   void (*mergeAnsAndAns)(void* ansInOut, void* ansIn));
void RoomyList_attachPredicate(RoomyList* rl, uint8 (*predFunc)(void* val));
uint64 RoomyList_predicateCount(RoomyList* rl, uint8 (*predFunc)(void* val));
uint64 RoomyList_size(RoomyList* rl);
void RoomyList_removeDupes(RoomyList* rl);
void RoomyList_addAll(RoomyList* rl1, RoomyList* rl2);
void RoomyList_removeAll(RoomyList* rl1, RoomyList* rl2);
void RoomyList_sync(RoomyList* rl);
int RoomyList_isSynced(RoomyList* rl);

/*************************************************************************
 * The methods below are only used internally by other Roomy code, and are
 * not a part of the user level API.
 ************************************************************************/

uint64 RoomyList_mergeUpdates(RoomyList* rl);
void RoomyList_getListFileName(RoomyList* rl, int node, char* ans);

#endif

