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
 * RoomyHashTable.c
 *
 * A parallel disk-based hash table. Stores (key,value) pairs. It is basically
 * a RoomyArray that is indexed by an arbitrary key instead of an integer.
 *****************************************************************************/

#include "RoomyHashTable.h"

#include "ReadBuffer.h"
#include "WriteBuffer.h"
#include "HashTable.h"
#include "hash.h"
#include "knudirs.h"
#include "knustring.h"
#include "knusort.h"
#include "params.h"
#include "roomy.h"
#include "types.h"

#include <sys/stat.h>
#include <string.h>
#include <unistd.h>

/******************************************************************************
 *                         Index Transformations
 * Sub-tables are evenly distributed among compute nodes.
 * Elements are distributed among sub-tables based on one hash fuction (oaatHash).
 * Elements are placed within the sub-tables based on another hash function
 * (hsiehHash).
 *****************************************************************************/

// Determine which sub-table an element with the given key belongs to.
uint32 RoomyHashTable_hash2subtable(RoomyHashTable* rht, void* key) {
    return oaatHash(key, rht->keySize) % rht-> subTables;
}

// Determine which node owns the given sub-table.
uint32 RoomyHashTable_subtable2node(RoomyHashTable* rht, uint32 subtable) {
    return subtable % RGLO_NUM_SLAVES;
}

/******************************************************************************
 *                              File Naming
 *****************************************************************************/

// Get the name of the file for the given subtable.
void RoomyHashTable_getSubtableName(RoomyHashTable* rht,
                                    uint32 subtable,
                                    char* ans) {
    sprintf(ans, "%s/subtable_%i", rht->lDirName, subtable);
}

// Get the name of the buffer file for updates to the given subtable.
void RoomyHashTable_getUpdateName(RoomyHashTable* rht,
                                  int updateNum,
                                  uint64 syncNum,
                                  int sourceNode,
                                  uint32 subtable,
                                  char* ans) {
    sprintf(ans, "%s/update%i_sync%lli_from%i_subtable%i",
            rht->lDirName, updateNum, syncNum, sourceNode, subtable);
}

// Get the name of the buffer file for accesses to the given subtable.
void RoomyHashTable_getAccessName(RoomyHashTable* rht,
                                  int accessNum,
                                  uint64 syncNum,
                                  int sourceNode,
                                  uint32 subtable,
                                  char* ans) {
    sprintf(ans, "%s/access%i_sync%lli_from%i_subtable%i",
            rht->lDirName, accessNum, syncNum, sourceNode, subtable);
}

// Get the name of the buffer file for remove operations to the given subtable.
void RoomyHashTable_getRemoveName(RoomyHashTable* rht,
                                  uint64 syncNum,
                                  int sourceNode,
                                  uint32 subtable,
                                  char* ans) {
    sprintf(ans, "%s/remove_sync%lli_from%i_subtable%i",
            rht->lDirName, syncNum, sourceNode, subtable);
}

/******************************************************************************
 *                          Buffer Open / Close 
 *****************************************************************************/

// Open one WriteBuffer per subtable to store delayed updates of the given kind
void RoomyHashTable_openUpdateBuffers(RoomyHashTable* rht, int update) {
    #ifdef DEBUG
    Roomy_logAny("Opening update buffers %s\n", rht->name);
    #endif

    // make one buffer for each subtable
    rht->updateWBS[update] = malloc(sizeof(WriteBuffer) * rht->subTables);
    char name[RGLO_STR_SIZE];
    int subtable;
    for (subtable = 0; subtable < rht->subTables; subtable++) {
        RoomyHashTable_getUpdateName(
            rht, update, rht->numSyncs, RGLO_MY_RANK, subtable, name);
        int node = RoomyHashTable_subtable2node(rht, subtable);

        // make a local buffer if on this node, or global disk is shared
        if(node == RGLO_MY_RANK || RPAR_SHARED_DISK) {
            rht->updateWBS[update][subtable] = WriteBuffer_make(
                    RGLO_BUFFER_SIZE, rht->updateSizes[update], name, 0);
        } else {
            rht->updateWBS[update][subtable] = WriteBuffer_makeRemote(
                    RGLO_BUFFER_SIZE, rht->updateSizes[update], name, node);
        }
    }
    rht->updateBuffersOpen[update] = 1;
}

// Close and destroy all WriteBuffers for all types of updates
void RoomyHashTable_closeUpdateBuffers(RoomyHashTable* rht) {
    #ifdef DEBUG
    Roomy_logAny("Closing update buffers %s\n", rht->name);
    #endif

    int update, subtable;
    for (update = 0; update < rht->numUpdateTypes; update++) {
        if (rht->updateBuffersOpen[update]) {
            for (subtable = 0; subtable < rht->subTables; subtable++) {
                WriteBuffer_destroy(&(rht->updateWBS[update][subtable]));
            }
            free(rht->updateWBS[update]);
            rht->updateBuffersOpen[update] = 0;
        }
    }
}

// Open one WriteBuffer per subtable to store delayed accesses of the given kind.
void RoomyHashTable_openAccessBuffers(RoomyHashTable* rht, int access) {
    #ifdef DEBUG
    Roomy_logAny("Opening access buffers %s\n", rht->name);
    #endif

    // make one buffer for each subtable
    rht->accessWBS[access] = malloc(sizeof(WriteBuffer) * rht->subTables);
    char name[RGLO_STR_SIZE];
    int subtable;
    for (subtable = 0; subtable < rht->subTables; subtable++) {
        RoomyHashTable_getAccessName(
            rht, access, rht->numSyncs, RGLO_MY_RANK, subtable, name);
        int node = RoomyHashTable_subtable2node(rht, subtable);
        // make a local buffer if on this node, or global disk is shared
        if(node == RGLO_MY_RANK || RPAR_SHARED_DISK) {
            rht->accessWBS[access][subtable] = WriteBuffer_make(
                    RGLO_BUFFER_SIZE, rht->accessSizes[access], name, 0);
        } else {
            rht->accessWBS[access][subtable] = WriteBuffer_makeRemote(
                    RGLO_BUFFER_SIZE, rht->accessSizes[access], name, node);
        }
    }
    rht->accessBuffersOpen[access] = 1;
}

// Close and destroy all WriteBuffers for all types of accesses.
void RoomyHashTable_closeAccessBuffers(RoomyHashTable* rht) {
    #ifdef DEBUG
    Roomy_logAny("Closing access buffers %s\n", rht->name);
    #endif

    int access, subtable;
    for (access = 0; access < rht->numAccessTypes; access++) {
        if (rht->accessBuffersOpen[access]) {
            for (subtable = 0; subtable < rht->subTables; subtable++) {
                WriteBuffer_destroy(&(rht->accessWBS[access][subtable]));
            }
            free(rht->accessWBS[access]);
            rht->accessBuffersOpen[access] = 0;
        }
    }
}

// Open one WriteBuffer per subtable to store delayed remove operations.
void RoomyHashTable_openRemoveBuffers(RoomyHashTable* rht) {
    #ifdef DEBUG
    Roomy_logAny("Opening remove buffers %s\n", rht->name);
    #endif

    // make one buffer for each subtable
    rht->removeWBS = malloc(sizeof(WriteBuffer) * rht->subTables);
    char name[RGLO_STR_SIZE];
    int subtable;
    for (subtable = 0; subtable < rht->subTables; subtable++) {
        RoomyHashTable_getRemoveName(
            rht, rht->numSyncs, RGLO_MY_RANK, subtable, name);
        int node = RoomyHashTable_subtable2node(rht, subtable);
        // make a local buffer if on this node, or global disk is shared
        if(node == RGLO_MY_RANK || RPAR_SHARED_DISK) {
            rht->removeWBS[subtable] = WriteBuffer_make(
                RGLO_BUFFER_SIZE, rht->keySize, name, 0);
        } else {
            rht->removeWBS[subtable] = WriteBuffer_makeRemote(
                RGLO_BUFFER_SIZE, rht->keySize, name, node);
        }
    }
    rht->removeBuffersOpen = 1;
}

// Close and destroy all WriteBuffers for removes.
void RoomyHashTable_closeRemoveBuffers(RoomyHashTable* rht) {
    #ifdef DEBUG
    Roomy_logAny("Closing remove buffers %s\n", rht->name);
    #endif

    if (rht->removeBuffersOpen) {
        int subtable;
        for (subtable = 0; subtable < rht->subTables; subtable++) {
            WriteBuffer_destroy(&(rht->removeWBS[subtable]));
        }
        free(rht->removeWBS);
        rht->removeBuffersOpen = 0;
    }
}

/******************************************************************************
 *                       Delayed Operation Processing
 *****************************************************************************/

// Return the given subtable as a HashTable.
HashTable RoomyHashTable_loadSubtable(RoomyHashTable* rht, uint64 subtableNum) {
    char name[RGLO_STR_SIZE];
    RoomyHashTable_getSubtableName(rht, subtableNum, name);
    return HashTable_makeMapped(name);
}

// Save the state of the given subtable and destroy the given HashTable.
void RoomyHashTable_saveSubtable(HashTable* ht) {
    HashTable_destroy(ht); // destruction saves an mmap'ed HashTable
}

// Process delayed access operations.
void RoomyHashTable_processAccess(RoomyHashTable* rht,
                                  HashTable* subtable,
                                  uint64 subtableNum) {
    #ifdef DEBUG
    Roomy_logAny("Performing accesses for %s\n", rht->name);
    #endif

    uint64 bytesDeleted = 0;

    char filename[RGLO_STR_SIZE];
    int srcNode, type;
    for(type = 0; type < rht->numAccessTypes; type++) {
        for(srcNode=0; srcNode<RGLO_NUM_SLAVES; srcNode++) {
            // Process ops from the last sync, new delayed ops go to the next sync
            RoomyHashTable_getAccessName(
                rht, type, rht->numSyncs-1, srcNode, subtableNum, filename);
            if(!fileExists(filename)) continue;
            bytesDeleted += numBytesInFile(filename);
            ReadBuffer buf = ReadBuffer_make(RGLO_BUFFER_SIZE,
                                             rht->accessSizes[type],
                                             filename, 1);
            while(ReadBuffer_hasMore(&buf)) {
                // perform op
                char* key = (char*)ReadBuffer_current(&buf); // use char* for
                                                             // ptr arithmetic
                char* passedValue = key + rht->keySize;
                rht->accessFuncs[type](key,
                                       HashTable_get(subtable, key),
                                       passedValue);
                ReadBuffer_next(&buf);
            }
            ReadBuffer_destroy(&buf);
            unlink(filename);
        }
    }

    // space accounting
    Roomy_dataRemoved(bytesDeleted);
}

// A default update function that is registered when the RoomyHashTable is
// constructed. This function is not actually called, it is just a place holder
// so internal Roomy code can specify that it wants to update a given
// key of the RoomyHashTable with a specific value. The reason it can't be
// directly called is that it doesn't know the size of the elements.
void RoomyHashTable_defaultUpdateFunc(void* key, void* oldVal, void* passedVal,
                                      void* newValOut) {
    Roomy_log("ERROR: RoomyHashTable_defaultUpdateFunc called\n");
    exit(1);
}

// Process delayed update operations.
void RoomyHashTable_processUpdate(RoomyHashTable* rht,
                                  HashTable* subtable,
                                  uint64 subtableNum) {

    #ifdef DEBUG
    Roomy_logAny("Performing updates for %s\n", rht->name);
    #endif

    uint64 bytesDeleted = 0;

    char filename[RGLO_STR_SIZE];
    int srcNode, type;
    for(type = 0; type < rht->numUpdateTypes; type++) {
        for(srcNode = 0; srcNode < RGLO_NUM_SLAVES; srcNode++) {
            // Process ops from the last sync, new delayed ops go to the next sync
            RoomyHashTable_getUpdateName(
                rht, type, rht->numSyncs-1, srcNode, subtableNum, filename);
            if(!fileExists(filename)) continue;
            bytesDeleted += numBytesInFile(filename);
            ReadBuffer buf = ReadBuffer_make(RGLO_BUFFER_SIZE,
                                             rht->updateSizes[type],
                                             filename, 1);
            char newVal[rht->valueSize];
            while(ReadBuffer_hasMore(&buf)) {
                // Perform op
                // If it's the default update function, copy update value,
                // otherwise call the user defined update function.
                char* key = (char*)ReadBuffer_current(&buf); // use char* for
                                                             // ptr arithmetic
                char* passedValue = key + rht->keySize;
                void* tableValue = HashTable_get(subtable, key);
                if (type == 0) {
                    memcpy(newVal, passedValue, rht->valueSize);
                } else {
                    rht->updateFuncs[type](key,
                                           tableValue,
                                           passedValue,
                                           newVal);
                }

                // update predicates and HashTable if newValue is different
                if (tableValue == NULL || 
                    memcmp(tableValue, newVal, rht->valueSize) != 0) {
                    int p;
                    for(p = 0; p < rht->numPredicates; p++) {
                        if (tableValue != NULL) {
                            rht->predicateVals[p] -=
                                rht->predFuncs[p](key, tableValue);
                        }
                        rht->predicateVals[p] += rht->predFuncs[p](key, newVal);
                    }
                    // TODO: HashTable_get above and this HashTable_insert both
                    // apply the hash function to key. If hash function is a
                    // bottleneck, can try using hash function once and saving
                    // result.
                    HashTable_insert(subtable, key, newVal);
                }

                ReadBuffer_next(&buf);
            }
            ReadBuffer_destroy(&buf);
            unlink(filename);
        }
    }

    // space accounting
    Roomy_dataRemoved(bytesDeleted);
}

// Process delayed remove operations.
void RoomyHashTable_processRemove(RoomyHashTable* rht,
                                  HashTable* subtable,
                                  uint64 subtableNum) {
    #ifdef DEBUG
    Roomy_logAny("Performing removes for %s\n", rht->name);
    #endif

    uint64 bytesDeleted = 0;

    char filename[RGLO_STR_SIZE];
    int srcNode;
    for(srcNode=0; srcNode<RGLO_NUM_SLAVES; srcNode++) {
        // Process ops from the last sync, new delayed ops go to the next sync
        RoomyHashTable_getRemoveName(
            rht, rht->numSyncs-1, srcNode, subtableNum, filename);
        if(!fileExists(filename)) continue;
        bytesDeleted += numBytesInFile(filename);
        ReadBuffer buf = ReadBuffer_make(RGLO_BUFFER_SIZE,
                                         rht->keySize,
                                         filename, 1);
        while(ReadBuffer_hasMore(&buf)) {
            void* key = ReadBuffer_current(&buf);
            void* value = HashTable_get(subtable, key);
            // update predicates and delete element, if it exists in table
            if (value != NULL) {
                int p;
                for(p = 0; p < rht->numPredicates; p++) {
                    rht->predicateVals[p] -= rht->predFuncs[p](key, value);
                }
                HashTable_delete(subtable, key);
            }
            ReadBuffer_next(&buf);
        }
        ReadBuffer_destroy(&buf);
        unlink(filename);
    }

    // space accounting
    Roomy_dataRemoved(bytesDeleted);
}

// Process all delayed operations.
void RoomyHashTable_processDelayedOps(RoomyHashTable* rht) {
    RGLO_IN_PARALLEL = 1;

    // record new size of hash table
    rht->lSize = 0;
    
    uint64 subNum;
    for(subNum = RGLO_MY_RANK; subNum < rht->subTables; subNum += RGLO_NUM_SLAVES) {
        HashTable subtable = RoomyHashTable_loadSubtable(rht, subNum);
        RoomyHashTable_processAccess(rht, &subtable, subNum);
        RoomyHashTable_processUpdate(rht, &subtable, subNum);
        RoomyHashTable_processRemove(rht, &subtable, subNum);
        rht->lSize += HashTable_size(&subtable);
        RoomyHashTable_saveSubtable(&subtable);
    }

    RGLO_IN_PARALLEL = 0;
}

/*************************************************************************
                           API Functions
*************************************************************************/

RoomyHashTable* RoomyHashTable_make(char* name, int keySize, int valueSize,
                                    uint64 capacity) {
    #ifdef DEBUG
    Roomy_logAny("Making %s\n", name);
    #endif

    if (Roomy_nameExists(name)) {
        return NULL;
    }

    RoomyHashTable* rht = malloc(sizeof(RoomyHashTable));
    strcpy(rht->name, name);    
    rht->keySize = keySize;
    rht->valueSize = valueSize;
    rht->size = 0;
    rht->lSize = 0;

    // Create subdir for this data structure
    sprintf(rht->lDirName, "%s/%s", RGLO_DATA_PATH, rht->name);
    Roomy_destroyDir(rht->lDirName);  // remove existing dir, if it exists
    Roomy_makeDir(rht->lDirName);

    // Determine number and initial capacity of subtables
    uint64 initCapacityPerNode = capacity / RGLO_NUM_SLAVES + 1;
    uint64 initBytesPerNode = initCapacityPerNode * valueSize;
    rht->lSubTables = initBytesPerNode / RGLO_MAX_SUBTABLE_SIZE + 1;
    rht->subTables = rht->lSubTables * RGLO_NUM_SLAVES;
    uint64 initSubtableCapacity = initCapacityPerNode / rht->lSubTables + 1;

    // create empty tables
    HashTable subtable =
        HashTable_make(rht->keySize, rht->valueSize, initSubtableCapacity);
    char filename[RGLO_STR_SIZE];
    int i;
    for (i = RGLO_MY_RANK; i < rht->subTables; i += RGLO_NUM_SLAVES) {
        RoomyHashTable_getSubtableName(rht, i, filename);
        HashTable_fileSave(&subtable, filename);
        RGLO_DISK_LEFT -= HashTable_bytesUsed(&subtable);
    }
    HashTable_destroy(&subtable);

    rht->numUpdateTypes = 0;
    memset(rht->updateBuffersOpen, 0, RGLO_MAX_UPDATE_FUNC);
    rht->numAccessTypes = 0;
    memset(rht->accessBuffersOpen, 0, RGLO_MAX_UPDATE_FUNC);
    rht->removeBuffersOpen = 0;
    rht->numPredicates = 0;
    
    rht->numSyncs = 0;
    rht->isSynced = 1;
    rht->destroyed = 0;

    // add to global list
    Roomy_registerRoomyHashTable(rht);

    // register the default update function.
    RoomyHashTable_registerUpdateFunc(
            rht, RoomyHashTable_defaultUpdateFunc, rht->valueSize);

    Roomy_barrier();

    return rht;
}

// Destroy the given RoomyHashTable by removing all files associated with it.
void RoomyHashTable_destroy(RoomyHashTable* rht) {
    #ifdef DEBUG
    Roomy_logAny("Destroying %s\n", rht->name);
    #endif

    if (rht == NULL) {
        Roomy_log("ERROR: RoomyHashTable is NULL or already destroyed\n");
        assert(rht != NULL);
    }

    Roomy_unregisterRoomyHashTable(rht);
    RGLO_DISK_LEFT += Roomy_destroyDir(rht->lDirName);
    //rht->destroyed = 1;
    free(rht);
}

// Return the number of elements in the given RoomyHashTable.
uint64 RoomyHashTable_size(RoomyHashTable* rht) {
    return rht->size;
}

// Given a function f to map, and a RoomyHashTable rht: execute f(key, value)
// for each (key, value) pair in rht.
// In general, the map function should usually perform updates to some other
// Roomy data structures.
void RoomyHashTable_map(RoomyHashTable* rht,
                        void (*mapFunc)(void* key, void* value)) {
    #ifdef DEBUG
    Roomy_logAny("Mapping over RA %s\n", rht->name);
    #endif
    
    RGLO_IN_PARALLEL = 1;
    uint64 subNum;
    for(subNum = RGLO_MY_RANK; subNum < rht->subTables; subNum += RGLO_NUM_SLAVES) {
        HashTable subtable = RoomyHashTable_loadSubtable(rht, subNum);
        HashTable_map(&subtable, mapFunc);
        HashTable_destroy(&subtable);
    }
    RGLO_IN_PARALLEL = 0;     
}

// Collect a "reduction" of the given RoomyHashTable. The result, containing
// bytesInAns bytes, will be returned in the pointer ansInOut. The ansInOut
// pointer will also serve as an input for the initial (i.e. default) value for
// the reduction answer. The first function given, mergeValAndAns, will be used
// to update a partial answer given an element from the array. The secon
// function given, mergeAnsAndAns, will be used to merge two partial answers.
// Because the order of reductions is not specified, the merge functions should
// be associative and commutative.
void RoomyHashTable_reduce(
  RoomyHashTable *rht,
  void* ansInOut,
  uint64 bytesInAns,
  void (*mergeValAndAns)(void* ansInOut, void* key, void* value),
  void (*mergeAnsAndAns)(void* ansInOut, void* ansIn)) {

    #ifdef DEBUG
    Roomy_logAny("Reducing RA %s\n", rht->name);
    #endif
    
    RGLO_IN_PARALLEL = 1;

    // reduce over each subtable
    uint64 subNum;
    for(subNum = RGLO_MY_RANK; subNum < rht->subTables; subNum += RGLO_NUM_SLAVES) {
        HashTable subtable = RoomyHashTable_loadSubtable(rht, subNum);
        HashTable_reduce(&subtable, ansInOut, mergeValAndAns);
        HashTable_destroy(&subtable);
    }

    // global reduce with other nodes
    // (barrier use MPI_COMM_WORLD, so use RGLO_MPI_RANK instead of RGLO_MY_RANK).
    if(RGLO_MPI_RANK) { // slave
        Roomy_slaveSendBarrier(ansInOut, bytesInAns);
        Roomy_slaveWaitForBarrier(ansInOut, bytesInAns);
    } else {           // master
        uint8* buf = malloc(bytesInAns * (RGLO_NUM_SLAVES - 1));
        Roomy_masterRecvBarrier(buf, bytesInAns);
        int i;
        for(i = 0; i < RGLO_NUM_SLAVES-1; i++) {
            mergeAnsAndAns(ansInOut, &(buf[i*bytesInAns]));
        }
        Roomy_masterNotifySlaves(ansInOut, bytesInAns);
        free(buf);
    }

    RGLO_IN_PARALLEL = 0;
}
// User defined access functions must be registered before they can be used in
// a call to RoomyHashTable_update.
void RoomyHashTable_registerAccessFunc(
  RoomyHashTable* rht,
  void (*accessFunc)(void* key, void* tableValue, void* passedValue),
  uint64 passedValSize) {
    rht->accessFuncs[rht->numAccessTypes] = accessFunc;
    rht->accessSizes[rht->numAccessTypes] = passedValSize + rht->keySize;
    rht->numAccessTypes++;

    #ifdef DEBUG
    Roomy_logAny("registered access function %i\n", rht->numAccessTypes-1);
    #endif
}

// Access the (key, value) pair corresponding to the given key in the given
// RoomyHashTable. The given access function will be applied to the key, value,
// and the given passedValue.  If no value is associated with the key,
// tableValue will be NULL.  The access is delayed until RoomyHashTable_sync
// has been called on the RoomyHashTable. It is a run-time error if the given
// accessFunc has not been registered with RoomyHashTable_registerAccessFunc().
void RoomyHashTable_access(
  RoomyHashTable* rht, void* key, void* passedValue,
  void (*accessFunc)(void* key, void* tableValue, void* passedValue)) {
    rht->isSynced = 0;

    // determine function type. error if not found.
    uint64 type = -1;
    int i;
    for(i = 0; i < rht->numAccessTypes; i++) {
        if(rht->accessFuncs[i] == accessFunc) {
            type = i;
            break;
        }
    }
    if(type == -1) {
        Roomy_logAny("ERROR: user access function not registered\n");
        exit(1);
    }
    
    // open buffers if needed
    if(!rht->accessBuffersOpen[type]) {
        RoomyHashTable_openAccessBuffers(rht, type);
    }

    // save delayed access 
    // If we aren't in parallel mode (e.g. inside map()), only the node in
    // charge of this access will save it
    int subtable = RoomyHashTable_hash2subtable(rht, key);
    int node = RoomyHashTable_subtable2node(rht, subtable); 
    if(RGLO_IN_PARALLEL || RGLO_MY_RANK == node) { 
        uint8 packed[rht->accessSizes[type]];
        memcpy(packed, key, rht->keySize);
        memcpy(&(packed[rht->keySize]), passedValue,
               rht->accessSizes[type] - rht->keySize);
        WriteBuffer_write(&(rht->accessWBS[type][subtable]), packed);
        Roomy_bufferedData(rht->accessSizes[type]);
    }
}

// User defined update functions must be registered before they can be used in
// a call to RoomyHashTable_update.
void RoomyHashTable_registerUpdateFunc(
  RoomyHashTable* rht,
  void (*updateFunc)(void* key, void* oldValue, void* passedValue, void* newValOut),
  uint64 passedValSize) {
    rht->updateFuncs[rht->numUpdateTypes] = updateFunc;
    rht->updateSizes[rht->numUpdateTypes] = passedValSize + rht->keySize;
    rht->numUpdateTypes++;

    #ifdef DEBUG
    Roomy_logAny("registered update function %i\n", rht->numUpdateTypes-1);
    #endif
}

// Update the value assocaited with the given key in the given RoomyHashTable.
// The given update function will be applied to the key, the current value
// associate with that key, and the given passedValue. If there is no value
// assocaited with the given key, oldValue will be NULL.  The update is delayed
// until RoomyHashTable_sync has been called on the RoomyHashTable.  It is a
// run-time error if the given updateFunc has not been registered with
// RoomyHashTable_registerUpdateFunc().
void RoomyHashTable_update(
  RoomyHashTable* rht, void* key, void* passedValue,
  void (*updateFunc)
    (void* key, void* oldValue, void* passedValue, void* newValOut)) {
    rht->isSynced = 0;

    // determine function type. error if not found.
    uint64 type = -1;
    int i;
    for(i = 0; i < rht->numUpdateTypes; i++) {
        if(rht->updateFuncs[i] == updateFunc) {
            type = i;
            break;
        }
    }
    if(type == -1) {
        Roomy_logAny("ERROR: user update function not registered\n");
        exit(1);
    }
    
    // open buffers if needed
    if(!rht->updateBuffersOpen[type]) {
        RoomyHashTable_openUpdateBuffers(rht, type);
    }

    // save delayed update
    // If we aren't in parallel mode (e.g. inside map()), only the node in
    // charge of this update will save it
    int subtable = RoomyHashTable_hash2subtable(rht, key);
    int node = RoomyHashTable_subtable2node(rht, subtable); 
    if(RGLO_IN_PARALLEL || RGLO_MY_RANK == node) { 
        uint8 packed[rht->updateSizes[type]];
        memcpy(packed, key, rht->keySize);
        memcpy(&(packed[rht->keySize]), passedValue,
               rht->updateSizes[type] - rht->keySize);
        WriteBuffer_write(&(rht->updateWBS[type][subtable]), packed);
        Roomy_bufferedData(rht->updateSizes[type]);
    }
}

// Insert the given (key, value) pair in the RoomyHashTable. If the key already
// exists, its value is replaced. The operation is delayed until
// RoomyHashTable_sync is called.
void RoomyHashTable_insert(RoomyHashTable* rht, void* key, void* value) {
    RoomyHashTable_update(rht, key, value, RoomyHashTable_defaultUpdateFunc);
}

// Remove the given key from the given RoomyHashTable. The operation is delayed
// until RoomyHashTable_sync is called.
void RoomyHashTable_remove(RoomyHashTable* rht, void* key) {
    rht->isSynced = 0;

    // open buffers if needed
    if(!rht->removeBuffersOpen) {
        RoomyHashTable_openRemoveBuffers(rht);
    }

    // save delayed remove
    // If we aren't in parallel mode (e.g. inside map()), only the node in
    // charge of this update will save it
    int subtable = RoomyHashTable_hash2subtable(rht, key);
    int node = RoomyHashTable_subtable2node(rht, subtable); 
    if(RGLO_IN_PARALLEL || RGLO_MY_RANK == node) { 
        WriteBuffer_write(&(rht->removeWBS[subtable]), key);
        Roomy_bufferedData(rht->keySize);
    }
}

// Attach a predicate function to the given RoomyArray. A running count
// will be kept on the number of elements in the array that satisfy the
// predicate.
void RoomyHashTable_attachPredicate(RoomyHashTable* rht,
                                    uint8 (*predFunc)(void* key, void* value)) {
    rht->predFuncs[rht->numPredicates] = predFunc;
    rht->predicateVals[rht->numPredicates] = 0;
    rht->numPredicates++;

    #ifdef DEBUG
    Roomy_logAny("registered update function %i\n", rht->numUpdateTypes-1);
    #endif
}

// Get the current count of the number of elements in the given RoomyArray
// that satisfy the given predicate function.
uint64 RoomyHashTable_predicateCount(RoomyHashTable* rht,
                                     uint8 (*predFunc)(void* key, void* value)) {
    // find predicate number
    int predNum = -1;
    int i;
    for(i=0; i<rht->numPredicates; i++) {
        if(rht->predFuncs[i] == predFunc) {
            predNum = i;
        }
    }
    if(i == -1) {
        fprintf(stderr, "Unknown predicate given to RoomyHashTable_predicateCount");
        exit(1);
    }

    // return combined value
    return Roomy_sumBarrier(rht->predicateVals[predNum]); 
}

// Complete all delayed update opperations for the given RoomyHashTable.
void RoomyHashTable_sync(RoomyHashTable* rht) {
    // STATS
    RSTAT_NUM_SYNCS++;

    if (RoomyHashTable_isSynced(rht)) {
        return;
    }

    // Close WriteBuffers and flush data to destination
    RoomyHashTable_closeUpdateBuffers(rht);
    RoomyHashTable_closeAccessBuffers(rht);
    RoomyHashTable_closeRemoveBuffers(rht);

    // A barrier to make sure all slaves are ready to merge
    Roomy_barrier();

    // Make sure all remote writes are flushed to disk
    Roomy_waitForRemoteWrites();

    rht->numSyncs++;
    rht->isSynced = 1;

    // process all delayed operations
    RoomyHashTable_processDelayedOps(rht);

    // Another barrier to make sure all slaves are done processing, and update
    // size of RoomyHashTable.
    rht->size = Roomy_sumBarrier(rht->lSize);
}

// Return true if there are no outstanding delayed operations for this RoomyArray,
// false otherwise.
int RoomyHashTable_isSynced(RoomyHashTable* rht) {
    // can't simply return rht->isSynced, because it can differ between nodes.
    return Roomy_sumBarrier(rht->isSynced) == RGLO_NUM_SLAVES;
}

