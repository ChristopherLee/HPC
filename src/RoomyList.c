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
 * RoomyList.c
 *
 * A list with an arbitrary number of elements. Each element is the same size,
 * that size being an arbitrary number of bytes.
 *****************************************************************************/

#include "ReadBuffer.h"
#include "WriteBuffer.h"
#include "types.h"
#include "Array.h"
#include "RoomyList.h"
#include "roomy.h"
#include "params.h"
#include "knudirs.h"
#include "knustring.h"
#include "knusort.h"
#include "hash.h"

#include <sys/stat.h>
#include <string.h>
#include <errno.h>
#include <unistd.h>

//#define DEBUG

/******************************************************************************
 *                   Element Distribution and Ordering
 *****************************************************************************/

// Distribute elements of the list to compute nodes based on a hash of the key.
uint64 RoomyList_value2node(RoomyList* rl, void* value) {
    return hsiehHash((char*)value, rl->keySize) % RGLO_NUM_SLAVES;
}

// Comparison by keys.
int RoomyList_compareByKey(void* p1, void* p2, int keyStart, int keySize) {
    return memcmp((uint8*)p1 + keyStart, (uint8*)p2 + keyStart, keySize);
}

/******************************************************************************
 *                             File Naming
 *****************************************************************************/

// Get the data file name for given list on the given node.
void RoomyList_getListFileName(RoomyList* rl, int node, char* ans) {
    sprintf(ans, "%s/node_%i", rl->lDirName, node);
}

// Get the directory name for storing buffered add operations on this node.
void RoomyList_getAddDirName(RoomyList* rl, char* ans) {
    sprintf(ans, "%s/add_%i/", rl->lDirName, RGLO_MY_RANK);
}

// Get the directory name for storing buffered remove operations on this node.
void RoomyList_getRemoveDirName(RoomyList* rl, char* ans) {
    sprintf(ans, "%s/remove_%i/", rl->lDirName, RGLO_MY_RANK);
}

// Get the directory name for temporary storage on this node (e.g., used when
// performing external sort).
void RoomyList_getTmpDirName(RoomyList* rl, char* ans) {
    sprintf(ans, "%s/tmp_%i/", rl->lDirName, RGLO_MY_RANK);
}

// Get the name of the file for buffered adds from this node to the given node.
void RoomyList_getAddFileName(RoomyList* rl, int syncNum,
                              int destNode, char* ans) {
    sprintf(ans, "%s/add_%i/add_sync%i_from%i",
        rl->lDirName, destNode, syncNum, RGLO_MY_RANK);
}

// Get the name of the file for buffered removes from this node to the given node.
void RoomyList_getRemoveFileName(RoomyList* rl, int syncNum,
                                 int destNode, char* ans) {
    sprintf(ans, "%s/remove_%i/remove_sync%i_from%i",
        rl->lDirName, destNode, syncNum, RGLO_MY_RANK);
}

// Get the name of the file for adding or concatenating elements from another list
// NOTE: not currently used, because adds are immediately appended to rlTo.
void RoomyList_getAddListName(RoomyList* rlTo, RoomyList* rlFrom,
                              int syncNum, char* ans) {
    sprintf(ans, "%s/add_%i/add_sync%i_list_%s",
        rlTo->lDirName, RGLO_MY_RANK, syncNum, rlFrom->name);
}

// Get the name of the file for removing elements from another list
void RoomyList_getRemoveListName(RoomyList* rlTo, RoomyList* rlFrom,
                                 int syncNum, char* ans) {
    sprintf(ans, "%s/remove_%i/remove_sync%i_list_%s",
        rlTo->lDirName, RGLO_MY_RANK, syncNum, rlFrom->name);
}

// Get the name of the file for sorted pending adds
void RoomyList_getSortedAddName(RoomyList* rl, char* ans) {
    sprintf(ans, "%s/add_%i/sorted", rl->lDirName, RGLO_MY_RANK);
}

// Get the name of the file for sorted pending removes
void RoomyList_getSortedRemoveName(RoomyList* rl, char* ans) {
    sprintf(ans, "%s/remove_%i/sorted", rl->lDirName, RGLO_MY_RANK);
}

// Return a list of all of the files for pending adds
int RoomyList_getAllAddFiles(RoomyList* rl, int syncNum, char*** ans) {
    char substr[RGLO_STR_SIZE];
    sprintf(substr, "add_sync%i", syncNum);
    char dir[RGLO_STR_SIZE];
    RoomyList_getAddDirName(rl, dir);
    return getFileNamesWith(dir, substr, ans);
}

// Return a list of all of the files for pending removes
int RoomyList_getAllRemoveFiles(RoomyList* rl, int syncNum, char*** ans) {
    char substr[RGLO_STR_SIZE];
    sprintf(substr, "remove_sync%i", syncNum);
    char dir[RGLO_STR_SIZE];
    RoomyList_getRemoveDirName(rl, dir);
    return getFileNamesWith(dir, substr, ans);
}

/******************************************************************************
 *                            Local Functions
 *****************************************************************************/

// Open write buffers for add and remove operations
void RoomyList_openWriteBuffers(RoomyList* rl) {
    #ifdef DEBUG
    Roomy_logAny("Opening write buffers %s\n", rl->name);
    #endif

    rl->addWBS = malloc(sizeof(WriteBuffer) * RGLO_NUM_SLAVES);
    rl->removeWBS = malloc(sizeof(WriteBuffer) * RGLO_NUM_SLAVES);

    char name[RGLO_STR_SIZE];

    // make local bufffer for this node

    RoomyList_getAddFileName(rl, rl->numSyncs, RGLO_MY_RANK, name);
    rl->addWBS[RGLO_MY_RANK] = 
        WriteBuffer_make(RGLO_BUFFER_SIZE, rl->updateSize, name, 0);
    RoomyList_getRemoveFileName(rl, rl->numSyncs, RGLO_MY_RANK, name);
    rl->removeWBS[RGLO_MY_RANK] = 
        WriteBuffer_make(RGLO_BUFFER_SIZE, rl->updateSize, name, 0);

    // make remote buffers, if there's no global file system. otherwise,
    // buffers for other nodes are local too.
    int i;
    if(RPAR_SHARED_DISK) {
        for(i=0; i<RGLO_NUM_SLAVES; i++) {
            if(i != RGLO_MY_RANK) {
                RoomyList_getAddFileName(rl, rl->numSyncs, i, name);
                rl->addWBS[i] =
                    WriteBuffer_make(RGLO_BUFFER_SIZE, rl->updateSize, name, 0);
                RoomyList_getRemoveFileName(rl, rl->numSyncs, i, name);
                rl->removeWBS[i] =
                    WriteBuffer_make(RGLO_BUFFER_SIZE, rl->updateSize, name, 0);
            }
        }
    } else {
        for(i=0; i<RGLO_NUM_SLAVES; i++) {
            if(i != RGLO_MY_RANK) {
                RoomyList_getAddFileName(rl, rl->numSyncs, i, name);
                rl->addWBS[i] = WriteBuffer_makeRemote(
                                    RGLO_BUFFER_SIZE, rl->updateSize, name, i);
                RoomyList_getRemoveFileName(rl, rl->numSyncs, i, name);
                rl->removeWBS[i] = WriteBuffer_makeRemote(
                                    RGLO_BUFFER_SIZE, rl->updateSize, name, i);
            }
        }
    }
    rl->buffersOpen = 1;
}

// Close write buffers for add and remove operations
void RoomyList_closeWriteBuffers(RoomyList* rl) {
    #ifdef DEBUG
    Roomy_logAny("Closing write buffer %s\n", rl->name);
    #endif

    if(rl->buffersOpen) {
        int i;
        for(i=0; i<RGLO_NUM_SLAVES; i++) {
            WriteBuffer_destroy(&(rl->addWBS[i]));
            WriteBuffer_destroy(&(rl->removeWBS[i]));
        }
        free(rl->addWBS);
        free(rl->removeWBS);
        rl->buffersOpen = 0;
    }
}

// Merge all add, remove, list add, list remove, and concatenation operations
// into the given RoomyList. Return the number of bytes deleted from disk.
uint64 RoomyList_mergeUpdates(RoomyList* rl) {
    uint64 bytesDeleted = 0;    
    char listFile[RGLO_STR_SIZE];
    RoomyList_getListFileName(rl, RGLO_MY_RANK, listFile);

    // merge all add operations: concatenating the list file with all buffer
    // files, while computing predicates
    char** fileNames;
    int numFiles = RoomyList_getAllAddFiles(rl, rl->numSyncs - 1, &fileNames);
    if(numFiles) {
        WriteBuffer wb =
            WriteBuffer_make(RGLO_BUFFER_SIZE, rl->bytesPer, listFile, 1);
        int i;
        for(i=0; i<numFiles; i++) {
            ReadBuffer rb =
                ReadBuffer_make(RGLO_BUFFER_SIZE, rl->bytesPer, fileNames[i], 1);
            while(ReadBuffer_hasMore(&rb)) {
                // calculate predicate value changes
                int p;
                for(p=0; p<rl->numPredicates; p++) {
                    rl->predicateVals[p] +=
                        rl->predFuncs[p](ReadBuffer_current(&rb));
                }
                // write to main file
                WriteBuffer_write(&wb, ReadBuffer_current(&rb));
                rl->lSize++;
                ReadBuffer_next(&rb);
            }
            ReadBuffer_destroy(&rb);
            unlink(fileNames[i]);
        }
        WriteBuffer_destroy(&wb);
        rl->isSorted = 0;
        freeStringArray(fileNames, numFiles);
    }
    
    // merging removes: requires both list and buffers be sorted, then stream
    // through both
    numFiles = RoomyList_getAllRemoveFiles(rl, rl->numSyncs - 1, &fileNames);
    if(numFiles) {
        bytesDeleted += numBytesInAllFiles(fileNames, numFiles);        

        // If list is locally empty, just delete all remove files and return.
        // otherwise, continue processing removes.
        if(rl->lSize == 0) {
            int i;
            for(i=0; i<numFiles; i++)
                unlink(fileNames[i]);
            return bytesDeleted;
        }

        char tmpdir[RGLO_STR_SIZE];
        RoomyList_getTmpDirName(rl, tmpdir);

        // sort list file
        if(!rl->isSorted) {
            char* files[1];
            files[0] = listFile;
            extMergeSortByKey(files, 1, 1, 0, listFile, tmpdir,
                rl->bytesPer, rl->keyStart, rl->keySize, RGLO_MAX_CHUNK_SIZE);
            rl->isSorted = 1;
        }

        // sort removes
        char sortedName[RGLO_STR_SIZE];
        RoomyList_getSortedRemoveName(rl, sortedName);
        extMergeSortByKey(fileNames, numFiles, 1, 1, sortedName, tmpdir,
            rl->updateSize, rl->keyStart, rl->keySize, RGLO_MAX_CHUNK_SIZE);

        uint64 sizeBefore = numBytesInFile(listFile);

        // stream through both and perform removes
        ReadBuffer listRB =
                   ReadBuffer_make(RGLO_BUFFER_SIZE, rl->bytesPer, listFile, 1);
        ReadBuffer remRB =
                   ReadBuffer_make(RGLO_BUFFER_SIZE, rl->updateSize, sortedName, 1);
        char tmpFile[RGLO_STR_SIZE];
        sprintf(tmpFile, "%s%s", listFile, ".tmp");
        WriteBuffer newListWB =
                    WriteBuffer_make(RGLO_BUFFER_SIZE, rl->bytesPer, tmpFile, 1);
        void* listCur;
        void* remCur;
        int comp;
        while(ReadBuffer_hasMore(&listRB) && ReadBuffer_hasMore(&remRB)) {
            listCur = ReadBuffer_current(&listRB);
            remCur = ReadBuffer_current(&remRB);
            comp =
                RoomyList_compareByKey(listCur, remCur, rl->keyStart, rl->keySize);
            if(comp < 0) {
                WriteBuffer_write(&newListWB, listCur);
                ReadBuffer_next(&listRB);
            } else if(comp == 0) {
                // element is being removed, calculate predicate and size changes
                int p;
                for(p=0; p<rl->numPredicates; p++) {
                    rl->predicateVals[p] -= rl->predFuncs[p](listCur);
                }
                rl->lSize--;
                ReadBuffer_next(&listRB);
            } else {
                ReadBuffer_next(&remRB);
            }
        }
        while(ReadBuffer_hasMore(&listRB)) {
            listCur = ReadBuffer_current(&listRB);
            WriteBuffer_write(&newListWB, listCur);
            ReadBuffer_next(&listRB);
        }
        ReadBuffer_destroy(&listRB);
        ReadBuffer_destroy(&remRB);
        WriteBuffer_destroy(&newListWB);
        unlink(listFile);
        rename(tmpFile, listFile);

        bytesDeleted += sizeBefore - numBytesInFile(listFile);
        freeStringArray(fileNames, numFiles);
    }

    // space accounting
    Roomy_dataRemoved(bytesDeleted);

    return bytesDeleted;
}

/*************************************************************************
                           API Functions
*************************************************************************/

// RoomyList constructor, where equality of elements is based on a user defined
// key. The user supplies a function that returns a pointer to the key given
// an element of the list.
RoomyList* RoomyList_makeWithKey(char* name, uint64 bytesPerElt, uint64 keyStart,
                                 uint64 keySize) {
    #ifdef DEBUG
    Roomy_logAny("Making %s\n", name);
    #endif

    if (Roomy_nameExists(name)) {
        return NULL;
    }
    
    RoomyList* rl = malloc(sizeof(RoomyList));

    // Global init
    strcpy(rl->name, name);
    rl->bytesPer = bytesPerElt;
    rl->destroyed = 0;

    // Local init
    rl->lSize = 0;
    rl->isSorted = 1;
    rl->updateSize = bytesPerElt;
    rl->buffersOpen = 0;
    rl->numPredicates = 0;

    rl->keyStart = keyStart;
    rl->keySize = keySize;

    // Create subdirs for this data structure
    sprintf(rl->lDirName, "%s/%s", RGLO_DATA_PATH, rl->name);
    Roomy_destroyDir(rl->lDirName);  // remove existing dir, if it exists
    Roomy_makeDir(rl->lDirName);
    // separate subdirectories for each node's adds and removes and tmp
    char subdir[RGLO_STR_SIZE];
    RoomyList_getAddDirName(rl, subdir);
    mkdir(subdir, S_IRWXU);
    RoomyList_getRemoveDirName(rl, subdir);
    mkdir(subdir, S_IRWXU);
    RoomyList_getTmpDirName(rl, subdir);
    mkdir(subdir, S_IRWXU);

    // Create empty list
    char listName[RGLO_STR_SIZE];
    RoomyList_getListFileName(rl, RGLO_MY_RANK, listName);
    FILE* f = fopen(listName, "w");
    if (f == NULL) {
        fprintf(stderr, "Could not open file: %s\n", listName);
        fprintf(stderr, "%s\n", strerror(errno));
    }
    fclose(f);

    rl->destroyed = 0;
    rl->numSyncs = 0;
    rl->isSynced = 1;

    // add to global list
    Roomy_registerRoomyList(rl);

    Roomy_barrier();

    return rl;
}

// Default RoomyList constructor.
RoomyList* RoomyList_make(char* name, uint64 bytesPerElt) {
    return RoomyList_makeWithKey(name, bytesPerElt, 0, bytesPerElt);
}

// Destroy the given RoomyList by removing all files associated with it
void RoomyList_destroy(RoomyList* rl) {
    #ifdef DEBUG
    Roomy_logAny("Destroying %s\n", rl->name);
    #endif

    if (rl == NULL) {
        Roomy_log("ERROR: RoomyList is NULL or already destroyed\n");
        assert(rl != NULL);
    }

    Roomy_unregisterRoomyList(rl);
    Roomy_dataRemoved(Roomy_destroyDir(rl->lDirName));
    //rl->destroyed = 1;
    free(rl);
}

// Add the given element to the given RoomyList. This is a delayed operation.
void RoomyList_add(RoomyList* rl, void* elt) {
    if(!rl->buffersOpen)
        RoomyList_openWriteBuffers(rl);
    int dest = RoomyList_value2node(rl, elt);
    // If we aren't in parallel mode (e.g. inside map()), only the node in
    // charge of this update will save it
    if(RGLO_IN_PARALLEL || RGLO_MY_RANK == dest) { 
        WriteBuffer_write(&(rl->addWBS[dest]), elt);
        Roomy_bufferedData(rl->updateSize);
    }
    rl->isSynced = 0;
}

// Remove all occurances of the given element from the given RoomyList. This
// is a delayed operation.
void RoomyList_remove(RoomyList* rl, void* elt) {
    if(!rl->buffersOpen)
        RoomyList_openWriteBuffers(rl);
    int dest = RoomyList_value2node(rl, elt);
    // If we aren't in parallel mode (e.g. inside map()), only the node in
    // charge of this update will save it
    if(RGLO_IN_PARALLEL || RGLO_MY_RANK == dest) { 
        WriteBuffer_write(&(rl->removeWBS[dest]), elt);
        Roomy_bufferedData(rl->updateSize);
    }
    rl->isSynced = 0;
}

// Given a function f to map, and a RoomyList rl: for
// each index i, execute the function f(i, rl[i]).
// In general, the map function will usually perform updates to some other
// Roomy data structures.
void RoomyList_map(RoomyList* rl, void (*mapFunc)(void* val)) {
    #ifdef DEBUG
    Roomy_logAny("Mapping over RL %s\n", rl->name);
    #endif
    
    // if list is locally empty, return without mapping
    if(rl->lSize == 0) return;

    RGLO_IN_PARALLEL = 1;

    char listFile[RGLO_STR_SIZE];
    RoomyList_getListFileName(rl, RGLO_MY_RANK, listFile);
    ReadBuffer buf = ReadBuffer_make(RGLO_BUFFER_SIZE, rl->bytesPer, listFile, 1);
    while(ReadBuffer_hasMore(&buf)) {
        // perform map
        mapFunc(ReadBuffer_current(&buf));
        ReadBuffer_next(&buf);
    }

    // clean up
    ReadBuffer_destroy(&buf);
    
    RGLO_IN_PARALLEL = 0;  
}

// Same as the map function above, but also updates the values of the
// input array.
void RoomyList_mapAndModify(RoomyList *rl,
                     void (*mapFunc)(void* oldVal, void* newValOut)) {
    // if list is locally empty, return without mapping
    if(rl->lSize == 0) return;

    char listFile[RGLO_STR_SIZE];
    RoomyList_getListFileName(rl, RGLO_MY_RANK, listFile);
    uint64 bytesLeft = numBytesInFile(listFile);
    uint64 chunkSize = RGLO_BUFFER_SIZE;
    chunkSize = chunkSize - (chunkSize % rl->bytesPer);
    uint64 arraySize = chunkSize / rl->bytesPer;
    Array chunk = Array_make(rl->bytesPer, arraySize);
    FILE* f = fopen(listFile, "a+");
    uint64 filePos = 0;
    void* newVal = malloc(rl->bytesPer);
    while(bytesLeft) {
        // load chunk
        uint64 curChunkSize = chunkSize;
        if(curChunkSize > bytesLeft) curChunkSize = bytesLeft;
        uint64 curArraySize = curChunkSize / rl->bytesPer;
        fseek(f, filePos, SEEK_SET);
        Array_fileDescLoad(&chunk, f, curArraySize);

        // apply map function, with modifications
        int i;
        for(i=0; i<curArraySize; i++) {
            mapFunc(Array_get(&chunk, i), newVal);
            
            // keep track of predicate changes
            int p;
            for(p=0; p<rl->numPredicates; p++) {
                rl->predicateVals[p] += rl->predFuncs[p](newVal) -
                                        rl->predFuncs[p](Array_get(&chunk, i));
            }

            Array_set(&chunk, i, newVal);
        }

        // save chunk
        fseek(f, filePos, SEEK_SET);
        Array_fileDescSave(&chunk, f, curArraySize);
        
        filePos += curChunkSize;
        bytesLeft -= curChunkSize;
    }
    
    // clean up
    Array_destroy(&chunk);
    free(newVal);
    fclose(f);
}

// Collect a "reduction" of the given RoomyList. The result, containing
// bytesInAns bytes, will be returned in the pointer ans. The ans pointer
// will also serve as an input for the initial (i.e. default) value for
// the reduction answer. The two functions given will update a partial answer
// given either an element from the array, or another partial answer.
// Because the order of reductions is not specified, the merge functions should
// be associative and commutative.
void RoomyList_reduce(RoomyList *rl,
                   void* ansInOut,
                   uint64 bytesInAns,
                   void (*mergeValAndAns)(void* ansInOut, void* val),
                   void (*mergeAnsAndAns)(void* ansInOut, void* ansIn)) {
    #ifdef DEBUG
    Roomy_logAny("Reducing RL %s\n", rl->name);
    #endif
    
    RGLO_IN_PARALLEL = 1;

    // Step 1: perform reduction locally

    // skip this part of list is locally empty
    if(rl->lSize > 0) {
        // open read buffer
        char listFile[RGLO_STR_SIZE];
        RoomyList_getListFileName(rl, RGLO_MY_RANK, listFile);
        ReadBuffer buf =
                    ReadBuffer_make(RGLO_BUFFER_SIZE, rl->bytesPer, listFile, 1);
        
        // reduce each element assigned to this node
        while(ReadBuffer_hasMore(&buf)) {
            // perform reduction
            mergeValAndAns(ansInOut, ReadBuffer_current(&buf));
            ReadBuffer_next(&buf);
        }
        
        // clean up
        ReadBuffer_destroy(&buf);
    }
    
    // Step 2: send partial answers to master, who then distributes the final
    // answer to all slaves
    // (barrier use MPI_COMM_WORLD, so use RGLO_MPI_RANK instead of RGLO_MY_RANK).
    if(RGLO_MPI_RANK) { // slave
        Roomy_slaveSendBarrier(ansInOut, bytesInAns);
        Roomy_slaveWaitForBarrier(ansInOut, bytesInAns);
    } else {           // master
        uint8* buf = malloc(bytesInAns * (RGLO_NUM_SLAVES - 1));
        Roomy_masterRecvBarrier(buf, bytesInAns);
        int i;
        for(i=0; i<RGLO_NUM_SLAVES-1; i++) {
            mergeAnsAndAns(ansInOut, &(buf[i*bytesInAns]));
        }
        Roomy_masterNotifySlaves(ansInOut, bytesInAns);
    }

    RGLO_IN_PARALLEL = 0;
}

// Attach a predicate function to the given RoomyList. A running count
// will be kept on the number of elements in the array that satisfy the
// predicate.
void RoomyList_attachPredicate(RoomyList* rl, uint8 (*predFunc)(void* val)) {
    rl->predFuncs[rl->numPredicates] = predFunc;
    rl->predicateVals[rl->numPredicates] = 0;
    rl->numPredicates++;
}

// Get the current count of the number of elements in the given RoomyList
// that satisfy the given predicate function.
uint64 RoomyList_predicateCount(RoomyList* rl, uint8 (*predFunc)(void* val)) {
    // find predicate number
    int predNum = -1;
    int i;
    for(i=0; i<rl->numPredicates; i++) {
        if(rl->predFuncs[i] == predFunc) predNum = i;
    }
    if(i == -1) {
        fprintf(stderr, "Unknown predicate given to RoomyList_predicateCount");
        exit(1);
    }

    // return combined value
    return Roomy_sumBarrier(rl->predicateVals[predNum]);   
}

// Return the total size of the given RoomyList
uint64 RoomyList_size(RoomyList* rl) {
    return Roomy_sumBarrier(rl->lSize);
}

// Remove all duplicate elements from the given RoomyList
void RoomyList_removeDupes(RoomyList* rl) {
    // if list is locally empty, return early
    if(rl->lSize == 0) return;

    // sort and remove dupes
    char listFile[RGLO_STR_SIZE];
    RoomyList_getListFileName(rl, RGLO_MY_RANK, listFile); 
    char* files[1];
    files[0] = listFile;
    char tmpdir[RGLO_STR_SIZE];
    RoomyList_getTmpDirName(rl, tmpdir);
    extMergeSortByKey(files, 1, 1, 1, listFile, tmpdir,
                 rl->bytesPer, rl->keyStart, rl->keySize, RGLO_RAM_LEFT);
    rl->isSorted = 1;

    // scan to recompute predicate values (optimization: this could be combined
    // with the sorting proceedure)
    if(rl->numPredicates > 0) {
        int p;
        for(p=0; p<rl->numPredicates; p++) rl->predicateVals[p] = 0;
        ReadBuffer buf =
                   ReadBuffer_make(RGLO_BUFFER_SIZE, rl->bytesPer, listFile, 1);
        while(ReadBuffer_hasMore(&buf)) {
            for(p=0; p<rl->numPredicates; p++)
                rl->predicateVals[p] += rl->predFuncs[p](ReadBuffer_current(&buf));
            ReadBuffer_next(&buf);
        }
        ReadBuffer_destroy(&buf);
    }

    // update list size
    rl->lSize = numBytesInFile(listFile) / rl->bytesPer;
}

// Add all of the elements from RoomyList rl2 to RoomyList rl1
void RoomyList_addAll(RoomyList* rl1, RoomyList* rl2) {
    assert(rl1->bytesPer == rl2->bytesPer);

    // halt early if rl2 is locally empty
    if(rl2->lSize == 0) return;

    // copy rl2 elements to end of rl1
    char rl1File[RGLO_STR_SIZE];
    RoomyList_getListFileName(rl1, RGLO_MY_RANK, rl1File);
    char rl2File[RGLO_STR_SIZE];
    RoomyList_getListFileName(rl2, RGLO_MY_RANK, rl2File);
    ReadBuffer in = ReadBuffer_make(RGLO_BUFFER_SIZE, rl2->bytesPer, rl2File, 1);
    WriteBuffer out = WriteBuffer_make(RGLO_BUFFER_SIZE, rl1->bytesPer, rl1File, 1);
    while(ReadBuffer_hasMore(&in)) {
        WriteBuffer_write(&out, ReadBuffer_current(&in));
        int p;
        for(p=0; p<rl1->numPredicates; p++) 
            rl1->predicateVals[p] += rl1->predFuncs[p](ReadBuffer_current(&in));
        ReadBuffer_next(&in);
    }
    ReadBuffer_destroy(&in);
    WriteBuffer_destroy(&out);

    rl1->lSize += rl2->lSize;

    Roomy_bufferedData(rl2->lSize);
}

// Remove all of the elements in RoomyList rl2 from RoomyList rl1
void RoomyList_removeAll(RoomyList* rl1, RoomyList* rl2) {
    assert(rl1->bytesPer == rl2->bytesPer);
    assert(rl1->keyStart == rl2->keyStart);
    assert(rl1->keySize == rl2->keySize);

    // halt early if rl2 is locally empty
    if(rl2->lSize == 0) return;

    // copy rl2 elements to rl1 remove folder
    char rl1File[RGLO_STR_SIZE];
    RoomyList_getRemoveListName(rl1, rl2, rl1->numSyncs, rl1File);
    char rl2File[RGLO_STR_SIZE];
    RoomyList_getListFileName(rl2, RGLO_MY_RANK, rl2File);
    ReadBuffer in = ReadBuffer_make(RGLO_BUFFER_SIZE, rl2->bytesPer, rl2File, 1);
    WriteBuffer out = WriteBuffer_make(RGLO_BUFFER_SIZE, rl1->bytesPer, rl1File, 1);
    while(ReadBuffer_hasMore(&in)) {
        WriteBuffer_write(&out, ReadBuffer_current(&in));
        ReadBuffer_next(&in);
    }
    ReadBuffer_destroy(&in);
    WriteBuffer_destroy(&out);

    rl1->isSynced = 0;
}

// Complete all delayed operations for the given RoomyList.
void RoomyList_sync(RoomyList* rl) {
    // STATS
    RSTAT_NUM_SYNCS++;

    if (RoomyList_isSynced(rl)) {
        return;
    }

    // Close WriteBuffers and flush data to destination
    RoomyList_closeWriteBuffers(rl);

    // A barrier to make sure all slaves are ready to merge
    Roomy_barrier();

    // Make sure all remote writes are flushed to disk
    Roomy_waitForRemoteWrites();

    rl->numSyncs++;
    rl->isSynced = 1;

    // merge updates and add the freed space back to the global value
    RoomyList_mergeUpdates(rl);

    // Another barrier to make sure all slaves are done merging
    Roomy_barrier();
}

// Return true if there are no outstanding delayed operations false otherwise.
int RoomyList_isSynced(RoomyList* rl) {
    // can't simply return rl->isSynced, because it can differ between nodes.
    return Roomy_sumBarrier(rl->isSynced) == RGLO_NUM_SLAVES;    
}
