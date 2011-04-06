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
 * RoomyArray.c
 *
 * A disk-based array. The array contains a fixed number of elements, each
 * element being the same size. Each element can contain between 1 and 7 bits,
 * or any number of bytes.
 *****************************************************************************/

#include "Array.h"
#include "BitsArray.h"
#include "BitArray.h"
#include "ReadBuffer.h"
#include "knudirs.h"
#include "knustring.h"
#include "knusort.h"
#include "params.h"
#include "roomy.h"
#include "RoomyArray.h"
#include "RoomyList.h"
#include "types.h"
#include "Timer.h"
#include "WriteBuffer.h"

#include <sys/stat.h>
#include <string.h>
#include <unistd.h>

//#define DEBUG

/******************************************************************************
 *                        Index Transformations
 * Elements are distributed among nodes by modulo. Elements are then stored
 * contiguously in a number of files, known as chunks.
 *****************************************************************************/

// Determine which node owns the given global address
inline uint64 RoomyArray_globalAddr2node(const RoomyArray* ra, uint64 gAddr) {
    return gAddr % RGLO_NUM_SLAVES;
}

// Determine which local chunk the given global address belongs to
inline uint64 RoomyArray_globalAddr2chunk(const RoomyArray* ra, uint64 gAddr) {
    return gAddr / ra->lChunkEltsAllNodes;
}

// Convert the given global address to the index into the current chunk
inline uint64 RoomyArray_globalAddr2chunkAddr(const RoomyArray* ra, uint64 gAddr) {
    return (gAddr / RGLO_NUM_SLAVES) % ra->lChunkElts;
}

// Determine which update bucket corresponds to the given node and chunk
inline uint64 RoomyArray_nodeAndChunk2bucket(const RoomyArray* ra,
                                             uint64 node,
                                             uint64 chunk) {
    return node * ra->lChunks + chunk;
}

// Determine which update bucket the given global address belongs to.
inline uint64 RoomyArray_globalAddr2bucket(const RoomyArray* ra, uint64 gAddr) {
    return RoomyArray_nodeAndChunk2bucket(ra,
                                          RoomyArray_globalAddr2node(ra, gAddr),
                                          RoomyArray_globalAddr2chunk(ra, gAddr));
}

/******************************************************************************
 *                             File Naming
 *****************************************************************************/

// Get the data file name for given node and chunk. Return answer in last arg.
void RoomyArray_getChunkName(RoomyArray* ra, int node, int chunk, char* ans) {
    sprintf(ans, "%s/node_%i.chunk_%i", ra->lDirName, node, chunk);
}

// Get all of the file names for update operations. Names are passed back in
// last argument, number of files is returned.
int RoomyArray_getAllUpdateFiles(RoomyArray* ra, int updateNum,
                                 uint64 syncNum, char*** ans) {
    char substr[RGLO_STR_SIZE];
    sprintf(substr, "updates%i_sync%lli_", updateNum, syncNum);
    return getFileNamesWith(ra->lDirName, substr, ans);
}

// Name for update buffer when using buckets
void RoomyArray_updateBucketName(RoomyArray* ra, int updateNum, uint64 syncNum,
                                 int sourceNode, int bucketNum, char* ans) {
    sprintf(ans, "%s/updates%i_sync%lli_from%i_bucket%i",
            ra->lDirName, updateNum, syncNum, sourceNode, bucketNum);
}

// Get all of the file names for access operations. Names are passed back in
// last argument, number of files is returned.
int RoomyArray_getAllAccessFiles(RoomyArray* ra, int accessNum,
                                 uint64 syncNum, char*** ans) {
    char substr[RGLO_STR_SIZE];
    sprintf(substr, "access%i_sync%lli_", accessNum, syncNum);
    return getFileNamesWith(ra->lDirName, substr, ans);
}

// Name for access buffer when using buckets
void RoomyArray_accessBucketName(RoomyArray* ra, int accessNum, uint64 syncNum,
                                 int sourceNode, int bucketNum, char* ans) {
    sprintf(ans, "%s/access%i_sync%lli_from%i_bucket%i",
            ra->lDirName, accessNum, syncNum, sourceNode, bucketNum);
}

/******************************************************************************
 *                            Packing Updates
 *****************************************************************************/

// Pack an RA_Update into a contiguous memory buffer
void RoomyArray_packUpdate(RA_Update* up, uint64 updateSize, void* buffer) {
    uint8* buf = (uint8*)buffer;
    memcpy(buf, &(up->index), sizeof(uint64));
    if (updateSize-sizeof(uint64) > 0) {
        memcpy(&(buf[sizeof(uint64)]), up->val, updateSize-sizeof(uint64));
    }
}

// Unpack the buffer into an update struct
void RoomyArray_unpackUpdate(RA_Update* up, uint64 updateSize, void* buffer) {
    uint8* buf = (uint8*)buffer;
    memcpy(&(up->index), buf, sizeof(uint64));
    if (updateSize-sizeof(uint64) > 0) {
        memcpy(up->val, &(buf[sizeof(uint64)]), updateSize-sizeof(uint64));
    }
}

/******************************************************************************
 *                            Local Functions
 *****************************************************************************/

// Open one WriteBuffer per bucket to store delayed updates of the given kind
void RoomyArray_openUpdateBuffers(RoomyArray* ra, int updateNum) {
    #ifdef DEBUG
    Roomy_logAny("Opening update buffers %s\n", ra->name);
    #endif

    uint64 numBuckets = ra->lChunks * RGLO_NUM_SLAVES;
    ra->updateWBS[updateNum] = malloc(sizeof(WriteBuffer) * numBuckets);
    char name[RGLO_STR_SIZE];

    // make one buffer for each bucket
    int node, chunk;
    for(node=0; node<RGLO_NUM_SLAVES; node++) {
        for(chunk=0; chunk<ra->lChunks; chunk++) {
            uint64 bucketNum = RoomyArray_nodeAndChunk2bucket(ra, node, chunk);
            RoomyArray_updateBucketName(
                ra, updateNum, ra->numSyncs, RGLO_MY_RANK, bucketNum, name);
            ra->updateWBS[updateNum][bucketNum] = Roomy_makeWriteBuffer(
                RGLO_BUFFER_SIZE, ra->updateSizes[updateNum], name, node);
        }
    }
    ra->updateBuffersOpen[updateNum] = 1;
}

// Close and destroy all WriteBuffers for all types of updates
void RoomyArray_closeUpdateBuffers(RoomyArray* ra) {
    #ifdef DEBUG
    Roomy_logAny("Closing update buffer %s\n", ra->name);
    #endif

    int updateNum, node, chunk;
    for(updateNum=0; updateNum<ra->numUpdateTypes; updateNum++) {
        if(ra->updateBuffersOpen[updateNum]) {
            for(node=0; node<RGLO_NUM_SLAVES; node++) {
                for(chunk=0; chunk<ra->lChunks; chunk++) {
                    uint64 bucketNum =
                            RoomyArray_nodeAndChunk2bucket(ra, node, chunk);
                    WriteBuffer_destroy(&(ra->updateWBS[updateNum][bucketNum]));
                }
            }
            free(ra->updateWBS[updateNum]);
            ra->updateBuffersOpen[updateNum] = 0;
        }
    }
}

// Open one WriteBuffer per bucket to store delayed access of the given kind
void RoomyArray_openAccessBuffers(RoomyArray* ra, int accessNum) {
    #ifdef DEBUG
    Roomy_logAny("Opening access buffers %s\n", ra->name);
    #endif

    uint64 numBuckets = ra->lChunks * RGLO_NUM_SLAVES;
    ra->accessWBS[accessNum] = malloc(sizeof(WriteBuffer) * numBuckets);
    char name[RGLO_STR_SIZE];

    // make one buffer for each bucket
    int node, chunk;
    for(node=0; node<RGLO_NUM_SLAVES; node++) {
        for(chunk=0; chunk<ra->lChunks; chunk++) {
            uint64 bucketNum = RoomyArray_nodeAndChunk2bucket(ra, node, chunk);
            RoomyArray_accessBucketName(
                ra, accessNum, ra->numSyncs, RGLO_MY_RANK, bucketNum, name);
            ra->accessWBS[accessNum][bucketNum] = Roomy_makeWriteBuffer(
                        RGLO_BUFFER_SIZE, ra->accessSizes[accessNum], name, node);
        }
    }
    ra->accessBuffersOpen[accessNum] = 1;
}

// Close and destroy all WriteBuffers for all types of accesses
void RoomyArray_closeAccessBuffers(RoomyArray* ra) {
    #ifdef DEBUG
    Roomy_logAny("Closing access buffer %s\n", ra->name);
    #endif

    int accessNum, node, chunk;
    for(accessNum=0; accessNum<ra->numAccessTypes; accessNum++) {
        if(ra->accessBuffersOpen[accessNum]) {
            for(node=0; node<RGLO_NUM_SLAVES; node++) {
                for(chunk=0; chunk<ra->lChunks; chunk++) {
                    uint64 bucketNum =
                            RoomyArray_nodeAndChunk2bucket(ra, node, chunk);
                    WriteBuffer_destroy(&(ra->accessWBS[accessNum][bucketNum]));
                }
            }
            free(ra->accessWBS[accessNum]);
            ra->accessBuffersOpen[accessNum] = 0;
        }
    }
}

/*******************************************************************************/

// Load an Array chunk
Array RoomyArray_loadArrayChunk(RoomyArray* ra, uint64 chunkNum) {
    uint64 chunkSize = ra->lChunkSize;
    if(chunkNum == ra->lChunks-1) chunkSize = ra->lLastChunkSize;
    char chunkName[RGLO_STR_SIZE];
    RoomyArray_getChunkName(ra, RGLO_MY_RANK, chunkNum, chunkName);
    Array chunk;
    if (!RGLO_USE_MMAP) {
        chunk = Array_make(ra->bytesPer, chunkSize / ra->bytesPer);
        Array_fileLoad(&chunk, chunkName);
    } else {
        chunk = Array_makeMapped(ra->bytesPer, chunkSize / ra->bytesPer,
                                 chunkName, 0);
    }
    return chunk;
}

// Save an Array chunk
void RoomyArray_saveArrayChunk(RoomyArray* ra, Array* chunk, uint64 chunkNum) {
    if (!RGLO_USE_MMAP) {
        char chunkName[RGLO_STR_SIZE];
        RoomyArray_getChunkName(ra, RGLO_MY_RANK, chunkNum, chunkName);
        Array_fileSave(chunk, chunkName);
    }
    Array_destroy(chunk);
}

// Load a BitsArray chunk
BitsArray RoomyArray_loadBitsArrayChunk(RoomyArray* ra, uint64 chunkNum) {
    uint64 chunkSize = ra->lChunkSize;
    if(chunkNum == ra->lChunks-1) chunkSize = ra->lLastChunkSize;
    char chunkName[RGLO_STR_SIZE];
    RoomyArray_getChunkName(ra, RGLO_MY_RANK, chunkNum, chunkName);
    BitsArray chunk;
    if (!RGLO_USE_MMAP) {
        chunk = BitsArray_make(ra->bitsPer, (chunkSize * 8)/ra->bitsPer);
        BitsArray_fileLoad(&chunk, chunkName);
    } else {
        chunk = BitsArray_makeMapped(ra->bitsPer, (chunkSize * 8)/ra->bitsPer,
                                     chunkName, 0);
    }
    return chunk;
}

// Save a BitsArray chunk
void RoomyArray_saveBitsArrayChunk(
  RoomyArray* ra, BitsArray* chunk, uint64 chunkNum) {
    if (!RGLO_USE_MMAP) {
        char chunkName[RGLO_STR_SIZE];
        RoomyArray_getChunkName(ra, RGLO_MY_RANK, chunkNum, chunkName);
        BitsArray_fileSave(chunk, chunkName);
    }
    BitsArray_destroy(chunk);
}

// Load a BitArray chunk
BitArray RoomyArray_loadBitArrayChunk(RoomyArray* ra, uint64 chunkNum) {
    uint64 chunkSize = ra->lChunkSize;
    if(chunkNum == ra->lChunks-1) chunkSize = ra->lLastChunkSize;
    char chunkName[RGLO_STR_SIZE];
    RoomyArray_getChunkName(ra, RGLO_MY_RANK, chunkNum, chunkName);
    BitArray chunk;
    if (!RGLO_USE_MMAP) {
        chunk = BitArray_make(chunkSize * 8);
        BitArray_fileLoad(&chunk, chunkName);
    } else {
        chunk = BitArray_makeMapped(chunkSize * 8, chunkName, 0);
    }
    return chunk;
}

// Save a BitArray chunk
void RoomyArray_saveBitArrayChunk(
  RoomyArray* ra, BitArray* chunk, uint64 chunkNum) {
    if (!RGLO_USE_MMAP) {
        char chunkName[RGLO_STR_SIZE];
        RoomyArray_getChunkName(ra, RGLO_MY_RANK, chunkNum, chunkName);
        BitArray_fileSave(chunk, chunkName);
    }
    BitArray_destroy(chunk);
}

// Processes all delayed operations (accesses and updates).
void RoomyArray_processDelayedOps(RoomyArray* ra) {
    #ifdef SORT_ARRAY_OPS
    // This is only done for testing. We sort all delayed operations to
    // measure how much faster the bucket method is than the sorting method.
    Roomy_log("WARNING: DELAYED ARRAY OPS ARE BEING SORTED!\n");
    #endif

    RGLO_IN_PARALLEL = 1;

    #ifdef SORT_ARRAY_OPS
    int updateType, accessType;
    for(updateType = 0; updateType < ra->numUpdateTypes; updateType++) {
        char** filenames;
        int numFiles = RoomyArray_getAllUpdateFiles(ra, updateType,
                                     ra->numSyncs-1, &filenames);
        if(numFiles == 0) continue; // skip if none exist
        char sortedName[RGLO_STR_SIZE];
        sprintf(sortedName, "%s/updates%i_sorted", ra->lDirName, updateType);
        extMergeSortByKey(filenames, numFiles, 0, 0, sortedName, ra->lDirName,
                          ra->updateSizes[updateType], 0,
                          ra->updateSizes[updateType], RGLO_MAX_CHUNK_SIZE);
        freeStringArray(filenames, numFiles);
        unlink(sortedName);
    }
    for(accessType = 0; accessType < ra->numAccessTypes; accessType++) {
        char** filenames;
        int numFiles = RoomyArray_getAllAccessFiles(ra, accessType,
                                     ra->numSyncs-1, &filenames);
        if(numFiles == 0) continue; // skip if none exist
        char sortedName[RGLO_STR_SIZE];
        sprintf(sortedName, "%s/accesses%i_sorted", ra->lDirName, accessType);
        extMergeSortByKey(filenames, numFiles, 0, 0, sortedName, ra->lDirName,
                          ra->accessSizes[accessType], 0,
                          ra->accessSizes[accessType], RGLO_MAX_CHUNK_SIZE);
        freeStringArray(filenames, numFiles);
        unlink(sortedName);
    }
    #endif

    int chunkNum;
    for(chunkNum=0; chunkNum<ra->lChunks; chunkNum++) {
        if (ra->eltType == SIZE_BYTES) {
            Array chunk = RoomyArray_loadArrayChunk(ra, chunkNum);
            RoomyArray_performAccess(ra, &chunk, chunkNum);
            RoomyArray_mergeUpdates(ra, &chunk, chunkNum);
            RoomyArray_saveArrayChunk(ra, &chunk, chunkNum);
        } else if (ra->eltType == SIZE_BITS) {
            BitsArray chunk = RoomyArray_loadBitsArrayChunk(ra, chunkNum);
            RoomyArray_performAccess(ra, &chunk, chunkNum);
            RoomyArray_mergeUpdates(ra, &chunk, chunkNum);
            RoomyArray_saveBitsArrayChunk(ra, &chunk, chunkNum);
        } else {
            BitArray chunk = RoomyArray_loadBitArrayChunk(ra, chunkNum);
            RoomyArray_performAccess(ra, &chunk, chunkNum);
            RoomyArray_mergeUpdates(ra, &chunk, chunkNum);
            RoomyArray_saveBitArrayChunk(ra, &chunk, chunkNum);
        }
    }

    RGLO_IN_PARALLEL = 0;
}

// Perform all delayed accesses for the given chunk. The given chunk will be
// cast to an Array, BitArray, or BitsArray depending on the size of the
// elements in this RoomyArray.
void RoomyArray_performAccess(RoomyArray* ra, void* chunk, int chunkNum) {
    #ifdef DEBUG
    Roomy_logAny("Performing accesses for %s\n", ra->name);
    #endif

    uint64 bytesDeleted = 0;
    
    char bucketName[RGLO_STR_SIZE];
    int srcNode, accessType;
    uint64 bucketNum = RoomyArray_nodeAndChunk2bucket(ra, RGLO_MY_RANK, chunkNum);
    for(accessType=0; accessType<ra->numAccessTypes; accessType++) {
        for(srcNode=0; srcNode<RGLO_NUM_SLAVES; srcNode++) {
            // Process ops from the last sync, new delayed ops go to the next sync
            RoomyArray_accessBucketName(
                ra, accessType, ra->numSyncs-1, srcNode, bucketNum, bucketName);
            if(!fileExists(bucketName)) continue;
            bytesDeleted += numBytesInFile(bucketName);
            ReadBuffer buf = ReadBuffer_make(RGLO_BUFFER_SIZE,
                                             ra->accessSizes[accessType],
                                             bucketName, 1);
            RA_Update cur;
            cur.val = malloc(ra->accessSizes[accessType]-sizeof(uint64));
            while(ReadBuffer_hasMore(&buf)) {
                RoomyArray_unpackUpdate(&cur, ra->accessSizes[accessType],
                                        ReadBuffer_current(&buf));

                #ifdef DEBUG
                assert(RoomyArray_globalAddr2chunk(ra, cur.index) ==
                       chunkNum);
                assert(RoomyArray_globalAddr2node(ra, cur.index) ==
                       RGLO_MY_RANK);
                #endif

                // perform op
                uint64 i = RoomyArray_globalAddr2chunkAddr(ra, cur.index);
                if (ra->eltType == SIZE_BYTES) {
                    void* arrayVal = Array_get((Array*)chunk, i);
                    ra->accessFuncs[accessType](cur.index, arrayVal, cur.val);
                } else if (ra->eltType == SIZE_BITS) {
                    uint8 arrayVal = BitsArray_get((BitsArray*)chunk, i);
                    ra->accessFuncs[accessType](cur.index, &arrayVal, cur.val);
                } else {
                    uint8 arrayVal = BitArray_test((BitArray*)chunk, i);
                    ra->accessFuncs[accessType](cur.index, &arrayVal, cur.val);
                }
                

                ReadBuffer_next(&buf);
            }
            free(cur.val);
            ReadBuffer_destroy(&buf);
            unlink(bucketName);
        }
    }

    // space accounting
    Roomy_dataRemoved(bytesDeleted);
}

// Merge all delayed updates for the given chunk Return the number of bytes
// deleted from disk. Returns 1 if the array is modified by an update, 0
// otherwise.
// The given chunk will be cast to an Array, BitArray, or BitsArray depending on
// the size of the elements in this RoomyArray.
int RoomyArray_mergeUpdates(RoomyArray* ra, void* chunk, int chunkNum) {
    #ifdef DEBUG
    Roomy_logAny("Merging updates for %s\n", ra->name);
    #endif

    uint64 bytesDeleted = 0;
    int modified = 0;
    
    char bucketName[RGLO_STR_SIZE];
    int srcNode, updateType;
    uint64 bucketNum = RoomyArray_nodeAndChunk2bucket(ra, RGLO_MY_RANK, chunkNum);
    for(updateType=0; updateType<ra->numUpdateTypes; updateType++) {
        for(srcNode=0; srcNode<RGLO_NUM_SLAVES; srcNode++) {
            // Process ops from the last sync, new delayed ops go to the next sync
            RoomyArray_updateBucketName(
                ra, updateType, ra->numSyncs-1, srcNode, bucketNum, bucketName);
            if(!fileExists(bucketName)) continue;
            bytesDeleted += numBytesInFile(bucketName);
            ReadBuffer buf = ReadBuffer_make(RGLO_BUFFER_SIZE,
                                             ra->updateSizes[updateType],
                                             bucketName, 1);
            RA_Update cur;
            cur.val = malloc(ra->updateSizes[updateType]-sizeof(uint64));
            while(ReadBuffer_hasMore(&buf)) {
                RoomyArray_unpackUpdate(&cur, ra->updateSizes[updateType],
                                        ReadBuffer_current(&buf));

                #ifdef DEBUG
                assert(RoomyArray_globalAddr2chunk(ra, cur.index) ==
                       chunkNum);
                assert(RoomyArray_globalAddr2node(ra, cur.index) ==
                       RGLO_MY_RANK);
                #endif

                // Perform op
                // If it's the default update function, copy update value,
                // otherwise call the user defined update function.
                // If new value is different, update predicates and set value.

                // Operations for elements of SIZE_BYTES
                uint64 i = RoomyArray_globalAddr2chunkAddr(ra, cur.index);
                if (ra->eltType == SIZE_BYTES) {
                    void* oldVal = Array_get((Array*)chunk, i);
                    uint8 newVal[ra->bytesPer];
                    if(updateType == 0) {
                        memcpy(newVal, cur.val, ra->bytesPer);
                    } else {
                        ra->updateFuncs[updateType](
                                    cur.index, oldVal, cur.val, newVal);
                    }

                    if(memcmp(oldVal, newVal, ra->bytesPer) != 0) {
                        modified = 1;
                        int p;
                        for(p=0; p<ra->numPredicates; p++) {
                            int diff = ra->predFuncs[p](cur.index, newVal) -
                                       ra->predFuncs[p](cur.index, oldVal);
                            ra->predicateVals[p] += diff;
                        }
                        Array_set((Array*)chunk, i, newVal);
                    }

                // Operations for elements of SIZE_BITS
                } else if (ra->eltType == SIZE_BITS) {
                    uint8 oldVal = BitsArray_get((BitsArray*)chunk, i);
                    uint8 newVal;
                    if(updateType == 0) {
                        memcpy(&newVal, cur.val, sizeof(uint8));
                    } else {
                        ra->updateFuncs[updateType](
                                    cur.index, &oldVal, cur.val, &newVal);
                    }

                    if(oldVal != newVal) {
                        modified = 1;
                        int p;
                        for(p=0; p<ra->numPredicates; p++) {
                            int diff = ra->predFuncs[p](cur.index, &newVal) -
                                       ra->predFuncs[p](cur.index, &oldVal);
                            ra->predicateVals[p] += diff;
                        }
                        BitsArray_set((BitsArray*)chunk, i, newVal);
                    }

                // Operations for elements of SIZE_BIT
                } else {
                    assert(ra->eltType == SIZE_BIT);
                    uint8 oldVal = BitArray_test((BitArray*)chunk, i);
                    uint8 newVal;
                    if(updateType == 0) {
                        memcpy(&newVal, cur.val, sizeof(uint8));
                    } else {
                        ra->updateFuncs[updateType](
                                    cur.index, &oldVal, cur.val, &newVal);
                    }

                    if(oldVal != newVal) {
                        modified = 1;
                        int p;
                        for(p=0; p<ra->numPredicates; p++) {
                            int diff = ra->predFuncs[p](cur.index, &newVal) -
                                       ra->predFuncs[p](cur.index, &oldVal);
                            ra->predicateVals[p] += diff;
                        }
                        if (newVal == 0) {
                            BitArray_clear((BitArray*)chunk, i);
                        } else if (newVal == 1) {
                            BitArray_set((BitArray*)chunk, i);
                        } else {
                            fprintf(stderr, "ERROR: value passed to ");
                            fprintf(stderr, "RoomyArray_update with 1-bit ");
                            fprintf(stderr, "elements must be 0 or 1.\n");
                            fprintf(stderr, "Value given: %i\n", newVal);
                            exit(1);
                        }
                    }
                }

                ReadBuffer_next(&buf);
            }
            free(cur.val);
            ReadBuffer_destroy(&buf);
            unlink(bucketName);
        }
    }

    // space accounting
    Roomy_dataRemoved(bytesDeleted);

    return modified;
}

// A default update function for the RoomyArray that is registered when the
// RoomyArray is constructed. This function is not actually called, it is
// just a place holder so other internal Roomy code can specify that it wants
// to update a given index of the RoomyArray with a specific value.
// The reason it can't be directly called is that it doesn't know the size
// of the elements of the RoomyArray, and so doesn't know how many bytes to
// copy from updateVal to newValOut.
void RoomyArray_defaultUpdateFunc(uint64 i, void* oldVal, void* updateVal,
                                  void* newValOut) {
    Roomy_log("ERROR: RoomyArray_defaultUpdateFunc called\n");
    exit(1);
}

/*************************************************************************
                           API Functions
*************************************************************************/

// Create a RoomyArray with elements containing one or more bytes.
RoomyArray* RoomyArray_makeBytes(char* name, uint64 bytesPerElt, uint64 numElts) {
    return RoomyArray_makeType(SIZE_BYTES, name, bytesPerElt, numElts);
}

// Create a RoomyArray with elements containing between 1 and 7 bits.
RoomyArray* RoomyArray_makeBits(char* name, uint64 bitsPerElt, uint64 numElts) {
    assert(bitsPerElt <= 7 && bitsPerElt >= 1);
    if (bitsPerElt == 1) {
        return RoomyArray_makeType(SIZE_BIT, name, bitsPerElt, numElts);
    } else {
        return RoomyArray_makeType(SIZE_BITS, name, bitsPerElt, numElts);
    }
}

// Create a RoomyArray with elements of the given type: SIZE_BYTES, SIZE_BITS,
// or SIZE_BIT.
RoomyArray* RoomyArray_makeType(int eltType, char* name, uint64 bytesOrBytesPerElt,
                                uint64 numElts) {
    #ifdef DEBUG
    Roomy_logAny("Making %s\n", name);
    #endif

    assert(eltType == SIZE_BYTES || eltType == SIZE_BITS || eltType == SIZE_BIT);

    if (Roomy_nameExists(name)) {
        return NULL;
    }
    
    RoomyArray* ra = malloc(sizeof(RoomyArray));

    strcpy(ra->name, name);
    ra->eltType = eltType;

    // calculate number of elements / bytes, globally and locally
    ra->elts = numElts;
    ra->lElts = ra->elts / RGLO_NUM_SLAVES;
    if(ra->elts % RGLO_NUM_SLAVES) {
        ra->lElts++;
    }
    if (ra->eltType == SIZE_BYTES) {
        ra->bytesPer = bytesOrBytesPerElt;
        ra->bytes = ra->bytesPer * ra->elts;
        ra->lBytes = ra->bytesPer * ra->lElts;
    } else {
        ra->bitsPer = bytesOrBytesPerElt;
        uint64 nBits = ra->elts * ra->bitsPer;
        ra->bytes = nBits / 8;
        if(nBits % 8) {
            ra->bytes++;
        }
        uint64 nLBits = ra->lElts * ra->bitsPer;
        ra->lBytes = nLBits / 8;
        if(nLBits % 8) {
            ra->lBytes++;
        }
        ra->bytesPer = 1;
    }
    
    // calculate number and size of chunks
    if(ra->lBytes <= RGLO_MAX_CHUNK_SIZE) {
        // only one chunk
        ra->lChunks = 1;
        ra->lChunkSize = ra->lBytes;
        ra->lLastChunkSize = ra->lBytes;
        if (ra->eltType == SIZE_BYTES) {
            ra->lChunkElts = ra->lChunkSize / ra->bytesPer;
        } else {
            ra->lChunkElts = (ra->lChunkSize * 8) / ra->bitsPer;
        }
    } else {
        // determine normal chunk size
        ra->lChunkSize = RGLO_MAX_CHUNK_SIZE;
        if (ra->eltType == SIZE_BYTES) {
            // make chunk size a multiple of element size
            ra->lChunkSize -= ra->lChunkSize % ra->bytesPer;
        }

        // determine number of elements per chunk
        if (ra->eltType == SIZE_BYTES) {
            ra->lChunkElts = ra->lChunkSize / ra->bytesPer;
        } else {
            ra->lChunkElts = (ra->lChunkSize * 8) / ra->bitsPer;
        }

        // determine number of chunks
        ra->lChunks = ra->lElts / ra->lChunkElts;
        uint64 eltsInLastChunk = ra->lElts % ra->lChunkElts;        
        if (eltsInLastChunk) {
            ra->lChunks++;
            if (ra->eltType == SIZE_BYTES) {
                ra->lLastChunkSize = eltsInLastChunk * ra->bytesPer;
            } else {
                ra->lLastChunkSize = (eltsInLastChunk * ra->bitsPer) / 8;
                if ((eltsInLastChunk * ra->bitsPer) % 8) {
                    ra->lLastChunkSize++;
                }
            }
        } else {
            ra->lLastChunkSize = ra->lChunkSize;
        }
    }

    ra->lChunkEltsAllNodes = ra->lChunkElts * RGLO_NUM_SLAVES;

    // Book keeping for user functions
    ra->numUpdateTypes = 0;
    memset(ra->updateBuffersOpen, 0, RGLO_MAX_UPDATE_FUNC);
    ra->numAccessTypes = 0;
    memset(ra->accessBuffersOpen, 0, RGLO_MAX_UPDATE_FUNC);
    ra->numPredicates = 0;

    ra->numSyncs = 0;
    ra->isSynced = 1;
    ra->destroyed = 0;

    // Create subdir for this data structure
    sprintf(ra->lDirName, "%s/%s", RGLO_DATA_PATH, ra->name);
    Roomy_destroyDir(ra->lDirName);  // remove existing dir, if it exists
    Roomy_makeDir(ra->lDirName);

    // Write zeroed chunks
    uint8* buffer = malloc(ra->lChunkSize);
    memset(buffer, 0, ra->lChunkSize);
    int i;
    for(i=0; i<ra->lChunks; i++) {
        uint64 thisSize = i < ra->lChunks-1 ?
                                ra->lChunkSize : ra->lLastChunkSize;
        char name[1024];
        RoomyArray_getChunkName(ra, RGLO_MY_RANK, i, name);
        FILE* f = fopen(name, "w");
        fwrite(buffer, 1, thisSize, f);
        fclose(f);
    }
    free(buffer);

    // Accounting
    RGLO_DISK_LEFT -= ra->lBytes;

    // add to global list
    Roomy_registerRoomyArray(ra);

    // register the default update function.
    if (ra->eltType == SIZE_BYTES) {
        RoomyArray_registerUpdateFunc(
            ra, RoomyArray_defaultUpdateFunc, ra->bytesPer);
    } else {
        RoomyArray_registerUpdateFunc(ra, RoomyArray_defaultUpdateFunc, 1);
    }

    Roomy_barrier();

    return ra;
}

// Create a RoomyArray from the given RoomyList. The new RoomyArray will be the
// same size as the RoomyList and will contain the elements of the RoomyList
// in an arbitrary order.
RoomyArray* RoomyArray_makeFromList(char* name, RoomyList* rl) {
    uint64 listSize = RoomyList_size(rl);

    // make RoomyArray
    RoomyArray* ra = RoomyArray_makeBytes(name, rl->bytesPer, listSize);

    if (ra == NULL) {
        return ra;
    }

    // switch to parallel mode so all updates go through (instead of just local
    // updates)
    RGLO_IN_PARALLEL = 1;

    // find out how many list elements are on each node. this is used to determine
    // the range of index values for elements on this node.
    uint64 eltsPerNode[RGLO_NUM_SLAVES];
    Roomy_collectBarrier(rl->lSize, eltsPerNode);    
    uint64 myFirstIndex = 0;
    int i;
    for(i=0; i<RGLO_MY_RANK; i++)
        myFirstIndex += eltsPerNode[i];

    // send RoomyArray_update operations for all list elements
    char listFile[RGLO_STR_SIZE];
    RoomyList_getListFileName(rl, RGLO_MY_RANK, listFile);
    ReadBuffer listRB =
                   ReadBuffer_make(RGLO_BUFFER_SIZE, rl->bytesPer, listFile, 1);
    uint64 curIndex = myFirstIndex;
    while(ReadBuffer_hasMore(&listRB)) {
        RoomyArray_update(ra, curIndex, ReadBuffer_current(&listRB),
                          RoomyArray_defaultUpdateFunc);
        ReadBuffer_next(&listRB);
        curIndex++;
    }
    ReadBuffer_destroy(&listRB);

    RGLO_IN_PARALLEL = 0;

    // commit updates
    RoomyArray_sync(ra);

    return ra;
}

// Destroy the given RoomyArray by removing all files associated with it
void RoomyArray_destroy(RoomyArray* ra) {
    #ifdef DEBUG
    Roomy_logAny("Destroying %s\n", ra->name);
    #endif

    if (ra == NULL) {
        Roomy_log("ERROR: RoomyArray is NULL or already destroyed\n");
        assert(ra != NULL);
    }

    Roomy_unregisterRoomyArray(ra);
    RGLO_DISK_LEFT += Roomy_destroyDir(ra->lDirName);
    //ra->destroyed = 1;
    free(ra);
}

// Return the size of the given RoomyArray.
uint64 RoomyArray_size(RoomyArray* ra) {
    return ra->elts;
}

// Given a function f to map, and a RoomyArrays ra: for
// each index i, execute the function f(i, ra[i]).
// In general, the map function should usually perform updates to some other
// Roomy data structures.
void RoomyArray_map(RoomyArray* ra, void (*mapFunc)(uint64 i, void* val)) {
    #ifdef DEBUG
    Roomy_logAny("Mapping over RA %s\n", ra->name);
    #endif
    
    RGLO_IN_PARALLEL = 1;

    // load first chunk
    Array arrayChunk;
    BitsArray bitsArrayChunk;
    BitArray bitArrayChunk;
    if (ra->eltType == SIZE_BYTES) {
        arrayChunk = RoomyArray_loadArrayChunk(ra, 0);
    } else if (ra->eltType == SIZE_BITS) {
        bitsArrayChunk = RoomyArray_loadBitsArrayChunk(ra, 0);
    } else {
        bitArrayChunk = RoomyArray_loadBitArrayChunk(ra, 0);
    }

    // map over each index assigned to this node
    uint64 i, chunkNum = 0, chunkAddr = 0;
    uint8 input;
    for(i = RGLO_MY_RANK; i < ra->elts; i += RGLO_NUM_SLAVES) {
        // load new chunk when needed
        if(chunkAddr == ra->lChunkElts) {
            chunkNum++;
            chunkAddr = 0;
            if (ra->eltType == SIZE_BYTES) {
                Array_destroy(&arrayChunk);
                arrayChunk = RoomyArray_loadArrayChunk(ra, chunkNum);
            } else if (ra->eltType == SIZE_BITS) {
                BitsArray_destroy(&bitsArrayChunk);
                bitsArrayChunk = RoomyArray_loadBitsArrayChunk(ra, chunkNum);
            } else {
                BitArray_destroy(&bitArrayChunk);
                bitArrayChunk = RoomyArray_loadBitArrayChunk(ra, chunkNum);
            }
        }

        // perform map
        if (ra->eltType == SIZE_BYTES) {
            mapFunc(i, Array_get(&arrayChunk, chunkAddr));
        } else if (ra->eltType == SIZE_BITS) {
            input = BitsArray_get(&bitsArrayChunk, chunkAddr);
            mapFunc(i, &input);
        } else {
            input = BitArray_test(&bitArrayChunk, chunkAddr);
            mapFunc(i, &input);
        }

        chunkAddr++;
    }

    // clean up
    if (ra->eltType == SIZE_BYTES) {
        Array_destroy(&arrayChunk);
    } else if (ra->eltType == SIZE_BITS) {
        BitsArray_destroy(&bitsArrayChunk);
    } else {
        BitArray_destroy(&bitArrayChunk);
    }
    
    RGLO_IN_PARALLEL = 0; 
}

// Same as the map function above, but also updates the values of the
// input array.
void RoomyArray_mapAndModify(RoomyArray *ra,
                 void (*mapFunc)(uint64 i, void* oldVal, void* newValOut)) {
    #ifdef DEBUG
    Roomy_logAny("Mapping and modifying over RBsA %s\n", ra->name);
    #endif
    
    RGLO_IN_PARALLEL = 1;
    
    // load first chunk
    Array arrayChunk;
    BitsArray bitsArrayChunk;
    BitArray bitArrayChunk;
    if (ra->eltType == SIZE_BYTES) {
        arrayChunk = RoomyArray_loadArrayChunk(ra, 0);
    } else if (ra->eltType == SIZE_BITS) {
        bitsArrayChunk = RoomyArray_loadBitsArrayChunk(ra, 0);
    } else {
        bitArrayChunk = RoomyArray_loadBitArrayChunk(ra, 0);
    }

    // map over each index assigned to this node
    void* input;
    uint8 input_uint8;
    uint8 output[ra->bytesPer];
    uint64 i, chunkNum = 0, chunkAddr = 0;
    for(i=RGLO_MY_RANK; i<ra->elts; i+=RGLO_NUM_SLAVES) {
        // load/save new chunk when needed
        if(chunkAddr == ra->lChunkElts) {
            chunkNum++;
            chunkAddr = 0;
            if (ra->eltType == SIZE_BYTES) {
                Array_destroy(&arrayChunk);
                arrayChunk = RoomyArray_loadArrayChunk(ra, chunkNum);
            } else if (ra->eltType == SIZE_BITS) {
                BitsArray_destroy(&bitsArrayChunk);
                bitsArrayChunk = RoomyArray_loadBitsArrayChunk(ra, chunkNum);
            } else {
                BitArray_destroy(&bitArrayChunk);
                bitArrayChunk = RoomyArray_loadBitArrayChunk(ra, chunkNum);
            }
        }

        // perform map / modify

        if (ra->eltType == SIZE_BYTES) {
            input = Array_get(&arrayChunk, chunkAddr);
        } else if (ra->eltType == SIZE_BITS) {
            input_uint8 = BitsArray_get(&bitsArrayChunk, chunkAddr);
            input = &input_uint8;
        } else {
            input_uint8 = BitArray_test(&bitArrayChunk, chunkAddr);
            input = &input_uint8;
        }
        mapFunc(i, input, output);

        // if new value is different than old, update all predicates and store
        // new value
        if(memcmp(input, output, ra->bytesPer) != 0) {
            int p;
            for(p=0; p<ra->numPredicates; p++) {
                int diff = ra->predFuncs[p](i, output) -
                           ra->predFuncs[p](i, input);
                ra->predicateVals[p] += diff;
            }
            if (ra->eltType == SIZE_BYTES) {
                Array_set(&arrayChunk, chunkAddr, output);
            } else if (ra->eltType == SIZE_BITS) {
                BitsArray_set(&bitsArrayChunk, chunkAddr, *(uint8*)output);
            } else {
                if (*(uint8*)output) {
                    BitArray_set(&bitArrayChunk, chunkAddr);
                } else {
                    BitArray_clear(&bitArrayChunk, chunkAddr);
                }
            }
        }

        chunkAddr++;
    }

    // clean up
    if (ra->eltType == SIZE_BYTES) {
        RoomyArray_saveArrayChunk(ra, &arrayChunk, chunkNum);
    } else if (ra->eltType == SIZE_BITS) {
        RoomyArray_saveBitsArrayChunk(ra, &bitsArrayChunk, chunkNum);
    } else {
        RoomyArray_saveBitArrayChunk(ra, &bitArrayChunk, chunkNum);
    }
    
    RGLO_IN_PARALLEL = 0;
}

// Attach a predicate function to the given RoomyArray. A running count
// will be kept on the number of elements in the array that satisfy the
// predicate.
void RoomyArray_attachPredicate(RoomyArray* ra,
                                uint8 (*predFunc)(uint64 i, void* val)) {
    ra->predFuncs[ra->numPredicates] = predFunc;
    ra->predicateVals[ra->numPredicates] = 0;
    ra->numPredicates++;
}

// Get the current count of the number of elements in the given RoomyArray
// that satisfy the given predicate function.
uint64 RoomyArray_predicateCount(RoomyArray* ra,
                                 uint8 (*predFunc)(uint64 i, void* val)) {
    // find predicate number
    int predNum = -1;
    int i;
    for(i=0; i<ra->numPredicates; i++) {
        if(ra->predFuncs[i] == predFunc) predNum = i;
    }
    if(i == -1) {
        fprintf(stderr, "Unknown predicate given to RoomyArray_predicateCount");
        exit(1);
    }

    // return combined value
    return Roomy_sumBarrier(ra->predicateVals[predNum]);    
}

// Collect a "reduction" of the given RoomyArray. The result, containing
// bytesInAns bytes, will be returned in the pointer ansInOut. The ansInOut
// pointer will also serve as an input for the initial (i.e. default) value for
// the reduction answer. The first function given, mergeValAndAns, will be used
// to update a partial answer given an element from the array. The secon
// function given, mergeAnsAndAns, will be used to merge two partial answers.
// Because the order of reductions is not specified, the merge functions should
// be associative and commutative.
void RoomyArray_reduce(RoomyArray* ra,
                   void* ansInOut,
                   uint64 bytesInAns,
                   void (*mergeValAndAns)(void* ansInOut, uint64 i, void* val),
                   void (*mergeAnsAndAns)(void* ansInOut, void* ansIn)) {
    #ifdef DEBUG
    Roomy_logAny("Reducing RA %s\n", ra->name);
    #endif
    
    RGLO_IN_PARALLEL = 1;

    // Step 1: perform reduction locally
    
    // load first chunk
    Array arrayChunk;
    BitsArray bitsArrayChunk;
    BitArray bitArrayChunk;
    if (ra->eltType == SIZE_BYTES) {
        arrayChunk = RoomyArray_loadArrayChunk(ra, 0);
    } else if (ra->eltType == SIZE_BITS) {
        bitsArrayChunk = RoomyArray_loadBitsArrayChunk(ra, 0);
    } else {
        bitArrayChunk = RoomyArray_loadBitArrayChunk(ra, 0);
    }

    // reduce each index assigned to this node
    void* input;
    uint8 input_uint8;
    uint64 i, chunkNum = 0, chunkAddr = 0;
    for(i=RGLO_MY_RANK; i<ra->elts; i+=RGLO_NUM_SLAVES) {
        // load new chunk when needed
        if(chunkAddr == ra->lChunkElts) {
            chunkNum++;
            chunkAddr = 0;
            if (ra->eltType == SIZE_BYTES) {
                Array_destroy(&arrayChunk);
                arrayChunk = RoomyArray_loadArrayChunk(ra, chunkNum);
            } else if (ra->eltType == SIZE_BITS) {
                BitsArray_destroy(&bitsArrayChunk);
                bitsArrayChunk = RoomyArray_loadBitsArrayChunk(ra, chunkNum);
            } else {
                BitArray_destroy(&bitArrayChunk);
                bitArrayChunk = RoomyArray_loadBitArrayChunk(ra, chunkNum);
            }
        }

        // perform reduction
        if (ra->eltType == SIZE_BYTES) {
            input = Array_get(&arrayChunk, chunkAddr);
        } else if (ra->eltType == SIZE_BITS) {
            input_uint8 = BitsArray_get(&bitsArrayChunk, chunkAddr);
            input = &input_uint8;
        } else {
            input_uint8 = BitArray_test(&bitArrayChunk, chunkAddr);
            input = &input_uint8;
        }
        mergeValAndAns(ansInOut, i, input);

        chunkAddr++;
    }

    // clean up
    if (ra->eltType == SIZE_BYTES) {
        Array_destroy(&arrayChunk);
    } else if (ra->eltType == SIZE_BITS) {
        BitsArray_destroy(&bitsArrayChunk);
    } else {
        BitArray_destroy(&bitArrayChunk);
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
        for(i=0; i<RGLO_NUM_SLAVES-1; i++) {
            mergeAnsAndAns(ansInOut, &(buf[i*bytesInAns]));
        }
        Roomy_masterNotifySlaves(ansInOut, bytesInAns);
    }

    RGLO_IN_PARALLEL = 0;
}

// User defined update functions must be registered before they can be used in
// a call to RoomyArray_update.
void RoomyArray_registerUpdateFunc(RoomyArray* ra,
    void (*updateFunc)(uint64 i, void* oldVal, void* updateVal, void* newValOut),
    uint64 updateValSize) {
                 
    ra->updateFuncs[ra->numUpdateTypes] = updateFunc;
    ra->updateSizes[ra->numUpdateTypes] = updateValSize + sizeof(uint64);
    ra->numUpdateTypes++;

    #ifdef DEBUG
    Roomy_logAny("registered update function %i\n", ra->numUpdateTypes-1);
    #endif
}

// Update the given index of the given RoomyArray by processing the
// updateVal using the updateFunc. The update is delayed until a sync, or
// until buffer space is exhausted.
void RoomyArray_update(RoomyArray *ra, uint64 i, void* updateVal,
    void (*updateFunc)(uint64 i, void* oldVal, void* updateVal, void* newValOut)) {
    
    assert(i<ra->elts);
    
    ra->isSynced = 0;

    // determine update type. error if not found.
    uint64 updateType = -1;
    int up;
    for(up=0; up<ra->numUpdateTypes; up++) {
        if(ra->updateFuncs[up] == updateFunc) {
            updateType = up;
            break;
        }
    }
    if(updateType == -1) {
        Roomy_logAny("ERROR: user update function not registered\n");
        exit(1);
    }
    
    // open buffers if needed
    if(!ra->updateBuffersOpen[updateType]) {
        RoomyArray_openUpdateBuffers(ra, updateType);
    }

    // save delayed update
    // If we aren't in parallel mode (e.g. inside map()), only the node in
    // charge of this update will save it
    int dest = RoomyArray_globalAddr2node(ra, i); 
    if(RGLO_IN_PARALLEL || RGLO_MY_RANK == dest) { 
    
        #ifdef DEBUG
        Roomy_logAny("delayed updaate to %lli, send to %i, size %i\n",
                     i, dest, ra->updateSizes[updateType]);
        #endif
    
        RA_Update update;
        update.index = i;
        update.val = updateVal;
        uint8 packed[ra->updateSizes[updateType]];
        RoomyArray_packUpdate(&update, ra->updateSizes[updateType], packed);

        uint64 bucket = RoomyArray_globalAddr2bucket(ra, i);
        WriteBuffer_write(&(ra->updateWBS[updateType][bucket]), packed);
        
        Roomy_bufferedData(ra->updateSizes[updateType]);
    }
}

// User defined access functions must be registered before they can be used in
// a call to RoomyArray_access.
void RoomyArray_registerAccessFunc(RoomyArray* ra,
    void (*accessFunc)(uint64 i, void* arrayVal, void* passedVal),
    uint64 passedValSize) {
                 
    ra->accessFuncs[ra->numAccessTypes] = accessFunc;
    ra->accessSizes[ra->numAccessTypes] = passedValSize + sizeof(uint64);
    ra->numAccessTypes++;

    #ifdef DEBUG
    Roomy_logAny("registered access function %i\n", ra->numAccessTypes-1);
    #endif
}

// Access the given index of the given RoomyArray by processing the
// passedVal and the value at ra[i] using the accessFunc. The update is delayed
// until a sync, or until buffer space is exhausted. It is a run-time error if the
// given accessFunc has not been registered with RoomyArray_registerAccessFunc().
void RoomyArray_access(RoomyArray *ra, uint64 i, void* passedVal,
    void (*accessFunc)(uint64 i, void* arrayVal, void* passedVal)) {
    
    assert(i < ra->elts);
    
    ra->isSynced = 0;

    // determine update type. error if not found.
    uint64 accessType = -1;
    int type;
    for(type = 0; type < ra->numAccessTypes; type++) {
        if(ra->accessFuncs[type] == accessFunc) {
            accessType = type;
            break;
        }
    }
    if(accessType == -1) {
        Roomy_logAny("ERROR: user access function not registered\n");
        exit(1);
    }
    
    // open buffers if needed
    if(!ra->accessBuffersOpen[accessType]) {
        RoomyArray_openAccessBuffers(ra, accessType);
    }

    // save delayed access 
    // If we aren't in parallel mode (e.g. inside map()), only the node in
    // charge of this access will save it
    int dest = RoomyArray_globalAddr2node(ra, i); 
    if(RGLO_IN_PARALLEL || RGLO_MY_RANK == dest) { 
    
        #ifdef DEBUG
        Roomy_logAny("delayed access to %lli, send to %i, size %i\n",
                     i, dest, ra->accessSizes[accessType]);
        #endif
    
        RA_Update access;
        access.index = i;
        access.val = passedVal;
        uint8 packed[ra->accessSizes[accessType]];
        RoomyArray_packUpdate(&access, ra->accessSizes[accessType], packed);

        uint64 bucket = RoomyArray_globalAddr2bucket(ra, i);
        WriteBuffer_write(&(ra->accessWBS[accessType][bucket]), packed);
        
        Roomy_bufferedData(ra->accessSizes[accessType]);
    }
}

// Complete all delayed update opperations for the given RoomyArray
void RoomyArray_sync(RoomyArray* ra) {
    // STATS
    RSTAT_NUM_SYNCS++;

    if (RoomyArray_isSynced(ra)) {
        return;
    }

    // Close WriteBuffers and flush data to destination
    RoomyArray_closeUpdateBuffers(ra);
    RoomyArray_closeAccessBuffers(ra);

    // A barrier to make sure all slaves are ready to merge
    Roomy_barrier();

    // Make sure all remote writes are flushed to disk
    Roomy_waitForRemoteWrites();

    ra->numSyncs++;
    ra->isSynced = 1;

    // process all delayed operations
    RoomyArray_processDelayedOps(ra);

    // Another barrier to make sure all slaves are done merging
    Roomy_barrier();
}

// Return true if there are no outstanding delayed operations for this RoomyArray,
// false otherwise.
int RoomyArray_isSynced(RoomyArray* ra) {
    // can't simply return ra->isSynced, because it can differ between nodes.
    return Roomy_sumBarrier(ra->isSynced) == RGLO_NUM_SLAVES;    
}

/*********************** MAKE RANDOM PERMUTATION **************************/

// Make a RoomyArray containing a random permutation of the numbers
// 0 to numElts-1. Elements are of type uint64.
RoomyArray* RoomyArray_makeRandomPerm(char* name, uint64 numElts) {
    RoomyArray* ra = RoomyArray_makeBytes(name, sizeof(uint64), numElts);
    
    //TESTING
    //Roomy_logAny("MADE %s\n", name);

    // Delete existing files.
    int i;
    for(i=0; i<ra->lChunks; i++) {
        char name[1024];
        RoomyArray_getChunkName(ra, RGLO_MY_RANK, i, name);
        unlink(name);
    }

    Roomy_barrier();

    //TESTING
    //Roomy_logAny("DELTED %s\n", name);

    // One node distributes elements to random chunks.
    if (RGLO_MY_RANK == 0) {
        // Keep track of which chunks are full; open write buffers.
        uint64 roomLeft[RGLO_NUM_SLAVES * ra->lChunks];
        WriteBuffer wbs[RGLO_NUM_SLAVES * ra->lChunks];
        int numChunks = RGLO_NUM_SLAVES * ra->lChunks;
        int node, chunk;
        for (node = 0; node < RGLO_NUM_SLAVES; node++) {
            for (chunk = 0; chunk < ra->lChunks; chunk++) {
                if (chunk < ra->lChunks - 1) {
                    roomLeft[node * ra->lChunks + chunk] =
                        ra->lChunkSize / sizeof(uint64);
                } else {
                    roomLeft[node * ra->lChunks + chunk] =
                        ra->lLastChunkSize / sizeof(uint64);
                }
                char name[1024];
                RoomyArray_getChunkName(ra, node, chunk, name);
                wbs[node * ra->lChunks + chunk] = Roomy_makeWriteBuffer(
                    RGLO_BUFFER_SIZE, sizeof(uint64), name, node);
            }
        }
        
        //TESTING
        //Roomy_logAny("SETUP BUFFERS %s\n", name);

        // Put each element in a random chunk.
        uint64 elt;
        for (elt = 0; elt < ra->elts; elt++) {
            assert(numChunks > 0);

            chunk = rand() % numChunks;
            WriteBuffer_write(&(wbs[chunk]), &elt);
            roomLeft[chunk]--;
            
            // if chunk is full, close buffer and remove it from list
            if (roomLeft[chunk] == 0) {
                WriteBuffer_destroy(&(wbs[chunk]));
                int i;
                for (i = chunk + 1; i < numChunks; i++) {
                    wbs[i - 1] = wbs[i];
                    roomLeft[i - 1] = roomLeft[i];
                }
                numChunks--;
            }
        }
        assert(numChunks == 0);

        //TESTING
        //Roomy_logAny("WROTE NEW CHUNKS %s\n", name);
    }

    // Barrier.
    Roomy_barrier();
    Roomy_waitForRemoteWrites();

    //TESTING
    //Roomy_logAny("PASSED BARRIER %s\n", name);

    // Each chunk is randomly permuted locally.
    Array array;
    int chunk;
    for (chunk = 0; chunk < ra->lChunks; chunk++) {
        array = RoomyArray_loadArrayChunk(ra, chunk);
        uint64 size = Array_size(&array);
        uint64 i, j, iVal, jVal;
        for (i = 0; i < size-1; i++) {
            j = i + (rand() % (size - i - 1));
            if (i == j) {
                continue;
            }
            iVal = *((uint64*)Array_get(&array, i));
            jVal = *((uint64*)Array_get(&array, j));
            Array_set(&array, i, &jVal);
            Array_set(&array, j, &iVal);
        }
        RoomyArray_saveArrayChunk(ra, &array, chunk);
    }

    //TESTING
    //Roomy_logAny("LOCAL PERMUTE DONE %s\n", name);

    Roomy_barrier();

    //TESTING
    //Roomy_logAny("RETURNING %s\n", name);

    return ra;
}

/*********************** PERMUTATION MULTIPLICATION **************************/

// Create write buffers for bucketized X values. The 'wbs' array should
// be pre-allocated by the caller.
void RoomyArray_createBXWriteBuffers(RoomyArray* X,
                                     WriteBuffer wbs[]) {
    char filename[RGLO_STR_SIZE];
    int node, chunk;
    for (node = 0; node < RGLO_NUM_SLAVES; node++) {
        for (chunk = 0; chunk < X->lChunks; chunk++) {
            uint64 bucketNum = RoomyArray_nodeAndChunk2bucket(X, node, chunk);
            sprintf(filename, "%sBX_from%i_bucket%lli", RGLO_PERMUTE_DATA_PATH,
                    RGLO_MY_RANK, bucketNum);
            wbs[bucketNum] =
              Roomy_makeWriteBuffer(RGLO_BUFFER_SIZE, X->bytesPer, filename, node);
        }
    }
}

// Create a read buffer for file BX_fromN_bucketB, where N is the source node and
// B is the bucket number. Returns 1 on success, or 0 if the file does not exist.
int RoomyArray_createBXReadBuffer(RoomyArray* X,
                                   int sourceNode,
                                   uint64 bucket,
                                   ReadBuffer* rb) {
    char filename[RGLO_STR_SIZE];
    sprintf(filename, "%sBX_from%i_bucket%lli", RGLO_PERMUTE_DATA_PATH,
            sourceNode, bucket);
    if (fileExists(filename)) {
        *rb = ReadBuffer_make(RGLO_LARGE_BUFFER_SIZE, X->bytesPer, filename, 1);
        return 1;
    } else {
        return 0;
    }
}

// Create a write buffer for BY_fromN_bucketB, where N is the source node and B is
// the bucket number.
void RoomyArray_createBYWriteBuffer(RoomyArray* Y,
                                    int sourceNode,
                                    uint64 bucket,
                                    WriteBuffer* wb) {
    char filename[RGLO_STR_SIZE];
    sprintf(filename, "%sBY_from%i_bucket%lli", RGLO_PERMUTE_DATA_PATH,
            sourceNode, bucket);
    *wb = Roomy_makeWriteBuffer(RGLO_LARGE_BUFFER_SIZE,
                                Y->bytesPer,
                                filename,
                                sourceNode);
}

// Create read buffers for BY_* files. The 'rbs' array should be pre-allocated
// by the caller. Buffers will not be created for files that don't exist. The
// value of exists[i] will be 0 if rbs[i] is not created, and 1 otherwise.
void RoomyArray_createBYReadBuffers(RoomyArray* Y,
                                    ReadBuffer rbs[],
                                    int exists[]) {
    char filename[RGLO_STR_SIZE];
    int node, chunk;
    for (node = 0; node < RGLO_NUM_SLAVES; node++) {
        for (chunk = 0; chunk < Y->lChunks; chunk++) {
            uint64 bucketNum = RoomyArray_nodeAndChunk2bucket(Y, node, chunk);
            sprintf(filename, "%sBY_from%i_bucket%lli", RGLO_PERMUTE_DATA_PATH,
                    RGLO_MY_RANK, bucketNum);
            if (fileExists(filename)) {
                rbs[bucketNum] =
                    ReadBuffer_make(RGLO_BUFFER_SIZE, Y->bytesPer, filename, 1);
                exists[bucketNum] = 1;
            } else {
                exists[bucketNum] = 0;
            }
        }
    }
}

// Verify that the given X, Y, and Z arrays have the right number of elements,
// and the right size elements, for permutation multiplication. Print an error
// and quit if they do not.
void RoomyArray_verifyPermuteData(RoomyArray* X, RoomyArray* Y, RoomyArray* Z) {
    int success = 1;
    if (X->elts != Y->elts || X->elts != Z->elts) {
        fprintf(stderr,
            "Arrays given to RoomyArray_permute do not have the same length\n");
        fprintf(stderr, "  size(X) = %lli, size(Y) = %lli, size(Z) = %lli\n",
                  X->elts, Y->elts, Z->elts);
        success = 0;
    }
    if (X->bytesPer != sizeof(uint64)) {
        fprintf(stderr,
          "Elements of X array in RoomyArray_permute are not uint64\n");
        fprintf(stderr,
          "  %lli bytes per elements instead of %li\n",
          X->bytesPer, sizeof(uint64));
        success = 0;
    }
    if (Y->bytesPer != sizeof(uint64)) {
        fprintf(stderr,
          "Elements of Y array in RoomyArray_permute are not uint64\n");
        fprintf(stderr,
          "  %lli bytes per elements instead of %li\n",
          Y->bytesPer, sizeof(uint64));
        success = 0;
    }
    if (Z->bytesPer != sizeof(uint64)) {
        fprintf(stderr,
          "Elements of Z array in RoomyArray_permute are not uint64\n");
        fprintf(stderr,
          "  %lli bytes per elements instead of %li\n",
          Z->bytesPer, sizeof(uint64));
        success = 0;
    }
    if (!success) exit(1);
}

// Multiply two permutations, X and Y, storing the results in a third, Z.
// i.e., perform  Z[i] = Y[X[i]], for all i in [0, N), where N is the size of
// each RoomyArray. All arrays must be of the same size, and must contain
// uint64 elements.

// Global variables
WriteBuffer *BX_WBS;
RoomyArray *PERM_X, *PERM_Y, *PERM_Z;
uint64 PERM_NUM_BUCKETS;

// Function to map across X
void bucketizeEltX(uint64 i, void* elt) {
    uint64 bucket = RoomyArray_globalAddr2bucket(PERM_Y, *(uint64*)elt);
    WriteBuffer_write(&(BX_WBS[bucket]), elt);
}

// Step 1: bucktize the elements of X
void permuteStep1() {
    // open write buffers for each BX_fromN_bucket*
    BX_WBS = malloc(PERM_NUM_BUCKETS * sizeof(WriteBuffer));
    RoomyArray_createBXWriteBuffers(PERM_X, BX_WBS);

    // place each X[i] in a file named BX_fromN_bucketB on the node
    // owning bucket B (where B is a function of X[i])
    RoomyArray_map(PERM_X, bucketizeEltX);

    // close BX write buffers
    int i;
    for (i = 0; i < PERM_NUM_BUCKETS; i++) {
        WriteBuffer_destroy(&(BX_WBS[i]));
    }
    free(BX_WBS);
}

// Step 2: replace indices from X with elements from Y
void permuteStep2() {
    // for C = each of my Y-chunks
    Array chunk;
    int chunkNum;
    for (chunkNum = 0; chunkNum < PERM_Y->lChunks; chunkNum++) {
        chunk = RoomyArray_loadArrayChunk(PERM_Y, chunkNum);

        // for N = all nodes
        int node;
        for (node = 0; node < RGLO_NUM_SLAVES; node++) {
            // read file with name BX_fromN_bucketC
            uint64 bucketNum =
                RoomyArray_nodeAndChunk2bucket(PERM_Y, RGLO_MY_RANK, chunkNum);
            ReadBuffer BX_RB;
            if (!RoomyArray_createBXReadBuffer(PERM_X, node, bucketNum, &BX_RB)) {
                continue;
            }

            // for each element e, store Y[e] in a file named BY_fromN_bucketC
            // *on node N* (which produced BX_fromN_bucketC).
            WriteBuffer BY_WB;
            RoomyArray_createBYWriteBuffer(PERM_Y, node, bucketNum, &BY_WB);
            while (ReadBuffer_hasMore(&BX_RB)) {
                WriteBuffer_write(&BY_WB,
                  Array_get(&chunk,
                    RoomyArray_globalAddr2chunkAddr(PERM_Y,
                      *(uint64*)ReadBuffer_current(&BX_RB))));
                ReadBuffer_next(&BX_RB);
            }

            // close buffers, delete input file
            ReadBuffer_destroyAndDelete(&BX_RB);
            WriteBuffer_destroy(&BY_WB);
        }

        Array_destroy(&chunk);
    }
}

// Step 3: invert bucketization, yielding final result
void permuteStep3() {
    // open a read buffer to each BY_fromN_bucket*
    ReadBuffer BY_RBS[PERM_NUM_BUCKETS];
    int BY_exists[PERM_NUM_BUCKETS];
    RoomyArray_createBYReadBuffers(PERM_Y, BY_RBS, BY_exists);

    // for C = each of my X-chunks
    Array X_chunk, Z_chunk;
    int chunkNum;
    for(chunkNum = 0; chunkNum < PERM_X->lChunks; chunkNum++) {
        // Load X chunk and create new Z chunk
        X_chunk = RoomyArray_loadArrayChunk(PERM_X, chunkNum);
        uint64 chunkSize = Array_size(&X_chunk);
        Z_chunk = Array_make(PERM_Z->bytesPer, chunkSize);

        // for i = 1 to chunk size
        uint64 i;
        for (i = 0; i < chunkSize; i++) {
            // let B be the bucket specified by X[i]. store next element from
            // BY_fromN_bucketB in Z[i].
            uint64 bucket = RoomyArray_globalAddr2bucket(
                PERM_Y, *(uint64*)Array_get(&X_chunk, i));
            // Don't need assert, ReadBuffer_next will throw error if needed
            //assert(ReadBuffer_hasMore(&(BY_RBS[bucket])));
            Array_set(&Z_chunk, i, ReadBuffer_current(&(BY_RBS[bucket])));
            ReadBuffer_next(&(BY_RBS[bucket]));
        }
        
        Array_destroy(&X_chunk);
        RoomyArray_saveArrayChunk(PERM_Z, &Z_chunk, chunkNum);
    }
    
    // close all read buffers, delete all files
    int i;
    for (i = 0; i < PERM_NUM_BUCKETS; i++) {
        if (BY_exists[i]) {
            ReadBuffer_destroyAndDelete(&(BY_RBS[i]));
        }
    }
}

void RoomyArray_permute(RoomyArray* X, RoomyArray* Y, RoomyArray* Z) {
    RoomyArray_verifyPermuteData(X, Y, Z);

    // set global vars
    PERM_NUM_BUCKETS = X->lChunks * RGLO_NUM_SLAVES;
    PERM_X = X;
    PERM_Y = Y;
    PERM_Z = Z;

    // Step 1: bucktize the elements of X

    #ifdef TIMING
    Timer timer;
    char timeString[RGLO_STR_SIZE];
    Timer_start(&timer);
    #endif

    permuteStep1();

    // Barrier between steps.
    Roomy_barrier();
    Roomy_waitForRemoteWrites();

    #ifdef TIMING
    Timer_stop(&timer);
    Timer_print(&timer, timeString, RGLO_STR_SIZE);
    Roomy_log("  Permute step 1 done in %s\n", timeString);
    Timer_start(&timer);
    #endif

    // Step 2: replace indices from X with elements from Y
    permuteStep2();

    // Barrier between steps.
    Roomy_barrier();
    Roomy_waitForRemoteWrites();

    #ifdef TIMING
    Timer_stop(&timer);
    Timer_print(&timer, timeString, RGLO_STR_SIZE);
    Roomy_log("  Permute step 2 done in %s\n", timeString);
    Timer_start(&timer);
    #endif

    // Step 3: invert bucketization, yielding final result
    permuteStep3();

    // Barrier before completion.
    Roomy_barrier();

    #ifdef TIMING
    Timer_stop(&timer);
    Timer_print(&timer, timeString, RGLO_STR_SIZE);
    Roomy_log("  Permute step 3 done in %s\n", timeString);
    #endif
}

void RoomyArray_save(RoomyArray* ra);
RoomyArray RoomyArray_load(char* name);
