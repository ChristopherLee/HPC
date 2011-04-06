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
 * knusort.c
 *
 * Procedures for sorting, both in RAM and external sort.
 *****************************************************************************/

#include "ReadBuffer.h"
#include "WriteBuffer.h"
#include "types.h"

#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <string.h>
#include <unistd.h>

// minimum read/write size
const uint64 minTranSize = 1<<19; // 512K

/*************************************************************************
                             UTILITIES 
*************************************************************************/

const uint64 MB = 1<<20;
const uint64 GB = 1<<30;

// Globals for comparison of elements by key
int KEY_START = -1;
int KEY_SIZE = -1;

// Function pointer for the current comparison
int (*compare)(void* p1, void* p2, int size) = NULL;

/* do: *p1 = *p2; for value of size bytes */
/*
#define assign(p1,p2,size) { int ii; for(ii=0; ii<size; ii++) { \
                             *((p1)+ii) = *((p2)+ii); }}
*/
static inline void assign(void* p1, void* p2, int size) {
    if(p1 != p2) {
        memcpy(p1, p2, size);
    }
}

/* swap values of size bytes at pointers p1 and p2, using pointer tmp */
/*
#define swap(p1,p2,tmp,size) \
    {assign(tmp,p1,size); assign(p1,p2,size); assign(p2,tmp,size);}
*/
static inline void swap(void* p1, void* p2, void* tmp, int size) {
    assign(tmp, p1, size);
    assign(p1, p2, size);
    assign(p2, tmp, size);
}

/* Compare keys byte-by-byte */
int compareByKey(void* p1, void* p2, int size) {
    return memcmp((uint8*)p1 + KEY_START, (uint8*)p2 + KEY_START, KEY_SIZE);
}

/* Compare function for unsigned 64 bit integers */
int compare_uint64(void* p1, void* p2, int size) {
    assert(size == 8);
    uint64* val1 = (uint64*)p1;
    uint64* val2 = (uint64*)p2;
    
    // can't use subtraction because of possible overflow
    //return *val1 - *val2;

    if(*val1 < *val2) return -1;
    if(*val1 > *val2) return 1;
    return 0;
}

/* Return the number of bytes in the given file */
uint64 numBytesInFile(char* filename) {
    struct stat fileStats;
    if(stat(filename, &fileStats) == -1) {
        fprintf(stderr, "trying to stat: %s\n", filename);
        perror("numBytesInFile produced a file stat error");
        exit(1);
    }
    return fileStats.st_size;
}

/* Return the combined number of bytes in all of the given files. */
uint64 numBytesInAllFiles(char** fileNames, int numFiles) {
    uint64 sum = 0;
    int i;
    for(i=0; i<numFiles; i++) {
        sum += numBytesInFile(fileNames[i]);
    }
    return sum;
}

/* Concatenate the given list of files to the end of the given file,
 * deleting the files in the process
 */
void concatenateFiles(char* file, char** otherFiles, int numFiles) {
    FILE* f = fopen(file, "a");
    int i;
    for(i=0; i<numFiles; i++) {
        FILE* other = fopen(otherFiles[i], "r");
        int val = 0;
        while((val = fgetc(other)) != EOF) {
            fputc(val, f);
        }
        fclose(other);
        unlink(otherFiles[i]);
    }
    fclose(f);
}

/*************************************************************************
                            IN-CORE SORTING 
*************************************************************************/

/*************************************************************************
 * Sort the elements in the given buffer (using quicksort).
 */

/* Recursive helper function for quicksort */
void quicksortRecur(uint8* buffer, int64 lower, int64 upper, uint64 size) {
    uint8 tmp[size];
    uint8 pivot[size];
    int64 i, m;
    if(lower < upper) {
        // randomize pivot
        uint64 range = upper - lower + 1;
        uint64 pivotPos = lower + rand() % range;

        swap(buffer+lower*size, buffer+pivotPos*size, tmp, size);
        assign(pivot, buffer+lower*size, size);
        m = lower;
        for(i=lower+1; i<=upper; i++) {
            if(compare(buffer+i*size, pivot, size) < 0) {
                m++;
                swap(buffer+m*size, buffer+i*size, tmp, size);
            }
        }
        swap(buffer+lower*size, buffer+m*size, tmp, size);
        quicksortRecur(buffer, lower, m-1, size);
        quicksortRecur(buffer, m+1, upper, size);
    }
}

/* Sort num elements in the given buffer, with num elements of
 * size bytes each */
void quicksort(void* buffer, uint64 num, uint64 size) {
    quicksortRecur((uint8*)buffer, 0, num-1, size);
}

/*************************************************************************
                           EXTERNAL SORTING 
*************************************************************************/

/*************************************************************************
 * Use Merge-based sorting to sort the given number of elements from
 * the given files, where each element has the given number of bytes.
 * Elements are compared for equality/ordering only based on the key,
 * which is a contiguous portion of the element defined by the user.
 *
 * Values are taken from inFileName, the final sorted result is written to
 * outFileName, and temporary files will be created in tmpDir. Note, there
 * should be available disk space of at least twice the size of the input
 * file (for the temporary files and output).
 * The algorithm will use numBytesAvail bytes of storage for in-core
 * sorting.
 * If removeOriginals == 1, the original input files will be deleted
 * (which reduces the overall space requirements to equal the size of the
 *  input, instead of 2x).
 * If removeDupes == 1, duplicate values will be removed from the final
 * sorted file.
 *
 * There are three top level external sort methods, which use different methods
 * for determining ordering:
 *
 *   extMergeSortByByte: compares the entire element byte-by-byte
 *   extMergeSortByKey:  compares keys byte-by-byte
 *   extMergeSortByFunc: compares using a custom function
 *
 * Note, the first two methods are sensitive to endianness (byte ordering).
 */
void extMergeSortBase(char** inFileNames, int numFiles, int removeOriginals,
                  int removeDupes, char* outFileName, char* tmpDir,
                  int eltSize, uint64 numBytesAvail) {
    uint64 readChunkSize = numBytesAvail;
    // make sure readChunkSize is multiple of eltSize
    readChunkSize -= readChunkSize % eltSize;

    // count number of bytes in all files
    uint64 numBytes = 0;
    int i;
    for(i=0; i<numFiles; i++) {
        numBytes += numBytesInFile(inFileNames[i]);
    }

    // number of chunks to read in Phase 1
    uint64 numChunks = numBytes / readChunkSize;
    uint64 lastChunkSize = readChunkSize;
    // if the last chunk is uneven
    if(numBytes % readChunkSize) {
        numChunks++;
        lastChunkSize = numBytes % readChunkSize;
    }
    // if all data can fit in one buffer
    if(numBytes < readChunkSize) {
        numChunks = 1;
        lastChunkSize = numBytes;
    }

    assert(numBytes % eltSize == 0);
    
    // size of read buffer for each run in Phase 2
    uint64 runBufferSize = numBytesAvail / numChunks;

    // make sure the size of the read buffers is enough to insure streaming
    // access
    if(runBufferSize < minTranSize) {
        fprintf(stderr, "ERROR in extSort: Number of chunks is too large.\n");
        fprintf(stderr,
                "Sorting %lli bytes with %lli bytes available = %lli chunks\n",
                numBytes, numBytesAvail, numChunks);
        fprintf(stderr,
                "Transfer size would be only %lli bytes\n",
                runBufferSize);
        fprintf(stderr,
                "Increase numBytesAvail for sorting, or sort less data\n");
        exit(1);
    }

    // PHASE 1

    // set up a buffer for sorting in
    uint8* buffer = malloc(readChunkSize);
    
    // set up files
    int curFile = 0;
    uint64 bytesLeftInFile = numBytesInFile(inFileNames[curFile]);
    FILE* inFileDesc = fopen(inFileNames[curFile], "r");

    for(i=0; i<numChunks; i++) {
        // Read chunk
        uint64 chunkSize = readChunkSize;
        if(i == numChunks-1) chunkSize = lastChunkSize;
        uint64 bytesToRead = chunkSize;
        uint64 bytesRead = 0;
        while(bytesToRead > 0) {
            if(bytesLeftInFile >= bytesToRead) {
                uint64 num = fread(&buffer[bytesRead], 1, bytesToRead, inFileDesc);
                if(num < bytesToRead) perror("Not enough read");
                
                bytesRead += bytesToRead;
                bytesLeftInFile = bytesLeftInFile - bytesToRead;
                bytesToRead = 0;
            } else {
                uint64 num = 
                    fread(&buffer[bytesRead], 1, bytesLeftInFile, inFileDesc);
                if(num < bytesLeftInFile) perror("Not enough read");
                bytesRead += bytesLeftInFile;
                bytesToRead = bytesToRead - bytesLeftInFile;
                fclose(inFileDesc);
                curFile++;
                bytesLeftInFile = numBytesInFile(inFileNames[curFile]);
                inFileDesc = fopen(inFileNames[curFile], "r");
            }
        }
        assert(bytesRead == chunkSize);

        // Sort chunk
        quicksort(buffer, chunkSize/eltSize, eltSize);
        
        // Write run, removing dupes if needed
        char bufFileName[1024];
        sprintf(bufFileName, "%s/extSortRun%d", tmpDir, i);
        WriteBuffer runWB =
            WriteBuffer_make(runBufferSize, eltSize, bufFileName, 1);
        WriteBuffer_write(&runWB, &(buffer[0]));
        uint64 i;
        for(i=eltSize; i<chunkSize; i=i+eltSize) {
            if(!removeDupes ||
               compare(&(buffer[i]), &(buffer[i-eltSize]), eltSize) != 0) {
                WriteBuffer_write(&runWB, &(buffer[i]));
            }
        }
        WriteBuffer_destroy(&runWB);
    }
    fclose(inFileDesc);
    free(buffer);

    // remove original files, if needed
    if(removeOriginals) {
        int i;
        for(i=0; i<numFiles; i++) {
            remove(inFileNames[i]);
        }
    }
        
    // PHASE 2

    // if there was only one run, rename it to the final and return
    if(numChunks == 1) {
        char bufFileName[1024];
        sprintf(bufFileName, "%s/extSortRun%d", tmpDir, 0);

        if(rename(bufFileName, outFileName) == -1) {
            perror("rename in extSort failed");
        } else {
            return;
        }
    }

    // Set up input buffer for each run
    ReadBuffer rbs[numChunks];
    for(i=0; i<numChunks; i++) {
        char bufFileName[1024];
        sprintf(bufFileName, "%s/extSortRun%d", tmpDir, i);
        uint64 chunkSize = readChunkSize;
        if(i == numChunks-1) chunkSize = lastChunkSize;
        rbs[i] =
            ReadBuffer_make(runBufferSize, eltSize, bufFileName, 1);
    }

    // Merge inputs into single output buffer
    WriteBuffer wb = WriteBuffer_make(readChunkSize, eltSize, outFileName, 1);
    uint8 maxVal[eltSize];
    for(i=0; i<eltSize; i++) {
        maxVal[i] = 255;
    }
    uint8 min[eltSize];
    uint8 last[eltSize];
    assign(last, maxVal, eltSize);
    int minBuf = -1;
    int done[numChunks];
    memset(done, 0, sizeof(int) * numChunks);
    int numDone = 0;
    while(numDone < numChunks) {
        assign(min, maxVal, eltSize);
        minBuf = -1;
        for(i=0; i<numChunks; i++) {
            if(!done[i] &&
               compare(ReadBuffer_current(&rbs[i]), min, eltSize) <= 0) {
                assign(min, ReadBuffer_current(&rbs[i]), eltSize);
                minBuf = i;
            }
        }

        if(!removeDupes || compare(min, last, eltSize) != 0) {
            WriteBuffer_write(&wb, min);
            assign(last, min, eltSize);
        }
        ReadBuffer_next(&rbs[minBuf]);
        if(!ReadBuffer_hasMore(&rbs[minBuf])) {
            done[minBuf] = 1;
            numDone++;
        }
    }
    WriteBuffer_flush(&wb);

    // Destroy buffers
    for(i=0; i<numChunks; i++) {
        ReadBuffer_destroy(&rbs[i]);
    }
    WriteBuffer_destroy(&wb);

    // delete files
    for(i=0; i<numChunks; i++) {
        char bufFileName[1024];
        sprintf(bufFileName, "%s/extSortRun%d", tmpDir, i);
        remove(bufFileName);
    }
}

void extMergeSortByByte(char** inFileNames, int numFiles, int removeOriginals,
                  int removeDupes, char* outFileName, char* tmpDir,
                  int eltSize, uint64 numBytesAvail) {
    KEY_START = 0;
    KEY_SIZE = eltSize;
    compare = compareByKey;
    extMergeSortBase(inFileNames, numFiles, removeOriginals, removeDupes,
                     outFileName, tmpDir, eltSize, numBytesAvail);
}

void extMergeSortByKey(char** inFileNames, int numFiles, int removeOriginals,
                  int removeDupes, char* outFileName, char* tmpDir,
                  int eltSize, int keyStart, int keySize, uint64 numBytesAvail) {
    KEY_START = keyStart;
    KEY_SIZE = keySize;
    compare = compareByKey;
    extMergeSortBase(inFileNames, numFiles, removeOriginals, removeDupes,
                     outFileName, tmpDir, eltSize, numBytesAvail);
}

void extMergeSortByFunc(char** inFileNames, int numFiles, int removeOriginals,
                  int removeDupes, char* outFileName, char* tmpDir,
                  int eltSize, int (*compareFunc)(void* p1, void* p2, int size),
                  uint64 numBytesAvail) {
    KEY_START = 0;
    KEY_SIZE = eltSize;
    compare = compareFunc;
    extMergeSortBase(inFileNames, numFiles, removeOriginals, removeDupes,
                     outFileName, tmpDir, eltSize, numBytesAvail);
}

/*************************************************************************
                               TESTS 
*************************************************************************/

/*************************************************************************
 * Return 1 if the given file, with elements of the given size, is sorted.
 * Return 0 otherwise.
 */
int isSortedFile(char* filename, int eltSize, int keyStart, int keySize) {
    KEY_START = keyStart;
    KEY_SIZE = keySize;
    compare = compareByKey;
    ReadBuffer rb = ReadBuffer_make(MB, eltSize, filename, 1);
    uint8 last[eltSize];
    memset(last, 0, eltSize);

    while(ReadBuffer_hasMore(&rb)) {
        uint8* cur = (uint8*)ReadBuffer_current(&rb);
        if(compare(last, cur, eltSize) > 0) {
            return 0;
        }
        memcpy(last, cur, eltSize);
        ReadBuffer_next(&rb);
    }
    return 1;
}

