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
 * ReadBuffer.c
 *
 * Read buffer for external data (i.e., on disk).
 *****************************************************************************/

#include "Array.h"
#include "ReadBuffer.h"
#include "types.h"

#include <stdio.h>
#include <fcntl.h>
#include <unistd.h>
#include <stdlib.h>
#include <errno.h>
#include <assert.h>
#include <time.h>
#include <string.h>
#include <sys/resource.h>
#include <sys/time.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <pthread.h>
#include <sys/utsname.h>

#define MAX_FN_LEN 1024

/*************************************************************************
 * Create a ReadBuffer, using bufferSize bytes in RAM, holding eltSize
 * elements, and reading from inFile containing totalBytes.
 * If keepOpen == 1, inFile will be kept open between reads.
 */
ReadBuffer ReadBuffer_make(uint64 bufferSize, int eltSize, char *inFile,
                           int keepOpen) {

    ReadBuffer rb;
    rb.eltSize = eltSize;
    rb.inFile = malloc(MAX_FN_LEN);
    strcpy(rb.inFile, inFile);
    rb.keepOpen = keepOpen;
    rb.maxEltsInBuffer = bufferSize / eltSize;

    // find file size to determine total number of elements
    struct stat fileStats;
    if(stat(rb.inFile, &fileStats) == -1) {
        fprintf(stderr, "Tried to stat: %s\n", rb.inFile);
        perror("ReadBuffer file stat error");
    }
    assert(fileStats.st_size % rb.eltSize == 0);
    rb.totalElts = fileStats.st_size / rb.eltSize;

    // if file is empty, set buffer as empty 
    if(rb.totalElts)
        rb.empty = 0;
    else
        rb.empty = 1;

    // open file, if keeping open all of the time
    if(rb.keepOpen) {
        rb.inFileDesc = fopen(rb.inFile, "r");
        if(rb.inFileDesc == NULL) perror("Error in ReadBuffer_make:");
    }

    // create buffer
    uint64 fillSize =
        (rb.maxEltsInBuffer < rb.totalElts) ?
        rb.maxEltsInBuffer :
        rb.totalElts;
    rb.buffer = Array_make(rb.eltSize, fillSize);
    rb.bufferIndex = 0;
    rb.globalIndex = 0;

    // initial fill of buffer
    ReadBuffer_bufferIn(&rb, fillSize);

    return rb;
}

/*************************************************************************
 * Read the next numElts elements in the given ReadBuffer.
 */
void ReadBuffer_bufferIn(ReadBuffer *rb, uint64 numElts) {
    if(! rb->keepOpen) {
        rb->inFileDesc = fopen(rb->inFile, "r");
        if(rb->inFileDesc == NULL) perror("Error in ReadBuffer_make:");
    
        if(fseek(rb->inFileDesc, rb->globalIndex * rb->eltSize, SEEK_SET))
            perror("Error positioning file in ReadBuffer_bufferIn:");
    }
    
    Array_fileDescLoad(&(rb->buffer), rb->inFileDesc, numElts);
    
    if(! rb->keepOpen) {
        fclose(rb->inFileDesc);
    }
}

/*************************************************************************
 * Return 1 if the ReadBuffer has more elements, 0 otherwise
 */
int ReadBuffer_hasMore(ReadBuffer *rb) {
    return ! rb->empty;
}

/*************************************************************************
 * Go to the next element
 */
void ReadBuffer_next(ReadBuffer *rb) {
    rb->bufferIndex++;
    rb->globalIndex++;
    
    // if we're at end of global data, set empty flag
    if(rb->globalIndex >= rb->totalElts) {
        rb->empty = 1;
    // if we're at end of buffer, re-fill
    } else if(rb->bufferIndex >= rb->maxEltsInBuffer) {
        rb->bufferIndex = 0;
        // fill entire buffer, or rest of file, whichever is smaller
        uint64 fillSize =
            (rb->maxEltsInBuffer < rb->totalElts - rb->globalIndex) ?
            rb->maxEltsInBuffer :
            rb->totalElts - rb->globalIndex;
        ReadBuffer_bufferIn(rb, fillSize);
    }
}

/*************************************************************************
 * Return a pointer to the current element
 */
void* ReadBuffer_current(ReadBuffer *rb) {
    if(!ReadBuffer_hasMore(rb)) {
        perror("Tried next on an empty ReadBuffer");
        exit(1);
    }

    return Array_get(&(rb->buffer), rb->bufferIndex);
}

/*************************************************************************
 * Destroy ReadBuffer: close file and free memory.
 */
void ReadBuffer_destroy(ReadBuffer *rb) {
    Array_destroy(&(rb->buffer));
    free(rb->inFile);
    if(rb->keepOpen) {
        fclose(rb->inFileDesc);
    }
}

/*************************************************************************
 * Destroy ReadBuffer and delete underlying file.
 */
void ReadBuffer_destroyAndDelete(ReadBuffer *rb) {
    ReadBuffer_destroy(rb);
    unlink(rb->inFile);
}
