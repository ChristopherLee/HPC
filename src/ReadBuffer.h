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
 * ReadBuffer.h
 *
 * Read buffer for external data (i.e., on disk).
 *****************************************************************************/

#ifndef _READBUFFER_H
#define _READBUFFER_H

#include "Array.h"
#include "types.h"

#include <stdio.h>

/*************************************************************************
                           DATA STRUCTURES
*************************************************************************/

typedef struct {
    uint64 maxEltsInBuffer;   // maximum number of elts in memory buffer
    char* inFile;             // the input file containing bytes on disk 
    FILE* inFileDesc;         // file descriptor
    uint64 totalElts;         // the total number of elts in the file
    int eltSize;              // number of bytes in one element
    Array buffer;             // the buffer in memory
    uint64 bufferIndex;       // current index into memory buffer
    uint64 globalIndex;       // current index for overall data
    int empty;                // 1 if there are no elements left, 0 otherwise
    int keepOpen;             // 1 if files should be kept open between reads
} ReadBuffer;

/*************************************************************************
                          FUNCTION HEADERS 
*************************************************************************/

ReadBuffer ReadBuffer_make(uint64 bufferSize, int eltSize, char *inFile,
                           int keepOpen);
int ReadBuffer_hasMore(ReadBuffer *rb);
void ReadBuffer_next(ReadBuffer *rb);
void* ReadBuffer_current(ReadBuffer *rb);
void ReadBuffer_destroy(ReadBuffer *rb);
void ReadBuffer_destroyAndDelete(ReadBuffer *rb);
void ReadBuffer_bufferIn(ReadBuffer *rb, uint64 numElts);

#endif
