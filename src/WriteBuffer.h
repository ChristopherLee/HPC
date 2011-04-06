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
 * WriteBuffer.h
 *
 * Write buffer for external data, either on a local disk or a remote disk.
 * Remote writes are handled with MPI messages.
 *****************************************************************************/

#ifndef _WRITEBUFFERS_H
#define _WRITEBUFFERS_H

#include "Array.h"
#include "types.h"

/*************************************************************************
                           DATA STRUCTURES
*************************************************************************/

typedef struct {
    uint64 maxEltsInBuffer;   // maximum number of elts in memory buffer
    char *outFile;            // the output file containing bytes on disk 
    FILE* outFileDesc;        // file descriptor
    int eltSize;              // number of bytes in one element
    Array buffer;             // the buffer in memory
    uint64 bufferIndex;       // current index into memory buffer
    uint64 globalIndex;       // current index for overall data
    int keepOpen;             // 1 if files should be kept open between writes
    int dest;                 // remote destination node (MPI Rank), -1 if local
} WriteBuffer;

/*************************************************************************
                          FUNCTION HEADERS 
*************************************************************************/

// WriteBuffer methods

WriteBuffer WriteBuffer_make(uint64 bufferSize, int eltSize,
                             char *outFile, int keepOpen);
void WriteBuffer_flush(WriteBuffer *wb);
void WriteBuffer_write(WriteBuffer *wb, void* value);
void WriteBuffer_destroy(WriteBuffer *wb);
WriteBuffer WriteBuffer_makeRemote(uint64 bufferSize, int eltSize,
                                   char *outFile, int dest);
void WriteBuffer_bufferOutLocal(WriteBuffer *wb, uint64 numElts);
void WriteBuffer_bufferOutRemote(WriteBuffer *wb, uint64 numElts);
                             
// Remote write methods
int createRankMap(int numSlaves, int rwRank);
int setupRemoteWriteGroup(int numSlaves);
void sendRemoteWrite(int dest, char *fileName, int count, void *buffer);
int recvRemoteWrite(char *fileName);
void startRecv();
void* inftRecv();

#endif

