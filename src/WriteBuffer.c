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
 * WriteBuffer.c
 *
 * Write buffer for external data, either on a local disk or a remote disk.
 * Remote writes are handled with MPI messages.
 *****************************************************************************/

#include "WriteBuffer.h"
#include "params.h"
#include "types.h"
#include "roomy.h"

#include <assert.h>
#include <errno.h>
#include <fcntl.h>
#include <mpi.h>
#include <pthread.h>
#include <signal.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <sys/resource.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/types.h>
#include <sys/utsname.h>
#include <time.h>
#include <unistd.h>

// Maximum length of a file name for remote writing
#define MAX_FN_LEN 1024

// Arbitrary integer tags for two types of remote write MPI messages
// First message passes filename and number of bytes to write
// Second message passes data to be written
#define MSG_TAG_1 42
#define MSG_TAG_2 43


/*************************************************************************
 * Create a WriteBuffer
 */
WriteBuffer WriteBuffer_make(uint64 bufferSize, int eltSize,
                             char *outFile, int keepOpen) {

    WriteBuffer wb;
    wb.maxEltsInBuffer = bufferSize / eltSize;
    wb.outFile = outFile;
    wb.outFile = malloc(MAX_FN_LEN);
    strcpy(wb.outFile, outFile);
    wb.eltSize = eltSize;

    wb.buffer = Array_make(wb.eltSize, wb.maxEltsInBuffer);
    wb.bufferIndex = 0;
    wb.globalIndex = 0;

    wb.keepOpen = keepOpen;
    wb.dest = -1;

    if(wb.keepOpen) {
        wb.outFileDesc = fopen(wb.outFile, "a");
        if(wb.outFileDesc == NULL) perror("Error in WriteBuffer_make:");       
    } else {
        wb.outFileDesc = NULL;
    }

    return wb;
}

/*************************************************************************
 * Write numElts elements from the buffer to file.
 */
void WriteBuffer_bufferOut(WriteBuffer *wb, uint64 numElts) {
    if(wb->dest == -1) {
       WriteBuffer_bufferOutLocal(wb, numElts);
    } else {
       WriteBuffer_bufferOutRemote(wb, numElts);
    }
}

/*************************************************************************
 * Buffer data to local file
 */
void WriteBuffer_bufferOutLocal(WriteBuffer *wb, uint64 numElts) {
    if(numElts > 0) {
        if(!wb->keepOpen) {
            wb->outFileDesc = fopen(wb->outFile, "a");
            if(wb->outFileDesc == NULL) perror("Error in WriteBuffer_make:");
        }
        Array_fileDescSave(&(wb->buffer), wb->outFileDesc, numElts);
        if(!wb->keepOpen) fclose(wb->outFileDesc);
    }
}

/*************************************************************************
 * Flush all of the elements currently in the buffer to file
 */
void WriteBuffer_flush(WriteBuffer *wb) {
    assert(wb->bufferIndex <= wb->maxEltsInBuffer);
    WriteBuffer_bufferOut(wb, wb->bufferIndex);
    wb->bufferIndex = 0;
}

/*************************************************************************
 * Write the next element
 */
void WriteBuffer_write(WriteBuffer *wb, void* value) {
    Array_set(&(wb->buffer), wb->bufferIndex, value);
    wb->bufferIndex++;
    wb->globalIndex++;

    // if we're at end of buffer, flush
    if(wb->bufferIndex == wb->maxEltsInBuffer) WriteBuffer_flush(wb);
}

/*************************************************************************
 * Destroy WriteBuffer
 */
void WriteBuffer_destroy(WriteBuffer *wb) {
    if(wb->bufferIndex > 0) WriteBuffer_flush(wb);
    if(wb->keepOpen) fclose(wb->outFileDesc);
    Array_destroy(&(wb->buffer));
    free(wb->outFile);
}

/******************* REMOTE WRITING BUFFER FUNCTIONS ********************/

/*************************************************************************
 * Create a remote WriteBuffer
 */
WriteBuffer WriteBuffer_makeRemote(uint64 bufferSize, int eltSize,
                                   char *outFile, int dest) {
    WriteBuffer wb;
    wb.maxEltsInBuffer = bufferSize / eltSize;
    wb.outFile = outFile;
    wb.outFile = malloc(MAX_FN_LEN);
    strcpy(wb.outFile, outFile);
    wb.eltSize = eltSize;
    wb.buffer = Array_make(wb.eltSize, wb.maxEltsInBuffer);
    wb.bufferIndex = 0;
    wb.globalIndex = 0;
    wb.keepOpen = 0;
    wb.dest = dest;
    wb.outFileDesc = NULL;

    return wb;
}

/*************************************************************************
 * Write numElts elements from the buffer to file.
 */
void WriteBuffer_bufferOutRemote(WriteBuffer *wb, uint64 numElts) {
    if(numElts > 0)
        sendRemoteWrite(wb->dest, wb->outFile, numElts * wb->eltSize,
                        (wb->buffer).data);
}

/*************************************************************************
                             REMOTE WRITE
*************************************************************************/

static int* rankMap;
static MPI_Group rwGroup;
static MPI_Comm rwComm;
static int groupMade = 0;

/*************************************************************************
 * Create a mapping between ranks in the new rwGroup to standard IDs (based
 * on matching hostnames to a user supplied list of machine names)
 */
/*
int createRankMap_fragile(int numSlaves, int rwRank) {
    rankMap = malloc(numSlaves * sizeof(int));

    // get local absolute rank
    struct utsname nameStruct;
    uname(&nameStruct);
    int absRank = -1;
    
    int i;
    for(i=0; i<numSlaves; i++) {
        if(strstr(nameStruct.nodename, RPAR_NODE_NAMES[i])) {
            absRank = i;
            break;
        }
    }
    assert(absRank >= 0);

    // send rank map to master (rank 0 in RW group)
    MPI_Status status;
    if(rwRank > 0) {
        MPI_Send(&absRank, 1, MPI_INT, 0, 1, rwComm); 
    } else {
        rankMap[absRank] = 0;
        int i;
        for(i=0; i<numSlaves-1; i++) {
            int remAbsRank;
            MPI_Recv(&remAbsRank, 1, MPI_INT, MPI_ANY_SOURCE, MPI_ANY_TAG,
                     rwComm, &status);
            rankMap[remAbsRank] = status.MPI_SOURCE;
        }
    }

    // master broadcasts rank map to group
    if(rwRank == 0) {
        int i;
        for(i=1; i<numSlaves; i++) {
            MPI_Send(rankMap, numSlaves, MPI_INT, i, 1, rwComm); 
        }
    } else {
        MPI_Recv(rankMap, numSlaves, MPI_INT, 0, MPI_ANY_TAG, rwComm, &status);
    }

    return absRank;
}
*/

/*************************************************************************
 * Create a mapping between ranks in the new rwGroup to standard IDs.
 * This version is less fragile than above: it sorts the hostnames and
 * assigns ranks in that order. So, it doesn't depend on users specifying
 * the names of the machines being used by MPI.
 */

// a struct for mapping a string name to a rank
#define MAX_NAME_SIZE 1024
struct listelt {
    char name[MAX_NAME_SIZE];
    int rank;
    struct listelt* next;
};
typedef struct listelt NameAndRank;

// top level function for creating rank map
int createRankMap(int numSlaves, int rwRank) {
    rankMap = malloc(numSlaves * sizeof(int)); // global mapping array

    // get hostname
    struct utsname nameStruct;
    uname(&nameStruct);
    assert(strlen(nameStruct.nodename) < MAX_NAME_SIZE);
    char name[MAX_NAME_SIZE];
    strcpy(name, nameStruct.nodename);
    
    // send name to master, master sorts names and creates the rank map
    MPI_Status status;
    if(rwRank > 0) {
        MPI_Send(name, MAX_NAME_SIZE, MPI_CHAR, 0, 1, rwComm); 
    } else {
        // get all names and rwRanks, sort using insertion sort
        NameAndRank* curElt = malloc(sizeof(NameAndRank));
        strcpy(curElt->name, name);
        curElt->rank = 0;
        curElt->next = NULL;
        NameAndRank* head = curElt;
        char recvBuf[MAX_NAME_SIZE];
        int i;
        for(i=1; i<numSlaves; i++) {
            curElt = malloc(sizeof(NameAndRank));
            MPI_Recv(&recvBuf, MAX_NAME_SIZE, MPI_CHAR, MPI_ANY_SOURCE, MPI_ANY_TAG,
                     rwComm, &status);
            strcpy(curElt->name, recvBuf);
            curElt->rank = status.MPI_SOURCE;
            NameAndRank* insertPt1 = NULL;
            NameAndRank* insertPt2 = head;
            while(strcmp(insertPt2->name, curElt->name) < 0) {
                if(insertPt1 == NULL)
                    insertPt1 = head;
                else
                    insertPt1 = insertPt1->next;
                insertPt2 = insertPt2->next;
                if(insertPt2 == NULL) break;
            }
            if(insertPt1 == NULL)
                head = curElt;
            else
                insertPt1->next = curElt;
            curElt->next = insertPt2;
        }
        

        // create mapping from absolute ranks to rwRanks
        NameAndRank* curpt = head;
        NameAndRank* nextpt;
        for(i=0; i<numSlaves; i++) {
            rankMap[i] = curpt->rank;
            nextpt = curpt->next;
            free(curpt);
            curpt = nextpt;
        }
    }

    // master broadcasts rank map to group
    if(rwRank == 0) {
        int i;
        for(i=1; i<numSlaves; i++) {
            MPI_Send(rankMap, numSlaves, MPI_INT, i, 1, rwComm); 
        }
    } else {
        MPI_Recv(rankMap, numSlaves, MPI_INT, 0, MPI_ANY_TAG, rwComm, &status);
    }

    // find the absolute rank of this node and return it
    int absRank = -1;
    int i;
    for(i=0; i<numSlaves; i++) {
        if(rankMap[i] == rwRank) {
            absRank = i;
            break;
        }
    }
    assert(absRank != -1);

    return absRank;
}

/*************************************************************************
 * Set up a new MPI group, to be used for remote file writes. Returns the
 * new rank in the new group (in the range 0 to n, where n=numSlaves).
 */
int setupRemoteWriteGroup(int numSlaves) {
    MPI_Group globalGroup;
    MPI_Comm_group(MPI_COMM_WORLD, &globalGroup);
    
    int *slaveRanks = malloc(sizeof(int) * numSlaves);
    int i;
    for(i=0; i<numSlaves; i++) slaveRanks[i] = i;

    MPI_Group_incl(globalGroup, numSlaves, slaveRanks, &rwGroup);
    MPI_Comm_create(MPI_COMM_WORLD, rwGroup, &rwComm);
    groupMade = 1;

    int newRank;
    int realRank;
    MPI_Group_rank(rwGroup, &newRank);
    realRank = createRankMap(numSlaves, newRank);

    return realRank;
}

/*************************************************************************
 * Send a write request to a remote machine.
 */
void sendRemoteWrite(int dest, char *fileName, int count, void *buffer) {
    // send first message: file name and buffer size
    int msgSize = (MAX_FN_LEN * sizeof(char)) + sizeof(int);
    uint8* msgBuffer = malloc(msgSize);
    strcpy((char*)msgBuffer, fileName);
    int *countLoc = (int *)(msgBuffer + msgSize - sizeof(int));
    *countLoc = count;
    MPI_Send(msgBuffer, msgSize, MPI_BYTE, rankMap[dest], MSG_TAG_1, rwComm);
    free(msgBuffer);

    // send second message: data
    MPI_Send(buffer, count, MPI_BYTE, rankMap[dest], MSG_TAG_2, rwComm);
}

/*************************************************************************
 * Wait for the next remote write request, and send it to disk. Returns the
 * number of bytes written. Passes back the filename written to in the given
 * buffer.
 */
int recvRemoteWrite(char *fileName) {
    MPI_Status status;
    
    // recv first message: name and buffer size from any source
    int msgSize = (MAX_FN_LEN * sizeof(char)) + sizeof(int);
    uint8* msgBuffer = malloc(msgSize);
    MPI_Recv(msgBuffer, msgSize, MPI_BYTE, MPI_ANY_SOURCE, MSG_TAG_1,
             rwComm, &status);
    
    strcpy(fileName, (char*)msgBuffer);
    int dataSize = *((int *)(msgBuffer + msgSize - sizeof(int)));

    // recv second message: data from same source as first message
    uint8* dataBuffer = malloc(dataSize);
    MPI_Recv(dataBuffer, dataSize, MPI_BYTE, status.MPI_SOURCE, MSG_TAG_2,
             rwComm, &status);

    // write data to file
    FILE *f = fopen(fileName, "a");
    assert(f != NULL);
    size_t writeSize = fwrite(dataBuffer, 1, dataSize, f);
    assert(writeSize == dataSize);
    fclose(f);

    // clean up
    free(msgBuffer);
    free(dataBuffer);
    
    return dataSize;
}

/*************************************************************************
 * Start a remote write reciever in another thread.
 */
 
pthread_t recvThread;
int threadMade = 0;

void startRecv() {
    assert(groupMade);
    if(!threadMade) {
        int err = pthread_create(&recvThread, NULL, inftRecv, NULL);
        assert(err == 0);
        threadMade = 1;
    }
}

void* inftRecv() {
    int count;
    char fileName[MAX_FN_LEN];
    while(1) {
        count = recvRemoteWrite(fileName);
    }

    return NULL;
}

void stopRecv() {
    int err = pthread_kill(recvThread, 1);
    assert(err == 0);
}

