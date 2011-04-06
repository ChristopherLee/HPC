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
 * roomy.c
 *
 * Roomy: a library for using high-latency storage as main memory.
 *****************************************************************************/

#include "params.h"
#include "types.h"
#include "mpi.h"
#include "WriteBuffer.h"
#include "knudirs.h"
#include "knusort.h"
#include "knustring.h"
#include "roomy.h"
#include "RoomyArray.h"
#include "RoomyList.h"
#include "RoomyHashTable.h"
#include "Timer.h"

#include <assert.h>
#include <stdlib.h>
#include <stdio.h>
#include <sys/stat.h>
#include <string.h>
#include <sys/time.h>
#include <stdarg.h>
#include <time.h>
#include <unistd.h>

// Standardized rank based on hostname
int RGLO_MY_RANK = 0;

// Rank in default MPI_COMM_WORLD
int RGLO_MPI_RANK = 0;

// Number of slaves (one less than the number of MPI tasks)
int RGLO_NUM_SLAVES = 0;

// Flag set when computation is within a Roomy map function, use to determine
// if some action (e.g. an update) should be processed by only the master, or
// by all slaves.
int RGLO_IN_PARALLEL = 0;

// If 1, use mmap for disk-based arrays, instead of explicitly reading/writing
// each chunk. This should provide a speedup when array updates are sparse.
int RGLO_USE_MMAP = 1;

// The maximum size (in bytes) of a data structure chunk (i.e. the maximum
// size of the piece of the disk-based data structure in RAM at one time)
//uint64 RGLO_MAX_CHUNK_SIZE = 1024 * 1024;  // 1 MB
//uint64 RGLO_MAX_CHUNK_SIZE = 64 * 1024 * 1024;  // 64 MB
//uint64 RGLO_MAX_CHUNK_SIZE = 256 * 1024 * 1024;  // 256 MB
//uint64 RGLO_MAX_CHUNK_SIZE = 512 * 1024 * 1024;  // 512 MB
uint64 RGLO_MAX_CHUNK_SIZE = 1024 * 1024 * 1024;  // 1 GB

// The maximum size of one subtable in a RoomyHashTable. This is smaller than
// the maximum size of array chunks because the subtables of a RoomyHashTable
// can dynamically increase in size as elements are inserted.
// So, a size of RGLO_MAX_CHUNK_SIZE / 8 will allow subtable to double in size
// three times before reaching the max chunk size.
// If the RoomyHashTables are more accurately sized, this can be larger.
//uint64 RGLO_MAX_SUBTABLE_SIZE = 128 * 1024 * 1024; // 128 MB
uint64 RGLO_MAX_SUBTABLE_SIZE = 1024 * 1024 * 1024; // 1 GB

// The size of the buffer for each ReadBuffer and WriteBuffer. So, this is also
// the size of read and write opperations to disk. It should be large enough
// to ensure full streaming access speed.
//uint64 RGLO_BUFFER_SIZE = 2 * 1024 * 1024;  // 2 MB
uint64 RGLO_BUFFER_SIZE = 4 * 1024 * 1024;  // 4 MB
//uint64 RGLO_BUFFER_SIZE = 16 * 1024 * 1024;  // 16 MB

// A buffer size to be used when create only one (or a small constant number) of
// buffers. The smaller size is typically used when opening one buffer for each
// chunk of a RoomyArray.
uint64 RGLO_LARGE_BUFFER_SIZE = 128 * 1024 * 1024;  // 128 MB

// The amount of memory held back for misc. data structures
uint64 RGLO_MEM_GAP = 0;
//uint64 RGLO_MEM_GAP = 64 * 1024 * 1024;  // 64 MB

// TODO: The variables below tracking RAM and disk remaining are not currently
// used.  This should be implemented, and used to provide warnings / errors to
// the user, and possibly to make policy decisions (e.g., the size of read /
// write buffers).

// RAM max/remaining (WARNING: only keeping track of some RAM allocation,
//                    TODO: wrap malloc and free to keep better track).
uint64 RGLO_RAM_MAX;
int64 RGLO_RAM_LEFT;

// Disk max/remaining (WARNING: currently not being used, TODO).
uint64 RGLO_DISK_MAX;
int64 RGLO_DISK_LEFT;

// The directory to store all Roomy data structures in
char RGLO_DATA_PATH[RGLO_STR_SIZE];

// The sub-directory to store temporary array permutation data in
char RGLO_PERMUTE_DATA_PATH[RGLO_STR_SIZE];

// The maximum number of Roomy data structures that can exist at one time.
int RGLO_MAX_STRUCTS = 65536;

// HashTables containing pointers to all Roomy data structures
HashTable RGLO_RA_LIST;
HashTable RGLO_RL_LIST;
HashTable RGLO_RHT_LIST;

// The next unique number to be returned by Roomy_uniqueInt() in parallel and
// serial modes. All slaves will use the same number in serial mode, and different
// numbers in parallel mode.
uint64 RGLO_NEXT_UNIQUE_NUM_PAR;
uint64 RGLO_NEXT_UNIQUE_NUM_SER;

// Create a roomy subdir in the root dir, if it doesn't already exist.
// Params must be initialized.
void makeRoomyDir() {
    // check user-specified directory exists
    if(!fileExists(RPAR_DISK_DATA_PATH)) {
        fprintf(stderr,
                "DISK_DATA_PATH %s does not exist\n", RPAR_DISK_DATA_PATH);
        exit(1);
    }

    // create roomy dir
    sprintf(RGLO_DATA_PATH, "%s/roomy/", RPAR_DISK_DATA_PATH);
    Roomy_makeDir(RGLO_DATA_PATH);

    // make sub-dir for remote write locks
    char lockDir[RGLO_STR_SIZE];
    sprintf(lockDir, "%slocks/", RGLO_DATA_PATH);
    Roomy_destroyDir(lockDir);
    Roomy_makeDir(lockDir);

    // make a sub-dir for temporary data produced by RoomyArray_permute
    sprintf(RGLO_PERMUTE_DATA_PATH, "%spermute/", RGLO_DATA_PATH);
    Roomy_destroyDir(RGLO_PERMUTE_DATA_PATH);
    Roomy_makeDir(RGLO_PERMUTE_DATA_PATH);
}

// Print a message from any slave, even if not in parallel mode.
void Roomy_logAny(char* message, ...) {
    char buf[RGLO_STR_SIZE];
    va_list vl_vars;
    struct timeval s_time;
    
    va_start(vl_vars, message);
    gettimeofday(&s_time, NULL);
    ctime_r(&(s_time.tv_sec), buf);
    sprintf(&(buf[strlen(buf)-1]), " [rank %i] ", RGLO_MY_RANK);
    strcat(buf, message);
    vfprintf(stdout, buf, vl_vars);
    fflush(stdout);
}

// Print a Roomy debug / tracing message. Only one slave prints the message
// in serial mode, any slave prints in parallel mode.
void Roomy_log(char* message, ...) {
    if(!RGLO_MY_RANK || RGLO_IN_PARALLEL) {
        char buf[RGLO_STR_SIZE];
        va_list vl_vars;
        struct timeval s_time;
        
        va_start(vl_vars, message);
        gettimeofday(&s_time, NULL);
        ctime_r(&(s_time.tv_sec), buf);
        sprintf(&(buf[strlen(buf)-1]), ": ");
        strcat(buf, message);
        vfprintf(stdout, buf, vl_vars);
        fflush(stdout);
    }
}

// Have one slave print a message without logging information (i.e., date/time).
void Roomy_print(char* message, ...) {
    if(!RGLO_MY_RANK || RGLO_IN_PARALLEL) {
        va_list vl_vars;
        va_start(vl_vars, message);
        vfprintf(stdout, message, vl_vars);
        fflush(stdout);
    }
}

// Have all slaves print a message without logging information (i.e., date/time).
void Roomy_printAny(char* message, ...) {
    char buf[RGLO_STR_SIZE];
    va_list vl_vars;
    
    va_start(vl_vars, message);
    sprintf(buf, "[rank %i] ", RGLO_MY_RANK);
    strcat(buf, message);
    vfprintf(stdout, buf, vl_vars);
    fflush(stdout);
}

// Get params, start MPI, etc.
void Roomy_init(int* argc, char*** argv) {
    Timer_start(&RSTAT_INIT_TIMER);

    // for now, just use file "params.in" in the current directory, make it
    // an optional command line arg in the future
    parseParams("params.in");

    // initialize MPI to use threads
    int required;
    if (RPAR_SHARED_DISK) {
        required = MPI_THREAD_SINGLE;
    } else {
        required = MPI_THREAD_MULTIPLE;
    }
    int provided;
    MPI_Init_thread(argc, argv, required, &provided);
    assert(provided >= required);

    // create new group and comm to be used by remote write utility, if we're not
    // using shared disk. for shared disk, just use the default group and ranks.
    MPI_Comm_size(MPI_COMM_WORLD, &RGLO_NUM_SLAVES);
    if(!RPAR_SHARED_DISK) {
        RGLO_MY_RANK = setupRemoteWriteGroup(RGLO_NUM_SLAVES);
        startRecv();
    } else {
        MPI_Comm_rank(MPI_COMM_WORLD, &RGLO_MY_RANK);
    }
    MPI_Comm_rank(MPI_COMM_WORLD, &RGLO_MPI_RANK);
    
    // make sure data structure directory exists
    makeRoomyDir();

    // calculate usable space
    RGLO_RAM_MAX = RPAR_MB_RAM_PER_NODE * 1<<20;
    RGLO_RAM_LEFT = RGLO_RAM_MAX - RGLO_MEM_GAP;
    RGLO_DISK_MAX = RPAR_GB_DISK_PER_NODE * 1<<30;
    RGLO_DISK_LEFT = RGLO_DISK_MAX;

    // init global lists of data structures
    RGLO_RA_LIST = HashTable_make(sizeof(RoomyArray*), 0, RGLO_MAX_STRUCTS);
    RGLO_RL_LIST = HashTable_make(sizeof(RoomyList*), 0, RGLO_MAX_STRUCTS);
    RGLO_RHT_LIST = HashTable_make(sizeof(RoomyHashTable*), 0, RGLO_MAX_STRUCTS);

    // init globals for Roomy_uniqueInt()
    RGLO_NEXT_UNIQUE_NUM_PAR = RGLO_MY_RANK;
    RGLO_NEXT_UNIQUE_NUM_SER = RGLO_NUM_SLAVES;

    Roomy_initStats();
}

// Shut everything down
void Roomy_finalize() {
    MPI_Finalize();
}

// Register a newly created Roomy data structure.
void Roomy_registerRoomyArray(RoomyArray* ra) {
    if (HashTable_size(&RGLO_RA_LIST) + 1 > RGLO_MAX_STRUCTS) {
        Roomy_log(
          "ERROR: too many RoomyArrays, need to increase RGLO_MAX_STRUCTS\n");
        exit(1);
    }
    HashTable_insert(&RGLO_RA_LIST, ra, NULL);
}
void Roomy_registerRoomyList(RoomyList* rl) {
    if (HashTable_size(&RGLO_RL_LIST) + 1 > RGLO_MAX_STRUCTS) {
        Roomy_log(
          "ERROR: too many RoomyLists, need to increase RGLO_MAX_STRUCTS\n");
        exit(1);
    }
    HashTable_insert(&RGLO_RL_LIST, rl, NULL);
}
void Roomy_registerRoomyHashTable(RoomyHashTable* rht) {
    if (HashTable_size(&RGLO_RHT_LIST) + 1 > RGLO_MAX_STRUCTS) {
        Roomy_log(
          "ERROR: too many RoomyHashTables, need to increase RGLO_MAX_STRUCTS\n");
        exit(1);
    }
    HashTable_insert(&RGLO_RHT_LIST, rht, NULL);
}


// Unregister a destroyed Roomy data structure.
void Roomy_unregisterRoomyArray(RoomyArray* ra) {
    HashTable_delete(&RGLO_RA_LIST, ra);
}
void Roomy_unregisterRoomyList(RoomyList* rl) {
    HashTable_delete(&RGLO_RL_LIST, rl);
}
void Roomy_unregisterRoomyHashTable(RoomyHashTable* rht) {
    HashTable_delete(&RGLO_RHT_LIST, rht);
}

// Return 1 if there is a Roomy data structure with the given name. Return 0
// otherwise. Previously destroyed data structures are not considered.
char* NAME_TO_CHECK;
int NAME_EXISTS;
void Roomy_sameNameRA(void* key, void* value) {
    RoomyArray* ra = (RoomyArray*)key;
    if (strcmp(ra->name, NAME_TO_CHECK) == 0) {
        NAME_EXISTS = 1;
    }
}
void Roomy_sameNameRL(void* key, void* value) {
    RoomyList* rl = (RoomyList*)key;
    if (strcmp(rl->name, NAME_TO_CHECK) == 0) {
        NAME_EXISTS = 1;
    }
}
void Roomy_sameNameRHT(void* key, void* value) {
    RoomyHashTable* rht = (RoomyHashTable*)key;
    if (strcmp(rht->name, NAME_TO_CHECK) == 0) {
        NAME_EXISTS = 1;
    }
}
int Roomy_nameExists(char* name) {
    NAME_EXISTS = 0;
    NAME_TO_CHECK = name;
    HashTable_map(&RGLO_RA_LIST, Roomy_sameNameRA);
    if (NAME_EXISTS) return 1;
    HashTable_map(&RGLO_RL_LIST, Roomy_sameNameRL);
    if (NAME_EXISTS) return 1;
    HashTable_map(&RGLO_RHT_LIST, Roomy_sameNameRHT);
    return NAME_EXISTS;
}

// Remove all files related to the Roomy data structure with the given path
// (recursively removes all files in the given directory, then removes directory)
// Returns the total number of bytes deleted.
uint64 Roomy_destroyDir(char* dir) {
    uint64 bytesDel = 0;
    // if using shared disk, only one node needs to destroy the directory.
    // need a barrier at the end to make sure it's done on return.
    if (!RPAR_SHARED_DISK || RGLO_MY_RANK == 0) {
        bytesDel = recursivelyDeleteDir(dir);
    }

    if (RPAR_SHARED_DISK) {
        Roomy_barrier();
    }

    return bytesDel;
}

// Create the a directory with the given name. Has no effect if the directory
// already exists. Causes fatal error if directory can not be created.
void Roomy_makeDir(char* dir) {
    // if using shared disk, only one node needs to create dir
    // need a barrier at the end to make sure it's done on return.
    if (!RPAR_SHARED_DISK || RGLO_MY_RANK == 0) {
        if (!fileExists(dir) && mkdir(dir, S_IRWXU) != 0) {
            fprintf(stderr, "Could not create directory: %s\n", dir);
            exit(1);
        }
    }

    if (RPAR_SHARED_DISK) {
        Roomy_barrier();
    }
}

// Record that size bytes have been buffered for a delayed update.
// TODO: currently, we just log an error if RGLO_DISK_LEFT reaches zero. In the
// future, we may attempt to automatically processes pending updates to free space,
// but this will cause problems if processing the updates also produces more data.
void Roomy_bufferedData(uint64 size) {
    RGLO_DISK_LEFT -= size;
    if (RGLO_DISK_LEFT <= 0) {
        //Roomy_log("WARNING: disk space limit reached (RGLO_DISK_LEFT = %lli)",
        //          RGLO_DISK_LEFT);
    }

    // Potential solution for automatically freeing disk space
    /*
    if(RGLO_DISK_LEFT < RGLO_DISK_MAX/2) {
        Roomy_freeSpace();
        // if we're still out of disk space, exit with error
        if(RGLO_DISK_LEFT < RGLO_DISK_MAX/2) {
            Roomy_log("ERROR: Insufficient disk space\n");
            exit(1);
        }
    }
    */
}

// Record that the given number of bytes have been removed from disk.
void Roomy_dataRemoved(uint64 size) {
    RGLO_DISK_LEFT += size;
}

// Record that size bytes have been allocated in RAM. Print a warning if
// RGLO_RAM_LEFT goes below zero.
void Roomy_ramAllocated(uint64 size) {
    RGLO_RAM_LEFT -= size;
    if (RGLO_RAM_LEFT < 0) {
        Roomy_log("WARNING: %lli bytes of RAM in use (%lli max)\n",
                  RGLO_RAM_MAX - RGLO_RAM_LEFT, RGLO_RAM_MAX);
    }
}

// Record that size bytes have been freed in RAM.
void Roomy_ramFreed(uint64 size) {
    RGLO_RAM_LEFT += size;
}

// Get the lock name for the given node.
void Roomy_getLockName(char* name, int from, int to) {
    sprintf(name, "%slocks/RWLOCK_from%i_to%i", RGLO_DATA_PATH, from, to);
}

// Return 1 if this node has a remote write lock from all other nodes.
// Return 0 otherwise.
int Roomy_allLocksIn() {
    char name[1024];
    int i;
    for(i=0; i<RGLO_NUM_SLAVES; i++) {
        if(i != RGLO_MY_RANK) {
            Roomy_getLockName(name, i, RGLO_MY_RANK);
            if(!fileExists(name)) return 0;
        }
    }
    return 1;
}

// Delete all remote write locks on this node.
void Roomy_deleteLocks() {
    char name[1024];
    int i;
    for(i=0; i<RGLO_NUM_SLAVES; i++) {
        if(i != RGLO_MY_RANK) {
            Roomy_getLockName(name, i, RGLO_MY_RANK);
            if(fileExists(name)) unlink(name);
        }
    }
}

// Make sure all remote writes are complete. Send a remote write to a lock
// file on each other node, and wait for all lock files from other nodes to
// be written locally before continuing.
void Roomy_waitForRemoteWrites() {
    #ifdef DEBUG
    Roomy_logAny("Waiting for remote writes to flush.\n");
    #endif 

    // STATS
    Timer t;
    Timer_start(&t);

    char name[1024];
    uint8 num = 42;
    int i;
    for(i=0; i<RGLO_NUM_SLAVES; i++) {
        if(i != RGLO_MY_RANK) {
            Roomy_getLockName(name, RGLO_MY_RANK, i);
            if(RPAR_SHARED_DISK) {
                FILE* f = fopen(name, "w");
                fwrite(&num, 1, 1, f);
                fclose(f);
            } else {
                sendRemoteWrite(i, name, 1, &num);
            }
        }
    }

    int retrys = 0;
    struct timespec ts;
    ts.tv_sec = 0;
    //ts.tv_nsec = 100000000; // 0.1 seconds
    //ts.tv_nsec = 10000000;    // 0.01 seconds
    ts.tv_nsec = 1000000;    // 0.001 seconds
    while(!Roomy_allLocksIn()) {
        retrys++;
        nanosleep(&ts, NULL);
    }

    Roomy_deleteLocks();

    // STATS
    Timer_stop(&t);
    RSTAT_WAIT_WRITE_TIME += Timer_elapsed_usec(&t);

    #ifdef DEBUG
    Roomy_logAny("Remote writes flushed.\n");
    #endif 
}

// Return a unique integer. It is unique over all compute nodes, since
// Roomy_init(), or the last call to Roomy_resetUniqueInt().
uint64 Roomy_uniqueInt() {
    uint64 nextNum;
    if(RGLO_IN_PARALLEL) {
        nextNum = RGLO_NEXT_UNIQUE_NUM_PAR;
        RGLO_NEXT_UNIQUE_NUM_PAR += RGLO_NUM_SLAVES + 1;
    } else {
        nextNum = RGLO_NEXT_UNIQUE_NUM_SER;
        RGLO_NEXT_UNIQUE_NUM_SER += RGLO_NUM_SLAVES + 1;
    }
    return nextNum;
}

// Reset unique numbers returned by Roomy_uniqueInt(). Unique number will not
// be less than the given minimum.
void Roomy_resetUniqueInt(uint64 min) {
    RGLO_NEXT_UNIQUE_NUM_PAR = min + RGLO_MY_RANK;
    RGLO_NEXT_UNIQUE_NUM_SER = min + RGLO_NUM_SLAVES;
}

// Clear disk space. Not yet implemented. One possibility: sync all data
// structures to remove any pending delayed operations.
/*
void Roomy_freeSpace() {
}
*/

// Make a WriteBuffer for elements of the given size, being written to a file
// with the given name on the given node.
WriteBuffer Roomy_makeWriteBuffer(uint64 bufferSize,
                                  int eltSize,
                                  char* filename,
                                  int node) {
    if(node == RGLO_MY_RANK || RPAR_SHARED_DISK) {
        return WriteBuffer_make(bufferSize, eltSize, filename, 0);
    } else {
        return WriteBuffer_makeRemote(bufferSize, eltSize, filename, node);
    } 
}

/******************************************************************************
 *                         Parallel:  Simple Barrier
 * A simple MPI barrier, that transfers an arbitrary block of data from each
 * slave to the master, blocking all slaves until the master has all responses.
 * Note that this uses the standard MPI group (i.e. uses MPI_COMM_WORLD), and
 * not the Roomy remote write group created in WriteBuffer.c. So, we use
 * RGLO_MPI_RANK instead of MPI_MY_RANK in barrier methods.
 *****************************************************************************/

// Recieve the result of a barrier from all slaves, and get data from each
void Roomy_masterRecvBarrier(void* buffer, int bytesPerSlave) {
    int numSlavesDone = 0;
    MPI_Status status;
    uint8* buf = (uint8*)buffer;
    while(numSlavesDone < RGLO_NUM_SLAVES-1) {
        uint8* cur = &(buf[numSlavesDone*bytesPerSlave]);
        MPI_Recv(cur, bytesPerSlave, MPI_BYTE, MPI_ANY_SOURCE, MPI_ANY_TAG,
                 MPI_COMM_WORLD, &status);
        numSlavesDone++;
    }
}

// Notify all slaves that barrier is complete, sending same data to each
void Roomy_masterNotifySlaves(void* buffer, int bytes) {
    int i;
    for(i=1; i<RGLO_NUM_SLAVES; i++) {
        MPI_Send(buffer, bytes, MPI_BYTE, i, 0, MPI_COMM_WORLD);
    }
}

// Send the result of a barrier to the master
void Roomy_slaveSendBarrier(void* buffer, int bytes) {
    MPI_Send(buffer, bytes, MPI_BYTE, 0, 0, MPI_COMM_WORLD);
}

// Wait until the master notifies barrier complete, and get data
void Roomy_slaveWaitForBarrier(void* buffer, int bytes) {
    MPI_Status status;
    MPI_Recv(buffer, bytes, MPI_BYTE, 0, 0, MPI_COMM_WORLD, &status);
}

// A top level barrier function, for both slaves and master.
// Returns the sum of uint64 values.
uint64 Roomy_sumBarrier(uint64 in) {
    // STATS
    Timer t;
    Timer_start(&t);

    if(RGLO_MPI_RANK) {
        Roomy_slaveSendBarrier(&in, sizeof(uint64));
        uint64 ans;
        Roomy_slaveWaitForBarrier(&ans, sizeof(uint64));
        // STATS
        Timer_stop(&t);
        RSTAT_BARRIER_TIME += Timer_elapsed_usec(&t);

        return ans;
    } else {
        uint64 buf[RGLO_NUM_SLAVES-1]; // the master is also a slave
        Roomy_masterRecvBarrier(buf, sizeof(uint64));
        uint64 sum = in;
        int i;
        for(i=0; i<RGLO_NUM_SLAVES-1; i++) sum += buf[i];
        Roomy_masterNotifySlaves(&sum, sizeof(uint64));
        // STATS
        Timer_stop(&t);
        RSTAT_BARRIER_TIME += Timer_elapsed_usec(&t);

        return sum;
    }
}

// A top level barrier function, for both slaves and master.
// Takes a uint64 from each node and returns an array of uint64 values, where
// the array value at index i is the uint64 value from node i.
// Memory for the output argument should be pre-allocated by the caller.
typedef struct {
    uint64 rank;
    uint64 val;
} RankVal;
void Roomy_collectBarrier(uint64 val, uint64* out) {
    // STATS
    Timer t;
    Timer_start(&t);

    if(RGLO_MPI_RANK) {
        RankVal myRV;
        myRV.rank = RGLO_MPI_RANK;
        myRV.val = val;
        Roomy_slaveSendBarrier(&myRV, sizeof(RankVal));
        Roomy_slaveWaitForBarrier(out, sizeof(uint64)*RGLO_NUM_SLAVES);
    } else {
        RankVal buf[RGLO_NUM_SLAVES-1]; // the master is also a slave
        Roomy_masterRecvBarrier(buf, sizeof(RankVal));
        out[RGLO_MPI_RANK] = val;
        int i;
        for(i=0; i<RGLO_NUM_SLAVES-1; i++)
            out[buf[i].rank] = buf[i].val;
        Roomy_masterNotifySlaves(out, sizeof(uint64)*RGLO_NUM_SLAVES);
    }

    // STATS
    Timer_stop(&t);
    RSTAT_BARRIER_TIME += Timer_elapsed_usec(&t);
}

// A simple barrier, just to syncronize all nodes.
void Roomy_barrier() {
    #ifdef DEBUG
    Roomy_logAny("Enter barrier.\n");
    #endif 

    Roomy_sumBarrier(1);

    #ifdef DEBUG
    Roomy_logAny("Exit barrier.\n");
    #endif 
}

/******************************************************************************
 *                               STATISTICS
 *****************************************************************************/

Timer  RSTAT_INIT_TIMER;       // timer started in Roomy_init()
uint64 RSTAT_NUM_SYNCS;        // total number of syncs across all data structures
uint64 RSTAT_BARRIER_TIME;     // time spent in Roomy_barrier
uint64 RSTAT_WAIT_WRITE_TIME;  // time spent in Roomy_waitForRemoteWrite

// Initialize statistics
void Roomy_initStats() {
    RSTAT_BARRIER_TIME = 0;
    RSTAT_WAIT_WRITE_TIME = 0;
    RSTAT_NUM_SYNCS = 0;
}

// Print performance statistics to stdout
void Roomy_printStats() {
    char strbuf[RGLO_STR_SIZE];
    uint64 vals[RGLO_NUM_SLAVES];

    Roomy_print("\n\n");
    Roomy_print("Roomy Statistics\n");
    Roomy_print("--------------------------------\n\n");

    Timer_print(&RSTAT_INIT_TIMER, strbuf, RGLO_STR_SIZE);
    Roomy_print("Total wall clock run time: %s\n\n", strbuf);

    Roomy_print("Total number of syncs: %lli\n\n", RSTAT_NUM_SYNCS);

    Roomy_collectBarrier(RSTAT_BARRIER_TIME, vals);
    if (!RGLO_MY_RANK) {
        Roomy_print("Barrier time for each process:\n");
        int i;
        for (i = 0; i < RGLO_NUM_SLAVES; i++) {
            Timer_printUsec(vals[i], strbuf, RGLO_STR_SIZE);
            Roomy_print("  rank %i: %s\n", i, strbuf);
        }
        Roomy_print("\n");
    }

    Roomy_collectBarrier(RSTAT_WAIT_WRITE_TIME, vals);
    if (!RGLO_MY_RANK) {
        Roomy_print("Remote write wait time for each process:\n");
        int i;
        for (i = 0; i < RGLO_NUM_SLAVES; i++) {
            Timer_printUsec(vals[i], strbuf, RGLO_STR_SIZE);
            Roomy_print("  rank %i: %s\n", i, strbuf);
        }
        Roomy_print("\n");
    }
}
