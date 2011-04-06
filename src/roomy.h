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
 * roomy.h
 *
 * Roomy: a C library for using high-latency storage as main memory.
 *****************************************************************************/

#ifndef _ROOMY_H
#define _ROOMY_H

#define RGLO_STR_SIZE 1024
#define RGLO_MAX_UPDATE_FUNC 1024

#include "HashTable.h"
#include "RoomyArray.h"
#include "RoomyList.h"
#include "RoomyHashTable.h"
#include "Timer.h"

extern int RGLO_MY_RANK;
extern int RGLO_MPI_RANK;
extern int RGLO_NUM_SLAVES;
extern int RGLO_IN_PARALLEL;
extern int RGLO_USE_MMAP;
extern uint64 RGLO_MAX_CHUNK_SIZE;
extern uint64 RGLO_MAX_SUBTABLE_SIZE;
extern uint64 RGLO_BUFFER_SIZE;
extern uint64 RGLO_LARGE_BUFFER_SIZE;
extern uint64 RGLO_MEM_GAP;
extern uint64 RGLO_RAM_MAX;
extern int64 RGLO_RAM_LEFT;
extern uint64 RGLO_DISK_MAX;
extern int64 RGLO_DISK_LEFT;
extern char RGLO_DATA_PATH[RGLO_STR_SIZE];
extern char RGLO_PERMUTE_DATA_PATH[RGLO_STR_SIZE];
extern uint64 RGLO_NEXT_UNIQUE_NUM;

extern int RGLO_MAX_STRUCTS;
extern HashTable RGLO_RA_LIST;
extern HashTable RGLO_RL_LIST;
extern HashTable RGLO_RHT_LIST;

// Public API

void Roomy_log(char* message, ...);
void Roomy_logAny(char* message, ...);
void Roomy_init(int* argc, char*** argv);
void Roomy_finalize();
void Roomy_sync();
uint64 Roomy_uniqueInt();
void Roomy_resetUniqueInt(uint64 min);

void Roomy_printStats(); // not yet implemented

// Private methods

void Roomy_registerRoomyArray(RoomyArray* ra);
void Roomy_registerRoomyList(RoomyList* rl);
void Roomy_registerRoomyHashTable(RoomyHashTable* rht);
void Roomy_unregisterRoomyArray(RoomyArray* ra);
void Roomy_unregisterRoomyList(RoomyList* rl);
void Roomy_unregisterRoomyHashTable(RoomyHashTable* rht);
void makeRoomyDir();
uint64 Roomy_destroyDir(char* dir);
void Roomy_makeDir(char* dir);
void Roomy_bufferedData(uint64 size);
void Roomy_freeSpace();
void Roomy_ramAllocated(uint64 size);
void Roomy_ramFreed(uint64 size);
WriteBuffer Roomy_makeWriteBuffer(uint64 bufferSize,
                                  int eltSize,
                                  char* filename,
                                  int node);
void Roomy_slaveWaitForBarrier(void* buffer, int bytes);
void Roomy_slaveSendBarrier(void* buffer, int bytes);
void Roomy_masterNotifySlaves(void* buffer, int bytes);
void Roomy_masterRecvBarrier(void* buffer, int bytesPerSlave);
uint64 Roomy_sumBarrier(uint64 in);
void Roomy_collectBarrier(uint64 val, uint64* out);
void Roomy_barrier();
void Roomy_dataRemoved(uint64 size);
void Roomy_waitForRemoteWrites();

// Statistics

extern Timer  RSTAT_INIT_TIMER;
extern uint64 RSTAT_BARRIER_TIME;
extern uint64 RSTAT_WAIT_WRITE_TIME;
extern uint64 RSTAT_NUM_SYNCS;

void Roomy_initStats();
void Roomy_printStats();

#endif
