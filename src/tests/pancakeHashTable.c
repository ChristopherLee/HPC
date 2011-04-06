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
 * pancake.c
 *
 * A Roomy-based breadth-first search of the pancake sorting problem.
 *****************************************************************************/

#include "types.h"
#include "Timer.h"
#include "roomy.h"
#include "RoomyArray.h"
#include "RoomyList.h"

#include <stdio.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <string.h>

// The length of the permutation
#define PERM_LEN 11

// The number of states in the search (i.e., !PERM_LEN)
#define N_STATES 39916800

/******************************************************************************
 *                              Permutations
 *****************************************************************************/

typedef uint8 Elt;
typedef Elt Perm[PERM_LEN];

void getIdentPerm(Perm out) {
    int i;
    for(i=0; i<PERM_LEN; i++)
        out[i] = i;
}

void swap(Perm p, Elt i, Elt j) {
    Elt tmp = p[i];
    p[i] = p[j];
    p[j] = tmp;
}

void copyPerm(Perm to, Perm from) {
    memcpy(to, from, PERM_LEN * sizeof(Elt));
}

// Reverse the first k elements of the given permutation
void revPrefix(Perm in, int k, Perm out) {
    copyPerm(out, in);
    int i; for(i=0; i<k/2; i++) swap(out, k-i-1, i);
}

/******************************************************************************
 *                       Permutation (un)ranking, from:
 *  Ranking and unranking permutations in linear time (2001)
 *  by Wendy Myrvold, Frank Ruskey 
 *****************************************************************************/

void invertPerm(Perm p, Perm out) {
    int i;
    for(i=0; i<PERM_LEN; i++)
        out[p[i]] = i;
}

uint64 rankRecur(Elt n, Perm p, Perm r) {
    if(n == 1) return 0;
    Elt s = p[n-1];
    swap(p, n-1, r[n-1]);
    swap(r, s, n-1);
    return s + n * rankRecur(n-1, p, r);
}
uint64 rank(Perm p) {
    Perm p2;
    copyPerm(p2, p);
    Perm r;
    invertPerm(p2, r);
    return rankRecur(PERM_LEN, p2, r);
}

void unrankRecur(Elt n, uint64 r, Perm p) {
    if(n > 0) {
        swap(p, n-1, r%n);
        unrankRecur(n-1, r/n, p);
    }
}
void unrank(uint64 r, Perm out) {
    getIdentPerm(out);
    unrankRecur(PERM_LEN, r, out);
}

/******************************************************************************
 *                    Generation of Neighboring States 
 *****************************************************************************/

// Generate all states that can be reached in one flip from the given ranked state
void genAll(uint64 state, uint64 nbrs[]) {
    Perm p;
    unrank(state, p);
    int i;
    for(i=2; i<=PERM_LEN; i++) {
        Perm rev;
        revPrefix(p, i, rev);
        nbrs[i-2] = rank(rev);
    }
}

/******************************************************************************
 *                        Roomy Implementation
 *                        Using RoomyHashTables
 *
 * This method uses all of the methods provided by the RoomyHashTable. It is
 * designed to exercise all functionallity, not for performance.
 *****************************************************************************/

// Three hash tables: all elts seen, cur level, next level
RoomyHashTable* allRHT;
RoomyHashTable* curRHT;
RoomyHashTable* nextRHT;

// The current level
uint8 CUR_LEVEL;

// Function to be mapped over curRHT to generate next level in nextRHT.
void generateNextLevel(void* key, void* value) {
    uint64* state = (uint64*)key;
    uint8* level = (uint8*)value;
    if(*level == CUR_LEVEL) {
        uint64 nbrs[PERM_LEN-1];
        genAll(*state, nbrs);
        uint8 nextLevel = CUR_LEVEL + 1;
        int i;
        for(i=0; i<PERM_LEN-1; i++) {
            RoomyHashTable_insert(nextRHT, &(nbrs[i]), &nextLevel);
        }
    }
}

// To be mapped over allRHT to remove all seen elements from nextRHT.
void removeDupes(void* key, void* value) {
    RoomyHashTable_remove(nextRHT, key);
}

// To be mapped over nextRHT to add all new elements to allRHT.
void recordNewElts(void* key, void* value) {
    RoomyHashTable_insert(allRHT, key, value);
}

// Reduction functions that count the number of elements.
void addOneToAns(void* ansInOut, void* key, void* value) {
    uint64* count = (uint64*)ansInOut;
    *count = *count + 1;
}
void sumAns(void* ansInOut, void* ansIn) {
    uint64* count = (uint64*)ansInOut;
    *count = *count + *(uint64*)ansIn;
}

// Top level function for method using one RoomyHashTable
void pancakeHashTable() {
    Roomy_log("BEGINNING %i PANCAKE BFS: RoomyHashTable version\n", PERM_LEN);

    // Set element counters (assuming level 0 has been completed).
    uint64 levelSize = 1;  // number of elements in curRHT
    uint64 totalElts = 1;  // total number of elements see so far

    // Create hash tables
    CUR_LEVEL = 0;
    allRHT = RoomyHashTable_make("allRHT",
                                  sizeof(uint64),
                                  sizeof(uint8),
                                  N_STATES);
    curRHT = RoomyHashTable_make("lev0RHT",
                                  sizeof(uint64),
                                  sizeof(uint8),
                                  levelSize);
    nextRHT = RoomyHashTable_make("lev1RHT",
                                   sizeof(uint64),
                                   sizeof(uint8),
                                   levelSize * PERM_LEN);

    // Set identity element
    Perm ident;
    getIdentPerm(ident);
    uint64 identPos = rank(ident);
    RoomyHashTable_insert(allRHT, &identPos, &CUR_LEVEL);
    RoomyHashTable_sync(allRHT);
    RoomyHashTable_insert(curRHT, &identPos, &CUR_LEVEL);
    RoomyHashTable_sync(curRHT);

    Roomy_log("Level %i done: %lli elements\n", CUR_LEVEL, levelSize);

    // While current level is not empty
    while (levelSize > 0) {
        // map over cur level, add neighbors to next level
        RoomyHashTable_map(curRHT, generateNextLevel);
        RoomyHashTable_sync(nextRHT);

        // remove all elts in seen from next (by mapping over seen and calling
        // remove)
        RoomyHashTable_map(allRHT, removeDupes);
        RoomyHashTable_sync(nextRHT);

        // add all elts in next to seen (by mapping over next and calling update)
        RoomyHashTable_map(nextRHT, recordNewElts);
        RoomyHashTable_sync(allRHT);

        // rotate levels
        RoomyHashTable_destroy(curRHT);
        curRHT = nextRHT;
        levelSize = RoomyHashTable_size(curRHT);
        CUR_LEVEL++;
        char levName[1024];
        sprintf(levName, "lev%iRHT", CUR_LEVEL + 1);

        // New table size is 30% bigger than the next level, to avoid doubling
        // of hash table capacity. But, it doesn't need to be bigger than the
        // entire search space. (PERM_LEN-1 is the branching factor.)
        uint64 tableSize = levelSize * (PERM_LEN-1) * 13 / 10;
        if (tableSize > N_STATES) {
            tableSize = N_STATES;
        }

        nextRHT = RoomyHashTable_make(levName,
                                      sizeof(uint64),
                                      sizeof(uint8),
                                      tableSize);

        // Confirm size reported by reduction matches level size.
        uint64 numEltsFromReduce = 0;
        RoomyHashTable_reduce(curRHT, &numEltsFromReduce, sizeof(uint64),
                              addOneToAns, sumAns);
        assert(numEltsFromReduce == levelSize);

        totalElts = RoomyHashTable_size(allRHT);

        Roomy_log("Level %i done: %lli elements\n", CUR_LEVEL, levelSize);
    }

    Roomy_log("PANCAKE BFS DONE\n");
}

/******************************************************************************
 *                                  Main 
 *****************************************************************************/

int main(int argc, char **argv) {
    Roomy_init(&argc, &argv);
    pancakeHashTable();
    Roomy_finalize();
    return 0;
}
