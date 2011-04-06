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
 * pancakeList.c
 *
 * A RoomyList-based breadth-first search of the pancake sorting problem.
 *****************************************************************************/

#include "types.h"
#include "Timer.h"
#include "roomy.h"
#include "RoomyList.h"

#include <stdio.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <string.h>

// The length of the permutation
#define PERM_LEN 11

// The number of states in the search (i.e., factorial(PERM_LEN))
#define N_STATES 39916800

/******************************************************************************
 *                              Permutations
 *****************************************************************************/

typedef uint8 Elt;
typedef Elt Perm[PERM_LEN];

// Return the identity permutation in the argument out.
void getIdentPerm(Perm out) {
    int i;
    for(i = 0; i < PERM_LEN; i++)
        out[i] = i;
}

// Swap elements i and j of permutation p.
void swap(Perm p, Elt i, Elt j) {
    Elt tmp = p[i];
    p[i] = p[j];
    p[j] = tmp;
}

// Copy permutation from to permutation to.
void copyPerm(Perm to, Perm from) {
    memcpy(to, from, PERM_LEN * sizeof(Elt));
}

// Reverse the first k elements of the given permutation.
void revPrefix(Perm in, int k, Perm out) {
    copyPerm(out, in);
    int i;
    for(i = 0; i < k/2; i++) {
        swap(out, k-i-1, i);
    }
}

/******************************************************************************
 *                    Generation of Neighboring States 
 *****************************************************************************/

// Generate all permutations that can be reached in one prefix reversal 
// from the given permutation.
void genAllPerm(Perm p, Perm nbrs[]) {
    int i;
    for(i = 2; i <= PERM_LEN; i++) {
        revPrefix(p, i, nbrs[i-2]);
    }
}


/******************************************************************************
 *                        Roomy Implementation
 *                    Using RoomyList data structure.
 *****************************************************************************/

RoomyList* allLevList;   // Contains all permutations discoverd so far.
RoomyList* curLevList;   // Contains permutations at the 'current' level.
RoomyList* nextLevList;  // Contains permutations at the 'next' level.

// To be mapped over the current level to produce the next level.
void genNextLevFromList(void* val) {
    Perm nbrs[PERM_LEN-1];
    genAllPerm(val, nbrs);
    int i;
    for(i=0; i<PERM_LEN-1; i++) {
        RoomyList_add(nextLevList, &(nbrs[i]));
    }
}

// Top level function for breadth-first search of pancake graph.
void pancakeListBFS() {
    Roomy_log("BEGINNING %i PANCAKE BFS\n", PERM_LEN);

    // Create lists.
    allLevList = RoomyList_make("pancakeAllLevList", sizeof(Perm));
    curLevList = RoomyList_make("pancakeLev0", sizeof(Perm));
    nextLevList = RoomyList_make("pancakeLev1", sizeof(Perm));

    Roomy_log("Initial RoomyLists constructed\n");

    // Set identity element.
    Perm ident;
    getIdentPerm(ident);
    RoomyList_add(allLevList, &ident);
    RoomyList_sync(allLevList);
    RoomyList_add(curLevList, &ident);
    RoomyList_sync(curLevList);

    int curLevel = 0;         // The current level number.
    uint64 numThisLevel = 1;  // Number of elements at current level.

    Roomy_log("Level %i done: %lli elements\n", curLevel, numThisLevel);

    // Continue generating new levels until the current level is empty.
    while(numThisLevel) {
        // Generate next level from current.
        RoomyList_map(curLevList, genNextLevFromList);
        RoomyList_sync(nextLevList);

        // Detect duplicates.
        RoomyList_removeDupes(nextLevList);
        RoomyList_removeAll(nextLevList, allLevList);
        RoomyList_sync(nextLevList);

        // Record new non-duplicate elements.
        RoomyList_addAll(allLevList, nextLevList);
        RoomyList_sync(allLevList);

        // Create new next level, delete current level, rotate pointers.
        curLevel++;
        RoomyList_destroy(curLevList);
        curLevList = nextLevList;
        char levName[1024];
        sprintf(levName, "pancakeLev%i", curLevel+1);
        nextLevList = RoomyList_make(levName, sizeof(Perm));

        // Get the number of new states at current level.
        numThisLevel = RoomyList_size(curLevList);

        Roomy_log("Level %i done: %lli elements\n", curLevel, numThisLevel);
    }

    RoomyList_destroy(allLevList);
    RoomyList_destroy(curLevList);
    RoomyList_destroy(nextLevList);

    Roomy_log("PANCAKE BFS DONE\n");
}

/******************************************************************************
 *                                  Main 
 *****************************************************************************/

int main(int argc, char **argv) {
    Roomy_init(&argc, &argv);
    pancakeListBFS();
    Roomy_finalize();
    return 0;
}
