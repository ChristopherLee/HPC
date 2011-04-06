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
 * A RoomyArray-based breadth-first search of the pancake sorting problem.
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

// Generate all states that can be reached in one reversal from the given
// ranked state.
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
 * Using a single one-bit RoomyArray data structure and cascading updates.
 *****************************************************************************/

RoomyArray* dupe;

// Update function: if the element hasn't been seen yet, sets the bit and generates
// neighboring states.
void generate(uint64 state, void* oldVal, void* updateVal, void* newVal) {
    uint8 seen = *(uint8*)oldVal;
    if(!seen) {
        uint64 nbrs[PERM_LEN-1];
        genAll(state, nbrs);
        int i;
        for(i=0; i<PERM_LEN-1; i++) {
            RoomyArray_update(dupe, nbrs[i], NULL, generate); // don't need to pass
                                                              // update val
        }
    }
    *(uint8*)newVal = 1;
}

// A predicate counting the number of non-zero elements.
uint8 isDupe(uint64 i, void* value) {
    return *(uint8*)value == 1;
}

// Top level function for method using three 1-bit arrays
void pancakeOneBitBFS() {
    Roomy_log("BEGINNING %i PANCAKE BFS\n", PERM_LEN);

    // Initialize bit array
    dupe = RoomyArray_makeBits("pancakeDupes", 1, N_STATES);
    RoomyArray_registerUpdateFunc(dupe, generate, 0);
    RoomyArray_attachPredicate(dupe, isDupe);    

    // set identity element
    Perm ident;
    getIdentPerm(ident);
    uint64 identPos = rank(ident);
    RoomyArray_update(dupe, identPos, NULL, generate);

    // generate levels
    int curLevel = 0;
    uint64 lastTotal = 0, newTotal = 0, numThisLevel = 1;

    while(!RoomyArray_isSynced(dupe)) {
        // process existing updates, generate updates for next level
        RoomyArray_sync(dupe);
        newTotal = RoomyArray_predicateCount(dupe, isDupe);
        numThisLevel = newTotal - lastTotal;
        lastTotal = newTotal;

        Roomy_log("Level %i done: %lli elements\n", curLevel, numThisLevel);

        // next level
        curLevel++;
    }

    RoomyArray_destroy(dupe);

    Roomy_log("ONE-BIT PANCAKE BFS DONE\n");
}

/******************************************************************************
 *                                  Main 
 *****************************************************************************/

int main(int argc, char **argv) {
    Roomy_init(&argc, &argv);
    pancakeOneBitBFS();
    Roomy_finalize();
    return 0;
}
