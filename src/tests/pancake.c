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
 * Several different implementations are given, using different features of
 * the RoomyArray, RoomyHashTable, and RoomyList data structures.
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
//#define PERM_LEN 12
//#define PERM_LEN 13

// The number of states in the search (i.e., !PERM_LEN)
#define N_STATES 39916800
//#define N_STATES 479001600
//#define N_STATES 6227020800

// Print tracing information
#define TRACING

// Check that each level of the search has the correct number of states
// (only works for PERM_LEN == 11)
#define CHECK_ANSWER

// The correct number of elements for each BFS level for PERM_LEN == 11
uint64 LEVEL_SIZES[15] = {1, 10, 90, 809, 6429, 43891, 252737, 1174766, 4126515,
                          9981073, 14250471, 9123648, 956354, 6, 0};

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

void printPerm(Perm p) {
    int i;
    printf("[");
    for(i=0; i<PERM_LEN-1; i++)
        printf("%i ", p[i]);
    printf("%i]", p[PERM_LEN-1]);
}

void printPermToStr(char* str, Perm p) {
    int i;
    sprintf(str, "[");
    char strTmp[1024];
    for(i=0; i<PERM_LEN-1; i++) {
        sprintf(strTmp, "%i ", p[i]);
        strcat(str, strTmp);
    }
    sprintf(strTmp, "%i]", p[PERM_LEN-1]);
    strcat(str, strTmp);
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

// Like genAll, but takes and returns Perms instead of a ranks
void genAllPerm(Perm p, Perm nbrs[]) {
    int i;
    for(i=2; i<=PERM_LEN; i++) {
        revPrefix(p, i, nbrs[i-2]);
    }
}

/******************************************************************************
 *                        Roomy Implementation 1
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

// a predicate counting the number of zero elements
uint8 isDupe(uint64 i, void* value) {
    return *(uint8*)value == 1;
}

// Top level function for method using three 1-bit arrays
void pancakeOneBitBFS() {
    #ifdef TRACING
    Roomy_log("BEGINNING %i PANCAKE BFS\n", PERM_LEN);
    Timer timer;
    Timer_start(&timer);
    #endif

    // Initialize bit array
    dupe = RoomyArray_makeBits("pancakeDupes", 1, N_STATES);
    RoomyArray_registerUpdateFunc(dupe, generate, 0);
    RoomyArray_attachPredicate(dupe, isDupe);    

    #ifdef TRACING
    Roomy_log("Initial one-bit RoomyArray constructed\n");
    #endif

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

        #ifdef TRACING
        Roomy_log("Level %i done: %lli elements\n", curLevel, numThisLevel);
        #endif

        #ifdef CHECK_ANSWER
        assert(LEVEL_SIZES[curLevel] == numThisLevel);
        #endif

        // next level
        curLevel++;
    }

    RoomyArray_destroy(dupe);

    #ifdef TRACING
    Roomy_log("ONE-BIT PANCAKE BFS DONE\n");
    Timer_stop(&timer);
    char timeString[1024];
    Timer_print(&timer, timeString, 1024);
    Roomy_log("Elapsed time: %s\n", timeString);
    #endif
}

/******************************************************************************
 *                        Roomy Implementation 2
 *          Using one RoomyArray data structure (two bits per elt)
 *
 * The four possible values mark each state as: not seen (open); previously
 * seen; at the current level; or at the next level.
 *****************************************************************************/

RoomyArray* states;

uint8 OPEN = 0;
uint8 DUPE = 1;
uint8 CUR = 2;
uint8 NEXT = 3;

// to update a state that could be at the next level
void updateState(uint64 state, void* oldVal, void* updateVal, void* newVal) {
    uint8* old = (uint8*)oldVal;
    uint8* update = (uint8*)updateVal;
    uint8* new = (uint8*)newVal;
    if(*old == OPEN) {
        *new = *update;
    } else {
        *new = *old;
    }
}

// to be mapped over the state array to generate new states
void generateStates(uint64 state, void* val) {
    if(*(uint8*)val == CUR) {
        uint64 nbrs[PERM_LEN-1];
        genAll(state, nbrs);
        int i;
        for(i=0; i<PERM_LEN-1; i++) {
            RoomyArray_update(states, nbrs[i], &NEXT, updateState);
        }
    }
}

// to be mapped (with modification) over the state array.
// rotates the levels, so that the CUR level is now DUPE, and the NEXT level
// is now CUR.
void rotate(uint64 state, void* oldVal, void* newVal) {
    uint8* old = (uint8*)oldVal;
    uint8* new = (uint8*)newVal;
    if(*old == CUR) *new = DUPE;
    else if(*old == NEXT) *new = CUR;
    else *new = *old;
}

// to be used as a reduction on the state array, to count the number of
// new states at the next level.
// first function:  combines an intermediate answer and an array value
// second function: combines two intermediate answers
void countNew_AnsVal(void* ansInOut, uint64 state, void* val) {
    if(*(uint8*)val == NEXT) {
        uint64* count = (uint64*)ansInOut;
        *count = *count + 1;
    }
}
void countNew_AnsAns(void* ansInOut, void* ansIn) {
    uint64* count1 = (uint64*)ansInOut;
    uint64* count2 = (uint64*)ansIn;
    *count1 = *count1 + *count2;
}

// an alternate method for counting the number of new elements is attaching
// a predicate, that will be kept accurate with all updates. this is
// more efficient than using a reduction, which requires a new scan of
// the data structure.
uint8 isNew(uint64 i, void* val) {
    return *(uint8*)val == NEXT;
}

// Top level function for method using one 2-bit array
void pancakeTwoBitBFS() {
    #ifdef TRACING
    Roomy_log("BEGINNING %i PANCAKE BFS\n", PERM_LEN);
    Timer timer;
    Timer_start(&timer);
    #endif

    // Initialize bit array
    states = RoomyArray_makeBits("pancakeStates", 2, N_STATES);

    #ifdef TRACING
    Roomy_log("Initial 2-bit RoomyArray constructed\n");
    #endif

    // register delayed update function
    RoomyArray_registerUpdateFunc(states, updateState, 1);

    // attach the predicate to count the number of elements at the next level
    RoomyArray_attachPredicate(states, isNew);

    // set identity element
    Perm ident;
    getIdentPerm(ident);
    uint64 identPos = rank(ident);
    RoomyArray_update(states, identPos, &CUR, updateState);
    RoomyArray_sync(states);

    // generate levels
    int curLevel = 0;
    uint64 numThisLevel = 1;

    #ifdef TRACING
    Roomy_log("Level %i done: %lli elements\n", curLevel, numThisLevel);
    #endif

    #ifdef CHECK_ANSWER
    assert(LEVEL_SIZES[curLevel] == numThisLevel);
    #endif

    while(numThisLevel) {
        // generate next level from current
        RoomyArray_map(states, generateStates);
        RoomyArray_sync(states);

        // count number at next level
        uint64 numNextLevel = 0;
        RoomyArray_reduce(states, &numNextLevel, sizeof(uint64),
                          countNew_AnsVal, countNew_AnsAns);

        // also count using the attached predicate, check that they match
        uint64 numFromPred = RoomyArray_predicateCount(states, isNew);
        assert(numNextLevel == numFromPred);

        // stopping condition
        if(!numNextLevel) break;

        // rotate levels
        RoomyArray_mapAndModify(states, rotate);
        curLevel++;
        numThisLevel = numNextLevel;

        #ifdef TRACING
        Roomy_log("Level %i done: %lli elements\n", curLevel, numThisLevel);
        #endif

        #ifdef CHECK_ANSWER
        assert(LEVEL_SIZES[curLevel] == numThisLevel);
        #endif
    }

    RoomyArray_destroy(states);

    #ifdef TRACING
    Roomy_log("PANCAKE BFS DONE\n");
    Timer_stop(&timer);
    char timeString[1024];
    Timer_print(&timer, timeString, 1024);
    Roomy_log("Elapsed time: %s\n", timeString);
    #endif
}

/******************************************************************************
 *                        Roomy Implementation 3
 *                  Using one RoomyArray data structure.
 *
 * The array uses one byte to store the level at which each element was first
 * discovered. This information is used to print all elements at the last level.
 * Note that in this implementation, level 1 is the identity element (so zero
 * can be used to represent "not seen"). So, subtract one from the value in the
 * array to get the true level.
 *****************************************************************************/

RoomyArray* stateLevs;

uint8 CUR_LEVEL;

#define NOT_SEEN 0

// to update a state that could be at the next level
void updateStateLev(uint64 state, void* oldVal, void* updateVal, void* newValOut) {
    uint8* oldLev = (uint8*)oldVal;
    uint8* updateLev = (uint8*)updateVal;
    uint8* newLev = (uint8*)newValOut;
    if(*oldLev == NOT_SEEN) *newLev = *updateLev;
    else *newLev = *oldLev;
}
// another version that tests using update values that are a different size than
// array elements
void updateStateLev2Bytes(uint64 state, void* oldVal, void* updateVal,
                          void* newValOut) {
    uint8* oldLev = (uint8*)oldVal;
    uint16* updateLev = (uint16*)updateVal;
    uint8* newLev = (uint8*)newValOut;
    if(*oldLev == NOT_SEEN) *newLev = *updateLev;
    else *newLev = *oldLev;
}

// to be mapped over the state array to generate new states
void generateStatesCurLev(uint64 state, void* val) {
    uint8* lev = (uint8*)val;
    if(*lev == CUR_LEVEL) {
        uint64 nbrs[PERM_LEN-1];
        genAll(state, nbrs);
        uint16 updateVal = CUR_LEVEL + 1;
        int i;
        for(i=0; i<PERM_LEN-1; i++) {
            RoomyArray_update(stateLevs, nbrs[i], &updateVal, updateStateLev);
        }
    }
}

// to be used as a reduction on the state array, to count the number of
// states at the current level.
// first function:  combines an intermediate answer and an array value
// second function: combines two intermediate answers
void countCurLev_AnsVal(void* ansInOut, uint64 state, void* val) {
    uint8* lev = (uint8*)val;
    if(*lev == CUR_LEVEL) {
        uint64* count = (uint64*)ansInOut;
        *count = *count + 1;
    }
}
void countCurLev_AnsAns(void* ansInOut, void* ansIn) {
    uint64* count1 = (uint64*)ansInOut;
    uint64* count2 = (uint64*)ansIn;
    *count1 = *count1 + *count2;
}

// print all elements at CUR_LEVEL (by mapping over state array)
void printCurLevel(uint64 state, void* val) {
    uint8* lev = (uint8*)val;
    if(*lev == CUR_LEVEL) {
        Perm p;
        unrank(state, p);
        char str[1024];
        printPermToStr(str, p);
        Roomy_log("  %s\n", str);
    }
}

// Top level function for method using one 1-byte array
void pancakeOneByteBFS() {
    #ifdef TRACING
    Roomy_log("BEGINNING %i PANCAKE BFS\n", PERM_LEN);
    Timer timer;
    Timer_start(&timer);
    #endif

    // Initialize bit array
    stateLevs = RoomyArray_makeBytes("pancakeStates", sizeof(uint8), N_STATES);

    #ifdef TRACING
    Roomy_log("Initial RoomyArray constructed\n");
    #endif

    // register delayed update function
    RoomyArray_registerUpdateFunc(stateLevs, updateStateLev, sizeof(uint16));

    // set identity element
    Perm ident;
    getIdentPerm(ident);
    uint64 identPos = rank(ident);
    uint16 firstLev = 1;
    RoomyArray_update(stateLevs, identPos, &firstLev, updateStateLev);
    RoomyArray_sync(stateLevs);

    // generate levels
    CUR_LEVEL = 1;
    uint64 numThisLevel = 1;

    #ifdef TRACING
    Roomy_log("Level %i done: %lli elements\n", CUR_LEVEL-1, numThisLevel);
    #endif

    #ifdef CHECK_ANSWER
    assert(LEVEL_SIZES[CUR_LEVEL-1] == numThisLevel);
    #endif

    while(numThisLevel) {
        // generate next level from current
        RoomyArray_map(stateLevs, generateStatesCurLev);
        RoomyArray_sync(stateLevs);

        // count number at next level
        CUR_LEVEL++;
        uint64 numNextLevel = 0;
        RoomyArray_reduce(stateLevs, &numNextLevel, sizeof(uint64),
                          countCurLev_AnsVal, countCurLev_AnsAns);

        // stopping condition
        if(!numNextLevel) break;

        // next level
        numThisLevel = numNextLevel;

        #ifdef TRACING
        Roomy_log("Level %i done: %lli elements\n", CUR_LEVEL-1, numThisLevel);
        #endif

        #ifdef CHECK_ANSWER
        assert(LEVEL_SIZES[CUR_LEVEL-1] == numThisLevel);
        #endif
    }

    // print all cases at the last level
    CUR_LEVEL--;
    //Roomy_log("ALL ELEMENTS AT LAST LEVEL\n");
    //RoomyArray_map(stateLevs, printCurLevel);

    RoomyArray_destroy(stateLevs);
    
    #ifdef TRACING
    Roomy_log("PANCAKE BFS DONE\n");
    Timer_stop(&timer);
    char timeString[1024];
    Timer_print(&timer, timeString, 1024);
    Roomy_log("Elapsed time: %s\n", timeString);
    #endif
}

/******************************************************************************
 *                        Roomy Implementation 4
 *                    Using RoomyList data structure.
 *
 * This value uses the explicit permutation representation of elements, and
 * so does not rely on a function mapping states to integers.
 *****************************************************************************/

RoomyList* allLevList;
RoomyList* curLevList;
RoomyList* nextLevList;

// Test use of keys in list elements by adding some random data to the end of
// the permutation, and using the permutation as the key.
typedef struct {
    Perm p;
    uint32 r;
} KeyValue;

// Print the given KeyValue.
void printKV(KeyValue kv) {
    printPerm(kv.p);
    printf(" r=%i\n", kv.r);
}

// Map over a RoomyList to print all KeyValue elements in that list.
void printList(void* val) {
    KeyValue kv;
    memcpy(&kv, val, sizeof(KeyValue));
    printKV(kv);
}

// To be mapped over the current level to produce the next level
void genNextLevFromList(void* val) {
    KeyValue kv;
    memcpy(&kv, val, sizeof(KeyValue));
    Perm nbrs[PERM_LEN-1];
    genAllPerm(kv.p, nbrs);
    int i;
    for(i=0; i<PERM_LEN-1; i++) {
        KeyValue nbr;
        memcpy(nbr.p, nbrs[i], sizeof(Perm));
        nbr.r = rand();
        RoomyList_add(nextLevList, &nbr);
    }
}

// Top level function for method using RoomyList data structure
void pancakeListBFS() {
    #ifdef TRACING
    Roomy_log("BEGINNING %i PANCAKE BFS\n", PERM_LEN);
    Timer timer;
    Timer_start(&timer);
    #endif
    
    // Init lists for duplicates, current level, and next level
    allLevList = RoomyList_makeWithKey("pancakeAllLevList", sizeof(KeyValue),
                                       0, sizeof(Perm));
    curLevList = RoomyList_makeWithKey("pancakeLev0", sizeof(KeyValue),
                                       0, sizeof(Perm));
    nextLevList = RoomyList_makeWithKey("pancakeLev1", sizeof(KeyValue),
                                       0, sizeof(Perm));

    #ifdef TRACING
    Roomy_log("Initial RoomyLists constructed\n");
    #endif

    // set identity element
    KeyValue ident;
    getIdentPerm(ident.p);
    RoomyList_add(allLevList, &ident);
    RoomyList_sync(allLevList);
    RoomyList_add(curLevList, &ident);
    RoomyList_sync(curLevList);

    // generate levels
    int curLevel = 0;
    uint64 numThisLevel = 1;

    #ifdef TRACING
    Roomy_log("Level %i done: %lli elements\n", curLevel, numThisLevel);
    #endif

    #ifdef CHECK_ANSWER
    assert(LEVEL_SIZES[curLevel] == numThisLevel);
    #endif

    while(numThisLevel) {
        // generate next level from current
        RoomyList_map(curLevList, genNextLevFromList);
        RoomyList_sync(nextLevList);

        // detect duplicates
        RoomyList_removeDupes(nextLevList);
        RoomyList_removeAll(nextLevList, allLevList);
        RoomyList_sync(nextLevList);
        RoomyList_addAll(allLevList, nextLevList);
        RoomyList_sync(allLevList);

        // if it's the last level, print elements
        if(RoomyList_size(nextLevList) == 0) {
            RoomyList_map(curLevList, printList);
        }

        // create new next level, delete current level, rotate pointers
        curLevel++;
        RoomyList_destroy(curLevList);
        curLevList = nextLevList;
        char levName[1024];
        sprintf(levName, "pancakeLev%i", curLevel+1);
        nextLevList = RoomyList_makeWithKey(levName, sizeof(KeyValue),
                                            0, sizeof(Perm));

        // get the number of new states at current level
        numThisLevel = RoomyList_size(curLevList);

        #ifdef TRACING
        Roomy_log("Level %i done: %lli elements\n", curLevel, numThisLevel);
        #endif

        #ifdef CHECK_ANSWER
        assert(LEVEL_SIZES[curLevel] == numThisLevel);
        #endif
    }

    RoomyList_destroy(allLevList);
    RoomyList_destroy(curLevList);
    RoomyList_destroy(nextLevList);

    #ifdef TRACING
    Roomy_log("PANCAKE BFS DONE\n");
    Timer_stop(&timer);
    char timeString[1024];
    Timer_print(&timer, timeString, 1024);
    Roomy_log("Elapsed time: %s\n", timeString);
    #endif 
}

/******************************************************************************
 *                        Roomy Implementation 5
 *        Using one RoomyArray data structure, and cascading updates.
 *
 * This implementation is similar to the other using a 1-byte RoomyArray, but
 * this version uses cascading updates instead of using map to generate each
 * level. So, when an element is updated, if it has not been seen before, it's
 * neighbors will be immediately generated. This method only works if all of the
 * updates for adjacent level fit on disk simultaneously.
 *****************************************************************************/

// to update a state that could be at the next level
void updateStateLevCascade(uint64 state, void* oldVal, void* updateVal,
                           void* newValOut) {
    uint8* oldLev = (uint8*)oldVal;
    uint8* updateLev = (uint8*)updateVal;
    uint8* newLev = (uint8*)newValOut;
    if(*oldLev == NOT_SEEN) {
        // set new level for this element
        *newLev = *updateLev;

        // generate neighbors
        uint64 nbrs[PERM_LEN-1];
        genAll(state, nbrs);
        uint8 nextLevel = *newLev + 1;
        int i;
        for(i=0; i<PERM_LEN-1; i++) {
            RoomyArray_update(
                stateLevs, nbrs[i], &nextLevel, updateStateLevCascade);
        }
    } else {
        *newLev = *oldLev;
    }
}

// a predicate counting the number of zero elements
uint8 isSeenElt(uint64 i, void* value) {
    return *(uint8*)value != NOT_SEEN;
}

// Top level function for method using one 1-byte array
void pancakeCascadingBFS() {
    #ifdef TRACING
    Roomy_log("BEGINNING %i PANCAKE BFS: cascading updates version\n", PERM_LEN);
    Timer timer;
    Timer_start(&timer);
    #endif

    // Initialize array
    stateLevs = RoomyArray_makeBytes("pancakeStates", sizeof(uint8), N_STATES);

    #ifdef TRACING
    Roomy_log("Initial RoomyArray constructed\n");
    #endif

    // register predicate counting the number of not-seen states
    RoomyArray_attachPredicate(stateLevs, isSeenElt);

    // register delayed update function
    RoomyArray_registerUpdateFunc(stateLevs, updateStateLevCascade, sizeof(uint8));

    // set identity element
    Perm ident;
    getIdentPerm(ident);
    uint64 identPos = rank(ident);
    uint8 firstLev = 1;
    RoomyArray_update(stateLevs, identPos, &firstLev, updateStateLevCascade);

    // generate levels
    CUR_LEVEL = 0;
    uint64 lastTotal = 0, newTotal = 0, numThisLevel = 0;

    while(!RoomyArray_isSynced(stateLevs)) {
        // process existing updates, generate updates for next level
        RoomyArray_sync(stateLevs);
        newTotal = RoomyArray_predicateCount(stateLevs, isSeenElt);
        numThisLevel = newTotal - lastTotal;
        lastTotal = newTotal;

        #ifdef TRACING
        Roomy_log("Level %i done: %lli elements\n", CUR_LEVEL, numThisLevel);
        #endif

        #ifdef CHECK_ANSWER
        assert(LEVEL_SIZES[CUR_LEVEL] == numThisLevel);
        #endif

        // next level
        CUR_LEVEL++;
    }
    RoomyArray_destroy(stateLevs);
    
    #ifdef TRACING
    Roomy_log("PANCAKE BFS DONE\n");
    Timer_stop(&timer);
    char timeString[1024];
    Timer_print(&timer, timeString, 1024);
    Roomy_log("Elapsed time: %s\n", timeString);
    #endif
}

/******************************************************************************
 *                        Roomy Implementation 6
 *                        Using RoomyHashTables
 *
 * This method uses all of the methods provided by the RoomyHashTable. It is
 * designed to exercise all functionallity, not performance.
 *****************************************************************************/

// Three hash tables: all elts seen, cur level, next level
RoomyHashTable* allRHT;
RoomyHashTable* curRHT;
RoomyHashTable* nextRHT;

// The current level
uint8 CUR_LEVEL_RHT;

// Function to be mapped over curRHT to generate next level in nextRHT.
void generateNextLevelRHT(void* key, void* value) {
    uint64* state = (uint64*)key;
    uint8* level = (uint8*)value;
    if(*level == CUR_LEVEL_RHT) {
        uint64 nbrs[PERM_LEN-1];
        genAll(*state, nbrs);
        uint8 nextLevel = CUR_LEVEL_RHT + 1;
        int i;
        for(i=0; i<PERM_LEN-1; i++) {
            RoomyHashTable_insert(nextRHT, &(nbrs[i]), &nextLevel);
        }
    }
}

// To be mapped over allRHT to remove all seen elements from nextRHT.
void removeDupesRHT(void* key, void* value) {
    RoomyHashTable_remove(nextRHT, key);
}

// To be mapped over nextRHT to add all new elements to allRHT.
void recordNewEltsRHT(void* key, void* value) {
    RoomyHashTable_insert(allRHT, key, value);
}

// TESTING: test that the passedValue equals the tableValue.
void verifyAccessRHT(void* key, void* tableValue, void* passedValue) {
    assert(memcmp(tableValue, passedValue, sizeof(uint8)) == 0);
}

// TESTING: map over allRHT and call access on same key in allRHT to verify that
// the same (key, value) pair is returned.
void testAccessRHT(void* key, void* value) {
    RoomyHashTable_access(allRHT, key, value, verifyAccessRHT);
}

// TESTING: a predicate that always returns true. Used to test that the number
// returned by the predicate and that returned by RoomyHashTable_size match.
uint8 truePredicateRHT(void* key, void* value) {
    return 1;
}

// TESTING: reduction functions that simply count the number of elements.
// Used to test that the values returned by RoomyHashTable_reduce matches that
// returned by RoomyHashTable_size.
void addOneToAnsRHT(void* ansInOut, void* key, void* value) {
    uint64* count = (uint64*)ansInOut;
    *count = *count + 1;
}
void sumAnsRHT(void* ansInOut, void* ansIn) {
    uint64* count = (uint64*)ansInOut;
    *count = *count + *(uint64*)ansIn;
}

// Top level function for method using one RoomyHashTable
void pancakeHashTable() {
    #ifdef TRACING
    Roomy_log("BEGINNING %i PANCAKE BFS: RoomyHashTable version\n", PERM_LEN);
    Timer timer;
    Timer_start(&timer);
    #endif

    // Set element counters (assuming level 0 has been completed).
    uint64 levelSize = 1;  // number of elements in curRHT
    uint64 totalElts = 1;  // total number of elements see so far

    // Create hash tables
    CUR_LEVEL_RHT = 0;
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

    #ifdef TRACING
    Roomy_log("Initial RoomyHashTables constructed\n");
    #endif

    // Register functions
    RoomyHashTable_registerAccessFunc(allRHT, verifyAccessRHT, sizeof(uint8));
    RoomyHashTable_attachPredicate(allRHT, truePredicateRHT);

    // Set identity element
    Perm ident;
    getIdentPerm(ident);
    uint64 identPos = rank(ident);
    RoomyHashTable_insert(allRHT, &identPos, &CUR_LEVEL_RHT);
    RoomyHashTable_sync(allRHT);
    RoomyHashTable_insert(curRHT, &identPos, &CUR_LEVEL_RHT);
    RoomyHashTable_sync(curRHT);

    #ifdef TRACING
    Roomy_log("Level %i done: %lli elements\n", CUR_LEVEL_RHT, levelSize);
    #endif

    // While current level is not empty
    while (levelSize > 0) {
        // map over cur level, add neighbors to next level
        RoomyHashTable_map(curRHT, generateNextLevelRHT);
        RoomyHashTable_sync(nextRHT);

        // remove all elts in seen from next (by mapping over seen and calling
        // remove)
        RoomyHashTable_map(allRHT, removeDupesRHT);
        RoomyHashTable_sync(nextRHT);

        // add all elts in next to seen (by mapping over next and calling update)
        RoomyHashTable_map(nextRHT, recordNewEltsRHT);
        RoomyHashTable_sync(allRHT);

        // rotate levels
        RoomyHashTable_destroy(curRHT);
        curRHT = nextRHT;
        levelSize = RoomyHashTable_size(curRHT);
        CUR_LEVEL_RHT++;
        char levName[1024];
        sprintf(levName, "lev%iRHT", CUR_LEVEL_RHT + 1);

        // New table size should be at least 30% bigger than the next level, to
        // avoid excessive hash table collisions. But, it doesn't need to be
        // bigger than the entire search space.
        // PERM_LEN-1 is the branching factor.
        uint64 tableSize = levelSize * (PERM_LEN-1) * 13 / 10;
        if (tableSize > N_STATES) {
            tableSize = N_STATES;
        }

        nextRHT = RoomyHashTable_make(levName,
                                      sizeof(uint64),
                                      sizeof(uint8),
                                      tableSize);

        // TESTING: make sure both pred. and reduce match size reported by seen
        uint64 predCount = RoomyHashTable_predicateCount(allRHT, truePredicateRHT);
        assert(predCount - totalElts == levelSize);
        uint64 numEltsFromReduce = 0;
        RoomyHashTable_reduce(curRHT, &numEltsFromReduce, sizeof(uint64),
                              addOneToAnsRHT, sumAnsRHT);
        assert(numEltsFromReduce == levelSize);

        totalElts = RoomyHashTable_size(allRHT);

        #ifdef TRACING
        Roomy_log("Level %i done: %lli elements\n", CUR_LEVEL_RHT, levelSize);
        #endif

        #ifdef CHECK_ANSWER
        assert(LEVEL_SIZES[CUR_LEVEL_RHT] == levelSize);
        #endif
    }

    // TESTING: map over seen when, call access on all keys, pass table value,
    // make sure tableValue passed to accessFunc matches passedValue
    RoomyHashTable_map(allRHT, testAccessRHT);
    RoomyHashTable_sync(allRHT);

    #ifdef TRACING
    Roomy_log("PANCAKE BFS DONE (RHT version)\n");
    Timer_stop(&timer);
    char timeString[1024];
    Timer_print(&timer, timeString, 1024);
    Roomy_log("Elapsed time: %s\n", timeString);
    #endif
}

/******************************************************************************
 *                                  Main 
 *****************************************************************************/

int main(int argc, char **argv) {
    #ifdef CHECK_ANSWER
    assert(PERM_LEN == 11);
    #endif

    Roomy_init(&argc, &argv);
    
    pancakeOneBitBFS();
    pancakeTwoBitBFS();
    pancakeOneByteBFS();
    pancakeCascadingBFS();
    pancakeListBFS();
    pancakeHashTable();
    
    Roomy_finalize();

    Roomy_log("PANCAKE TEST SUCCESS\n");

    return 0;
}
