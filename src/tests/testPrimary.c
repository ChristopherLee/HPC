#include "Array.h"
#include "BitArray.h"
#include "HashTable.h"
#include "ReadBuffer.h"
#include "WriteBuffer.h"
#include "types.h"
#include "params.h"
#include "knudirs.h"
#include "roomy.h"
#include "RoomyArray.h"
#include "RoomyList.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/utsname.h>

void updateAdd(uint64 i, void* oldVal, void* updateVal, void* newValOut) {
    uint64* o = (uint64*)oldVal;
    uint64* u = (uint64*)updateVal;
    uint64* n = (uint64*)newValOut;
    *n = *o + *u;
}

void mapPrint(uint64 i, void* val) {
    uint64* v = (uint64*)val;
    Roomy_logAny("Val at %lli is %lli\n", i, *v);
}

// compare two uint64 values for equality: the passed value and an element in
// a RoomyArray.
void assertEqualUint64(uint64 i, void* arrayVal, void* passedVal) {
    assert(memcmp(arrayVal, passedVal, sizeof(uint64)) == 0);
}

void testSmallRoomyArray() {
    // create
    int size = 2;
    RoomyArray* small = RoomyArray_makeBytes("smallTest", sizeof(uint64), size);

    // test initial zeros
    RoomyArray_registerAccessFunc(small, assertEqualUint64, sizeof(uint64));
    uint64 zero = 0;
    int i;
    for (i = 0; i < size; i++) {
        RoomyArray_access(small, i, &zero, assertEqualUint64);
    }

    RoomyArray_sync(small);

    // update values
    RoomyArray_registerUpdateFunc(small, updateAdd, sizeof(uint64));
    uint64 val1 = 42, val2 = 43;
    RoomyArray_update(small, 0, &val1, updateAdd);
    RoomyArray_update(small, 1, &val2, updateAdd);
    RoomyArray_sync(small);

    // check new values
    RoomyArray_access(small, 0, &val1, assertEqualUint64);
    RoomyArray_access(small, 1, &val2, assertEqualUint64);
    RoomyArray_sync(small);

    RoomyArray_destroy(small);

    Roomy_log("SUCCESS: testSmallRoomyArray\n");
}

void printEltArray(uint64 i, void* val) {
    uint64* v = (uint64*)val;
    printf("arr: %lli\n", *v);
}
void printEltList(void* val) {
    uint64* v = (uint64*)val;
    printf("list: %lli\n", *v);
}
void testListToArray() {
    // make a list with some arbitrary numbers, print them out as added
    Roomy_log("PRINTING NUMBERS\n");
    RoomyList* rl = RoomyList_make("testList", sizeof(uint64));
    uint64 numElts = 100;
    uint64 num = 3482;
    int i;
    for(i=0; i<numElts; i++) {
        if(RGLO_MY_RANK == 0) printf("pre: %lli\n", num);
        RoomyList_add(rl, &num);
        num = (num*num) % 196613;
    }
    RoomyList_sync(rl);

    // convert to array
    RoomyArray* ra = RoomyArray_makeFromList("testArray", rl);

    // print list of elements
    Roomy_log("Size of list: %lli\n", RoomyList_size(rl));
    Roomy_log("PRINTING LIST\n");
    RoomyList_map(rl, printEltList);

    // print array of elements
    Roomy_log("PRINTING ARRAY\n");
    RoomyArray_map(ra, printEltArray);

    // use unix sort and diff tools to confirm two lists match
}

// Test that no two Roomy data structures can have the same name.
void testNameChecking() {
    RoomyArray* ra1 = RoomyArray_makeBytes("one", sizeof(uint64), 100);
    assert(ra1 != NULL);
    RoomyHashTable* rht1 =
        RoomyHashTable_make("two", sizeof(uint64), sizeof(uint64), 100);
    assert(rht1 != NULL);
    RoomyList* rl1 = RoomyList_make("three", sizeof(uint64));
    assert(rl1 != NULL);

    // No data structures with names "one", "two", or "three" should be
    // possible.
    
    RoomyArray* raTest = RoomyArray_makeBytes("one", sizeof(uint64), 100);
    assert(raTest == NULL);
    RoomyHashTable* rhtTest =
        RoomyHashTable_make("one", sizeof(uint64), sizeof(uint64), 100);
    assert(rhtTest == NULL);
    RoomyList* rlTest = RoomyList_make("one", sizeof(uint64));
    assert(rlTest == NULL);
    
    raTest = RoomyArray_makeBytes("two", sizeof(uint64), 100);
    assert(raTest == NULL);
    rhtTest =
        RoomyHashTable_make("two", sizeof(uint64), sizeof(uint64), 100);
    assert(rhtTest == NULL);
    rlTest = RoomyList_make("two", sizeof(uint64));
    assert(rlTest == NULL);

    raTest = RoomyArray_makeBytes("three", sizeof(uint64), 100);
    assert(raTest == NULL);
    rhtTest =
        RoomyHashTable_make("three", sizeof(uint64), sizeof(uint64), 100);
    assert(rhtTest == NULL);
    rlTest = RoomyList_make("three", sizeof(uint64));
    assert(rlTest == NULL);

    // Destroy "one" and try again. It should work.
    RoomyArray_destroy(ra1);
    raTest = RoomyArray_makeBytes("one", sizeof(uint64), 100);
    assert(raTest != NULL);
    RoomyArray_destroy(raTest);
    rhtTest =
        RoomyHashTable_make("one", sizeof(uint64), sizeof(uint64), 100);
    assert(rhtTest != NULL);
    RoomyHashTable_destroy(rhtTest);
    rlTest = RoomyList_make("one", sizeof(uint64));
    assert(rlTest != NULL);
    RoomyList_destroy(rlTest);

    RoomyHashTable_destroy(rht1);
    RoomyList_destroy(rl1);

    Roomy_log("SUCCESS: testNameChecking\n");
}

// Test generation of unique integers.
RoomyList* uniqueIntList;
void addUniqueInt(uint64 i, void* oldVal) {
    uint64 num = Roomy_uniqueInt();
    RoomyList_add(uniqueIntList, &num);
}
void testUniqueInt() {
    // Add numbers to list in both serial and parallel mode and make sure there
    // are no duplicates.
    uniqueIntList = RoomyList_make("uniqueIntList", sizeof(uint64));
    RoomyArray* raTest = RoomyArray_makeBytes("raTest", sizeof(uint64), 100);
    RoomyArray_map(raTest, addUniqueInt);
    int i;
    for (i = 0; i < 100; i++) {
        uint64 i = Roomy_uniqueInt();
        RoomyList_add(uniqueIntList, &i);
    }
    RoomyList_sync(uniqueIntList);
    RoomyList_removeDupes(uniqueIntList);
    assert(RoomyList_size(uniqueIntList) == 200);
    RoomyList_destroy(uniqueIntList);

    // Try again after resetting unique int
    Roomy_resetUniqueInt(42);
    uniqueIntList = RoomyList_make("test", sizeof(uint64));
    RoomyArray_map(raTest, addUniqueInt);
    for (i = 0; i < 100; i++) {
        uint64 i = Roomy_uniqueInt();
        RoomyList_add(uniqueIntList, &i);
    }
    RoomyList_sync(uniqueIntList);
    RoomyList_removeDupes(uniqueIntList);
    assert(RoomyList_size(uniqueIntList) == 200);
    RoomyList_destroy(uniqueIntList);
    RoomyArray_destroy(raTest);

    Roomy_log("SUCCESS: testUniqueInt\n");
}

int main(int argc, char **argv) {
    Roomy_init(&argc, &argv);
    Roomy_log("Beginning primary test\n");
    testSmallRoomyArray();    
    //testListToArray();
    testNameChecking();
    testUniqueInt();
    Roomy_log("Finished primary test\n");
    Roomy_finalize();
    return 0;
}
