#include "Array.h"
#include "BitArray.h"
#include "HashTable.h"
#include "ReadBuffer.h"
#include "WriteBuffer.h"
#include "types.h"

#include <stdio.h>
#include <stdlib.h>

void testArray() {
    int n = 100;
    // make
    Array a = Array_make(sizeof(int), n);
    
    // set and get
    int i;
    for(i=0; i<n; i++) {
        Array_set(&a, i, &i);
    }
    for(i=0; i<n; i++) {
        int* val = (int*)Array_get(&a, i);
        assert(*val == i);
    }

    // save and load
    char* fname = "/tmp/test.array";
    Array_fileSave(&a, fname);
    Array b = Array_make(sizeof(int), n);
    Array_fileLoad(&b, fname);
    for(i=0; i<n; i++) {
        int* val = (int*)Array_get(&b, i);
        assert(*val == i);
    }

    // make init
    int buf[n];
    for(i=0; i<n; i++) {
        buf[i] = i;
    }
    Array c = Array_makeInit(sizeof(int), n, &buf);
    for(i=0; i<n; i++) {
        int* val = (int*)Array_get(&c, i);
        assert(*val == i);
    }

    Array_destroy(&a);
    Array_destroy(&b);
    Array_destroy(&c);

    printf("ARRAY PASSED TESTS\n");
}

void testBitArray() {
    int num = 100;
    BitArray ba = BitArray_make(num);

    // test all are initially clear
    int i;
    for(i=0; i<num; i++) assert(!BitArray_test(&ba, i));

    // test setting all
    for(i=0; i<num; i++) {
        BitArray_set(&ba, i);
        assert(BitArray_test(&ba, i));
    }

    // test clearing all
    for(i=0; i<num; i++) {
        BitArray_clear(&ba, i);
        assert(!BitArray_test(&ba, i));
    }

    // test that setting any one does not set another
    for(i=0; i<num; i++) {
        BitArray_set(&ba, i);
        int j;
        for(j=0; j<num; j++) {
           if(i!=j) assert(!BitArray_test(&ba, j));
        }
        BitArray_clear(&ba, i);
    }
    
    // test that clearing any one does not clear another
    for(i=0; i<num; i++) {
        BitArray_set(&ba, i);
        assert(BitArray_test(&ba, i));
    }
    for(i=0; i<num; i++) {
        BitArray_clear(&ba, i);
        int j;
        for(j=0; j<num; j++) {
           if(i!=j) assert(BitArray_test(&ba, j));
        }
        BitArray_set(&ba, i);
    }
    
    BitArray_destroy(&ba);

    printf("BIT ARRAY PASSED TESTS\n");
}

void testHashTable() {
    typedef struct {
        uint64 key;
        uint64 data;
    } TestData;

    int initialCapacity = 1000;
    int numEntries = 5000;

    // make hash table
    HashTable htb = HashTable_make(sizeof(uint64), sizeof(uint64), initialCapacity);

    // insert data, and some duplicates
    TestData entries[numEntries];
    int i;
    for (i = 0; i < numEntries; i++) {
        entries[i].key = rand();
        entries[i].data = rand();
        HashTable_insert(&htb, &(entries[i].key), &(entries[i].data));
        
        // insert a previous duplicate
        if (i > 10) {
            HashTable_insert(&htb, &(entries[i-10].key), &(entries[i-10].data));
        }

        assert(HashTable_size(&htb) == i+1);
        assert(HashTable_capacity(&htb) > i);
    }

    // check data
    for (i = 0; i < numEntries; i++) {
        uint64 *data = (uint64*)HashTable_get(&htb, &(entries[i].key));
        assert(data != NULL);
        assert(*data == entries[i].data);
    }
    assert(HashTable_size(&htb) == numEntries);
    assert(HashTable_capacity(&htb) >= numEntries);

    // remove and re-insert some elements
    int numRemoved = 0;
    for (i = 0; i < numEntries; i = i + 2) {
        HashTable_delete(&htb, &(entries[i].key));
        numRemoved++;
        assert(HashTable_size(&htb) == numEntries - numRemoved);
        assert(HashTable_get(&htb, &(entries[i].key)) == NULL);
    }
    for (i = 0; i < numEntries; i = i + 2) {
        HashTable_insert(&htb, &(entries[i].key), &(entries[i].data));
        numRemoved--;
        assert(HashTable_size(&htb) == numEntries - numRemoved);
        assert(HashTable_get(&htb, &(entries[i].key)) != NULL);
    }

    // save to file, destroy, and recreate from file
    char *filename = "/tmp/roomyTestHashTable";
    HashTable_fileSave(&htb, filename);
    HashTable_destroy(&htb);
    htb = HashTable_makeFromFiles(filename);
    for (i = 0; i < numEntries; i++) {
        uint64 *data = (uint64*)HashTable_get(&htb, &(entries[i].key));
        assert(data != NULL);
        assert(*data == entries[i].data);
    }
    assert(HashTable_size(&htb) == numEntries);
    assert(HashTable_capacity(&htb) >= numEntries);
    HashTable_destroy(&htb);

    // make using mmap
    htb = HashTable_makeMapped(filename);
    for (i = 0; i < numEntries; i++) {
        uint64 *data = (uint64*)HashTable_get(&htb, &(entries[i].key));
        assert(data != NULL);
        assert(*data == entries[i].data);
    }
    assert(HashTable_size(&htb) == numEntries);
    assert(HashTable_capacity(&htb) >= numEntries);
    HashTable_destroy(&htb);
    assert(HashTable_removeFiles(filename) == 0);

    printf("HASH TABLE PASSED TESTS\n");
}

void testBuffers() {
    char* filename = "/tmp/roomyTest.out";
    int numElts = 10000;

    // write data
    WriteBuffer wb = WriteBuffer_make(1<<10, sizeof(uint64), filename, 1);
    srand(42);
    int i;
    for (i = 0; i < numElts; i++) {
        uint64 elt = rand();
        WriteBuffer_write(&wb, &elt);
    }
    WriteBuffer_destroy(&wb);

    // read and check data
    ReadBuffer rb = ReadBuffer_make(1<<10, sizeof(uint64), filename, 1);
    srand(42);
    for (i = 0; i < numElts; i++) {
        uint64* val = (uint64*)ReadBuffer_current(&rb);
        assert(*val == rand());
        ReadBuffer_next(&rb);
    }
    ReadBuffer_destroy(&rb);

    // append additional data
    wb = WriteBuffer_make(1<<10, sizeof(uint64), filename, 1);
    srand(42);
    for (i = 0; i < numElts; i++) {
        uint64 elt = rand();
        WriteBuffer_write(&wb, &elt);
    }
    WriteBuffer_destroy(&wb);

    // check data again
    rb = ReadBuffer_make(1<<10, sizeof(uint64), filename, 1);
    for (i = 0; i < numElts*2; i++) {
        if (i % numElts == 0) {
            srand(42);
        }
        uint64* val = (uint64*)ReadBuffer_current(&rb);
        assert(*val == rand());
        ReadBuffer_next(&rb);
    }
    ReadBuffer_destroy(&rb);

    printf("READ AND WRITE BUFFERS PASSED TESTS\n");
}

/* Test Array, BitArray, HashTable, ReadBuffer, and WriteBuffer (including
 * remote writing */
int main(int argc, char **argv) {
    testArray();
    testBitArray();
    testHashTable();
    testBuffers();
    return 0;
}
