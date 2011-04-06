#include "knudirs.h"
#include "knusort.h"
#include "types.h"
#include "WriteBuffer.h"
#include "ReadBuffer.h"

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

typedef struct {
    uint64 key;
    uint64 value;
} KeyValue;

int compareKV(void* p1, void* p2, int size) {
    KeyValue* kv1 = (KeyValue*)p1;
    KeyValue* kv2 = (KeyValue*)p2;
    if(kv1->key < kv2->key) return -1;
    else if(kv2->key < kv1->key) return 1;
    return 0;
}

// test external sorting a file with the given number of bytes
void testSort(uint64 bytesInFile) {
    assert(bytesInFile >= sizeof(KeyValue));
    printf("Testing sort. Writting...\n");

    uint64 numKV = bytesInFile / sizeof(KeyValue);
    uint64 mult = 7;
    int removeDupes = 1;
    KeyValue kv;
    char* sortDir = "/tmp/";
    char* dataFileName = "/tmp/testSort.data";
    char* sortedFileName = "/tmp/testSort.sorted";
    if(fileExists(dataFileName)) unlink(dataFileName);
    if(fileExists(sortedFileName)) unlink(sortedFileName);
    WriteBuffer wb = WriteBuffer_make(1<<20, sizeof(KeyValue), dataFileName, 1);
    uint64 val = 42L;
    // write data
    uint64 ndupes = 2;
    uint64 i, d;
    for(d=0; d<ndupes; d++) {
        for(i=0; i<numKV; i++) {
            kv.key = i * mult % numKV;
            //kv.value = rand();
            kv.value = val;
            WriteBuffer_write(&wb, &kv);
        }
    }
    WriteBuffer_destroy(&wb);

    printf("File with %lli bytes written. Reading...\n",
           numKV * ndupes * (uint64)sizeof(KeyValue));

    // read it back and make sure it's what was written
    ReadBuffer rb = ReadBuffer_make(1<<20, sizeof(KeyValue), dataFileName, 1);
    for(d=0; d<ndupes; d++) {
        for(i=0; i<numKV; i++) {
            KeyValue* curKV = (KeyValue*)ReadBuffer_current(&rb);
            assert(curKV->key == i * mult % numKV);
            assert(curKV->value == val);
            ReadBuffer_next(&rb);
        }
    }
    ReadBuffer_destroy(&rb);

    printf("File read back sucessfully. Sorting...\n");

    // sort and remove dupes
    char** fileNames = malloc(sizeof(char*));
    fileNames[0] = dataFileName;
    extMergeSortByFunc(fileNames, 1, 1, removeDupes, sortedFileName, sortDir,
                       sizeof(KeyValue), compareKV, 1<<26);
    free(fileNames);
    if(removeDupes) {
        ndupes = 1; // duplicates have been removed
    }

    printf("Sorting done. Checking order...\n");

    // check sorted order
    rb = ReadBuffer_make(1<<20, sizeof(KeyValue), sortedFileName, 1);
    for(i=0; i<numKV*ndupes; i++) {
        KeyValue* curKV = (KeyValue*)ReadBuffer_current(&rb);
        if(curKV->key != i/ndupes) {
            printf("WRONG: key=%lli, i=%lli\n", curKV->key, i);
            assert(curKV->key == i/ndupes);
        }
        ReadBuffer_next(&rb);
    }
    assert(!ReadBuffer_hasMore(&rb));
    ReadBuffer_destroy(&rb);

    printf("SORT TEST SUCCESS (%lli bytes)\n", bytesInFile);

    unlink(dataFileName);
    unlink(sortedFileName);
}

int main(int argc, char **argv) {
    testSort(16);
    testSort(1<<10);
    testSort(1<<20);
    testSort(128 * 1<<20);
    return 0;
}
