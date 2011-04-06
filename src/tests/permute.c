#include "roomy.h"
#include "RoomyArray.h"
#include "Timer.h"
#include "types.h"

#include <stdio.h>
#include <stdlib.h>

// If true, verify Roomy permutation matches simple in-memory version
#define VERIFY 0

// If true, use the implicit index method
#define IMPLICIT_METHOD 1

// Use the standard Roomy buckets method
#define BUCKET_METHOD 1

// Number of elements in each array
uint64 N;

// three arrays containing uint64 values
RoomyArray *X;
RoomyArray *Y;
RoomyArray *Z;

/* Initialize X and Y arrays to arbitrary (but not random) permutations.
 * X[i] = (i * p) % N, where p is a large prime number.
 */

// Some large prime numbers
#define NUM_PRIMES 8
uint64 primes[NUM_PRIMES] =
  {15486833, 16319977, 17241031, 18644201, 19746217, 20365351, 21481673, 22505149}; 

int CURRENT_PRIME = 0;

// Set permutation values, used with RoomyArray_mapAndModify.
void setPermValue(uint64 i, void* oldVal, void* newVal) {
    *(uint64*)newVal = (i * primes[CURRENT_PRIME]) % N;
}

// Return a large random number, in the range [0, RAND_MAX^2).
uint64 largeRand() {
    return RAND_MAX * rand() + rand();
}

/* Confirm that the given array is a permutation of uint64 */

RoomyArray* eltCount;

void incrementUint8(uint64 i, void* oldVal, void* passedVal, void* newVal) {
    *(uint8*)newVal = *(uint8*)oldVal + 1;
}

void recordElt(uint64 i, void* value) {
    RoomyArray_update(eltCount, *(uint64*)value, NULL, incrementUint8);
}

void assertEqual1(uint64 i, void* value) {
    assert(*(uint8*)value == 1);
}

void verifyPermutation(RoomyArray* ra) {
    assert(ra->bytesPer == sizeof(uint64));
    eltCount =
        RoomyArray_makeBytes("eltCount", sizeof(uint8), RoomyArray_size(ra));
    RoomyArray_registerUpdateFunc(eltCount, incrementUint8, 0);
    RoomyArray_map(ra, recordElt);
    RoomyArray_sync(eltCount);
    RoomyArray_map(eltCount, assertEqual1);
    RoomyArray_destroy(eltCount);
}

void printPerm(uint64 i, void* value) {
   Roomy_logAny("ra[%lli] = %lli\n", i, *(uint64*)value);
}

/* Perform permutation multiplication: Z[i] = Y[X[i]], for i in [0, N) */

// Pre-declare delayed methods
void mapXtoY(uint64 i, void* xVal);
void accessXandY(uint64 i, void* yVal, void* xVal);
void setZ(uint64 i, void* oldZVal, void* newZVal, void* newZValOut);

// Map X values to Y values.
void mapXtoY(uint64 i, void* xVal) {
    uint64 x_i = *(uint64*)xVal;
    RoomyArray_access(Y, x_i, &i, accessXandY);
}

// Access X and Y values, update Z array.
void accessXandY(uint64 i, void* yVal, void* xVal) {
    uint64 dest_index = *(uint64*)xVal;
    RoomyArray_update(Z, dest_index, yVal, setZ);
}

// Set a value of the Z array.
void setZ(uint64 i, void* oldZVal, void* newZVal, void* newZValOut) {
    *(uint64*)newZValOut = *(uint64*)newZVal;
}

/* In-core permutation and verification */

uint64* inCoreX;
uint64* inCoreY;
uint64* inCoreZ;

// Perform permutation in-core.
void inCorePermute(int prime1, int prime2) {
    inCoreX = malloc(N * sizeof(uint64));
    inCoreY = malloc(N * sizeof(uint64));
    inCoreZ = malloc(N * sizeof(uint64));
    int i;
    for (i = 0; i < N; i++) {
        inCoreX[i] = (i * primes[prime1]) % N;
        inCoreY[i] = (i * primes[prime2]) % N;
    }
    for (i = 0; i < N; i++) {
        inCoreZ[i] = inCoreY[inCoreX[i]];
    }
}

// Verify Z == inCoreZ.
void verifyZ(uint64 i, void* zVal) {
    assert(inCoreZ[i] == *(uint64*)zVal);
}

// Destroy in-core data structures.
void destroyInCoreData() {
    free(inCoreX);
    free(inCoreY);
    free(inCoreZ);
}

/* Top-level Roomy permutation method */
void roomyPermute() {
    Timer timer;
    char timeString[1024];

    Roomy_log("Beginning permutation of %lli elements.\n", N);

    // Initialize arbitrary permutations
    /*
    X = RoomyArray_makeBytes("X", sizeof(uint64), N);
    Y = RoomyArray_makeBytes("Y", sizeof(uint64), N);
    Z = RoomyArray_makeBytes("Z", sizeof(uint64), N);
    int permsToUse[2] = {0, 1};
    CURRENT_PRIME = permsToUse[0];
    RoomyArray_mapAndModify(X, setPermValue);
    RoomyArray_sync(X);
    CURRENT_PRIME = permsToUse[1];
    RoomyArray_mapAndModify(Y, setPermValue);
    RoomyArray_sync(Y);
    */

    // Initialize truely random permutations
    X = RoomyArray_makeRandomPerm("X", N);
    Y = RoomyArray_makeRandomPerm("Y", N);
    Z = RoomyArray_makeBytes("Z", sizeof(uint64), N);

    RoomyArray_registerAccessFunc(Y, accessXandY, sizeof(uint64));
    RoomyArray_registerUpdateFunc(Z, setZ, sizeof(uint64));

    if (VERIFY) {
        verifyPermutation(X);
        verifyPermutation(Y);
    }

    Roomy_log("Permutations initialized.\n");

    // Use high-level method
    if (BUCKET_METHOD) {

    // Time permutation operation.
    Timer_start(&timer);

    // Z[i] = Y[X[i]]
    RoomyArray_map(X, mapXtoY);
    RoomyArray_sync(Y);
    RoomyArray_sync(Z);

    Timer_stop(&timer);

    Roomy_log(
      "BUCKET permute done: %lli elements (%lli bytes) in each array\n",
      N, N * sizeof(uint64));
    Timer_print(&timer, timeString, 1024);
    Roomy_log("BUCKET Elapsed time: %s\n", timeString);

    // Verify results of high-level implementation
    if (VERIFY) {
        //inCorePermute(permsToUse[0], permsToUse[1]);
        //RoomyArray_map(Z, verifyZ);
        //Roomy_log("BUCKET method CORRECT\n");
        verifyPermutation(Z);
    }

    } // end BUCKET

    // Use implicit index method
    if (IMPLICIT_METHOD) {

    Timer_start(&timer);
    RoomyArray_permute(X, Y, Z);
    Timer_stop(&timer);

    Roomy_log(
      "IMPLICIT INDEX permute done: %lli elements (%lli bytes) in each array\n",
      N, N * sizeof(uint64));
    Timer_print(&timer, timeString, 1024);
    Roomy_log("IMPLICIT INDEX Elapsed time: %s\n", timeString);

    // Verify results of low-level implementation
    if (VERIFY) {
        //RoomyArray_map(Z, verifyZ);
        //destroyInCoreData();
        //Roomy_log("IMPLICIT INDEX method CORRECT\n");
        verifyPermutation(Z);
    }

    } // end IMPLICIT INDEX

    // Destroy
    RoomyArray_destroy(X);
    RoomyArray_destroy(Y);
    RoomyArray_destroy(Z);

    Roomy_log("\n");
}

// Test timing for random access
void randAccessTime() {
    uint64 blockSize = 1024 * 1024 * 1024;
    uint64 blockElts = blockSize / sizeof(uint64);
    uint64 numBlocks = 8;
    uint64 accessPerBlock = blockSize / sizeof(uint64);

    printf("Starting random access timing\n");

    uint64* array = malloc(blockSize);
    Timer timer;
    Timer_start(&timer);
    int i, j;
    for (i = 0; i < numBlocks; i++) {
        for (j = 0; j < accessPerBlock; j++) {
            array[largeRand() % blockElts] = 42;
        }
    }
    Timer_stop(&timer);
    char timeString[1024];
    Timer_print(&timer, timeString, 1024);
    printf(
        "Random access: blockSize %lli, numBlocks %lli, accessPerBlock %lli\n",
        blockSize, numBlocks, accessPerBlock);
    printf("Elapsed time: %s\n", timeString);
}

int main(int argc, char **argv) {
    Roomy_init(&argc, &argv);

    //N = 1000000L; // 1M elts

    // Try power-of-two-sizes between startSize and endSize.
    //uint64 startSize = 50000000L; // 50 M
    uint64 startSize = 1600000000L; // 1.6 B
    //uint64 startSize = 3200000000L; // 3.2 B
    //uint64 startSize = 12800000000L; // 12.8 B

    //uint64 endSize = 50000000L; // 50 M
    uint64 endSize = 1600000000L; // 1.6 B
    //uint64 endSize   = 25600000000L; // 24.8 B

    N = startSize;
    while (N <= endSize) {
        roomyPermute();
        N *= 2;
    }
    
    Roomy_finalize();

    //randAccessTime();
    return 0;
}
