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
 * BitsArray.c
 *
 * An array with between 2 and 7 bits per element (i.e. storing values in the
 * range [0, 128]). See BitArray for elements with a single bit, and Array
 * for elements with one or more bytes.
 *****************************************************************************/

#include "BitsArray.h"
#include "types.h"

#include <assert.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>

/*************************************************************************
 * Make a BitsArray with the given number of bits. All elements zeroed.
 */
BitsArray BitsArray_make(uint8 bitsPerElt, uint64 numElts) {
    BitsArray ba;
    ba.elts = numElts;
    ba.bitsPer = bitsPerElt;
    uint64 nBits = ba.elts*ba.bitsPer;
    ba.numBytes = nBits/8;
    if(nBits % 8) ba.numBytes++;
    ba.array = malloc(ba.numBytes);
    Roomy_ramAllocated(ba.numBytes);    
    memset(ba.array, 0, ba.numBytes);
    ba.ownMemory = 1;
    ba.mapped = 0;
    ba.file = 0;

    return ba;
}

/*************************************************************************
 * Initialize a new BitArray that uses the given memory buffer. The initial
 * state is the current contents of the buffer. The size of the buffer
 * should be at least the size of the BitsArray.
 */
BitsArray BitsArray_makeInit(uint8 bitsPerElt, uint64 numElts, void* buffer) {
    BitsArray ba;
    ba.elts = numElts;
    ba.bitsPer = bitsPerElt;
    uint64 nBits = ba.elts*ba.bitsPer;
    ba.numBytes = nBits/8;
    if(nBits % 8) ba.numBytes++;
    ba.array = buffer;
    ba.ownMemory = 0;
    ba.mapped = 0;
    ba.file = 0;

    return ba;
}

/*************************************************************************
 * Create a BitsArray to manipulate data in the given file, beginning at the
 * given offset, using mmap.
 */
BitsArray BitsArray_makeMapped(uint8 bitsPerElt, uint64 numElts,
                               char* filename, uint64 offset) {
    BitsArray ba;
    ba.elts = numElts;
    ba.bitsPer = bitsPerElt;
    uint64 nBits = ba.elts*ba.bitsPer;
    ba.numBytes = nBits/8;
    if(nBits % 8) ba.numBytes++;
    ba.file = open(filename, O_RDWR);
    ba.array =
        mmap(0, ba.numBytes, PROT_READ | PROT_WRITE, MAP_SHARED, ba.file, offset);
    ba.ownMemory = 1;
    ba.mapped = 1;

    return ba;
}

/*************************************************************************
 * Load the bits in the given file into the given BitsArray. The file
 * should have at least as many bytes as the BitsArray.
 */
void BitsArray_fileLoad(BitsArray* ba, char* filename) {
    FILE* f = fopen(filename, "r");
    if (f == NULL || fread(ba->array, 1, ba->numBytes, f) != ba->numBytes) {
        fprintf(stderr, "ERROR: BitsArray_fileLoad failed read from %s\n",
                filename);
        exit(1);
    }
    fclose(f);
}

/*************************************************************************
 * Save the state of the given BitsArray to the given filename.
 */
void BitsArray_fileSave(BitsArray* ba, char* filename) {
    FILE* f = fopen(filename, "w");
    if (f == NULL || fwrite(ba->array, 1, ba->numBytes, f) != ba->numBytes) {
        fprintf(stderr, "ERROR: BitsArray_fileSave failed write to %s\n", filename);
        exit(1);
    }
    fclose(f);
}

/*************************************************************************
 * Make a bit mask with nBits 1's, starting from startBit. All other bits
 * are 0's. The high-order bit is bit 0.
 */
uint8 mask(uint8 startBit, uint8 nBits) {
    return ((1<<nBits)-1) << (8-startBit-nBits);
}

/*************************************************************************
 * Return the value at the given index of the given BitsArray.
 */
uint8 BitsArray_get(BitsArray* ba, uint64 i) {
    assert(i < ba->elts);

    uint64 byte = (i*ba->bitsPer)/8;    // the first byte containing target bits
    uint8 bitStart = (i*ba->bitsPer)%8; // the first target bit in that byte
    uint8 bitsIn1 = 8-bitStart;         // number of target bits in that bytes
    uint8 m = mask(8-ba->bitsPer, ba->bitsPer);
    if(bitsIn1 >= ba->bitsPer) {
        // all bits are in first byte
        return (ba->array[byte] >> (8-ba->bitsPer-bitStart)) & m;
    } else {
        // combine bits from first and second bytes
        uint8 bitsIn2 = ba->bitsPer - bitsIn1;
        uint8 whole = (ba->array[byte] << bitsIn2) +
                      (ba->array[byte+1] >> (8-bitsIn2));
        return whole & m;
    }
}

/*************************************************************************
 * Set the value at the given index of the given BitsArray to the given value.
 */
void BitsArray_set(BitsArray* ba, uint64 i, uint8 val) {
    assert(i < ba->elts);
    assert(val < (1<<ba->bitsPer));

    uint64 byte = (i*ba->bitsPer)/8;    // the first byte containing target bits
    uint8 bitStart = (i*ba->bitsPer)%8; // the first target bit in that byte
    uint8 bitsIn1 = 8-bitStart;         // number of target bits in that bytes
    if(bitsIn1 >= ba->bitsPer) {
        // all bits are in first byte
        uint8 m = mask(bitStart, ba->bitsPer);
        ba->array[byte] = ba->array[byte] & ~m; // clear target bits, then set
        ba->array[byte] = ba->array[byte] | (val << (8-ba->bitsPer-bitStart));
    } else {
        // split bits between first and second bytes (clear then set, as above)
        uint8 m = mask(8-bitsIn1, bitsIn1);
        ba->array[byte] = ba->array[byte] & ~m;
        ba->array[byte] = ba->array[byte] | (val >> (ba->bitsPer-bitsIn1));

        uint8 bitsIn2 = ba->bitsPer - bitsIn1;
        m = mask(0, bitsIn2);
        ba->array[byte+1] = ba->array[byte+1] & ~m;
        ba->array[byte+1] = ba->array[byte+1] | (val << (8-bitsIn2));
    }
}

/*************************************************************************
 * Destroy the given BitsArray (free memory).
 */
void BitsArray_destroy(BitsArray* ba) {
    if (!ba->mapped) {
        if (ba->ownMemory) {
            free(ba->array);
            Roomy_ramFreed(ba->numBytes);
        }
    } else {
        if(munmap(ba->array, ba->numBytes) == -1) {
            fprintf(stderr, "Error destroying BitArray, munmap failed.");
            exit(1);
        }
        close(ba->file);
    }
}

/*************************************************************************
                                TESTS
*************************************************************************/

void test() {
    // make a BitsArray of each size (2 to 7 bits) and test gets/sets
    int numElts = 10000;
    int i;
    for(i=2; i<8; i++) {
        BitsArray ba = BitsArray_make(i, numElts);
        int j;
        for(j=0; j<numElts; j++) {
            BitsArray_set(&ba, j, j%i);
        }
        for(j=0; j<numElts; j++) {
            //printf("i=%i, j=%i, val=%i\n", i, j, BitsArray_get(&ba, j));
            assert(BitsArray_get(&ba, j) == j%i);
        }
    }
    printf("BitsArray passed tests\n");
}
