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
 * BitArray.c
 *
 * An array of bits, where individual bits can be set, cleared, or tested.
 *****************************************************************************/

#include "BitArray.h"
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
                               GLOBALS
*************************************************************************/

/*************************************************************************
 * Make a Bit Array with the given number of bits. All elements are unset.
 */

BitArray BitArray_make(uint64 size) {
    BitArray ba;
    ba.size = size;
    ba.numBytes = size/8;
    if(size % 8) ba.numBytes++;
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
 * should be at least size/8 bytes.
 */
BitArray BitArray_makeInit(uint64 size, void* buffer) {
    BitArray ba;
    ba.size = size;
    ba.numBytes = size/8;
    if(size % 8) ba.numBytes++;
    ba.array = (uint8*)buffer;
    ba.ownMemory = 0;
    ba.mapped = 0;
    ba.file = 0;

    return ba;
}

/*************************************************************************
 * Create a BitArray to manipulate data in the given file, beginning at the
 * given offset, using mmap.
 */
BitArray BitArray_makeMapped(uint64 size, char* filename, uint64 offset) {
    BitArray ba;
    ba.size = size;
    ba.numBytes = size/8;
    if(size % 8) ba.numBytes++;
    ba.file = open(filename, O_RDWR);
    ba.array =
        mmap(0, ba.numBytes, PROT_READ | PROT_WRITE, MAP_SHARED, ba.file, offset);
    ba.ownMemory = 1;
    ba.mapped = 1;

    return ba;
}

/*************************************************************************
 * Load the bits in the given file into the given BitArray. The file
 * should have at least ba.size bits.
 */
void BitArray_fileLoad(BitArray* ba, char* filename) {
    FILE* f = fopen(filename, "r");
    if (f == NULL || fread(ba->array, 1, ba->numBytes, f) != ba->numBytes) {
        fprintf(stderr, "ERROR: BitArray_fileLoad failed read from %s\n", filename);
        exit(1);
    }
    fclose(f);
}

/*************************************************************************
 * Save the state of the given BitArray to the given filename.
 */
void BitArray_fileSave(BitArray* ba, char* filename) {
    FILE* f = fopen(filename, "w");
    if (f == NULL || fwrite(ba->array, 1, ba->numBytes, f) != ba->numBytes) {
        fprintf(stderr, "ERROR: BitArray_fileSave failed write to %s\n", filename);
        exit(1);
    }
    fclose(f);
}

/*************************************************************************
 * Return 1 if the given index is set, 0 otherwise.
 */
int BitArray_test(BitArray* ba, uint64 i) {
    assert(i < ba->size);
    return (ba->array[i/8] >> (i%8)) & 1;
}

/*************************************************************************
 * Set the given bit.
 */
void BitArray_set(BitArray* ba, uint64 i) {
    assert(i < ba->size);
    ba->array[i/8] = ba->array[i/8] | (1 << (i%8));
}

/*************************************************************************
 * Clear the given bit.
 */
void BitArray_clear(BitArray* ba, uint64 i) {
    assert(i < ba->size);
    ba->array[i/8] = ba->array[i/8] & ~(1 << (i%8));
}

/*************************************************************************
 * Destroy the given BitArray (free memory).
 */
void BitArray_destroy(BitArray* ba) {
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
