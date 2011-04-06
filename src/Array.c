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
 * Array.c
 *
 * A fixed sized array containing uniformly sized elements, that size being an
 * arbitrary number of bytes.
 *****************************************************************************/

#include "Array.h"
#include "types.h"
#include "knusort.h"

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
 * Make an Array with the given number of elements of the given size.
 */
Array Array_make(int eltSize, uint64 numElts) {
    Array a;
    a.eltSize = eltSize;
    a.numElts = numElts;
    a.totalBytes = eltSize * numElts;
    a.data = malloc(a.totalBytes);
    Roomy_ramAllocated(a.totalBytes);    
    a.ownMemory = 1;
    memset(a.data, 0, a.totalBytes);
    a.mapped = 0;
    a.file = 0;

    return a;
}

/*************************************************************************
 * Initialize a new Array that uses the given memory as the initial
 * data and as the Array's main data storage space. The caller is responsible
 * for the given memory buffer (Array will not free the space when destroyed).
 */
Array Array_makeInit(int eltSize, uint64 numElts, void* data) {
    Array a;
    a.eltSize = eltSize;
    a.numElts = numElts;
    a.totalBytes = eltSize * numElts;
    a.data = (uint8*)data;
    a.ownMemory = 0;
    a.mapped = 0;
    a.file = 0;

    return a;
}

/*************************************************************************
 * Create an Array to manipulate data in the given file, beginning at the
 * given offset, using mmap.
 */
Array Array_makeMapped(int eltSize, uint64 numElts, char* filename, uint64 offset) {
    Array a;
    a.eltSize = eltSize;
    a.numElts = numElts;
    a.totalBytes = eltSize * numElts;
    a.file = open(filename, O_RDWR);
    a.data =
        mmap(0, a.totalBytes, PROT_READ | PROT_WRITE, MAP_SHARED, a.file, offset);
    a.ownMemory = 1;
    a.mapped = 1;

    return a;
}

/*************************************************************************
 * Fill the given Array with data from the given file. The file
 * should have at least a.totalBytes bytes.
 */
void Array_fileLoad(Array* a, char* filename) {
    FILE* f = fopen(filename, "r");
    if(f == NULL) perror("Error in Array_fileLoad:");
    Array_fileDescLoad(a, f, a->numElts);
    fclose(f);
}

/*************************************************************************
 * Load the given number of bytes from the given open file descriptor into
 * the given Array (starting at the beginning of the Array).
 */
void Array_fileDescLoad(Array* a, FILE* f, uint64 numElts) {
    int numRead = fread(a->data, a->eltSize, numElts, f);
    if(numRead < numElts) {
        fprintf(stderr, "Too few elts read in Array_fileDescLoad");
        exit(1);
    }
}

/*************************************************************************
 * Save the state of the given Array to the given filename.
 */
void Array_fileSave(Array* a, char* filename) {
    FILE* f = fopen(filename, "w");
    if(f == NULL) perror("Error in Array_fileSave:");
    Array_fileDescSave(a, f, a->numElts);
    fclose(f);
}

/*************************************************************************
 * Save the first numElts elements from the given Array to the given
 * open file descriptor.
 */
void Array_fileDescSave(Array* a, FILE* f, uint64 numElts) {
    int numWrote = fwrite(a->data, a->eltSize, numElts, f);
    if(numWrote < numElts) {
        fprintf(stderr, "Too few bytes written in Array_fileSave");
        exit(1);
    }
}

/*************************************************************************
 * Return the number of elements in the given Array.
 */
uint64 Array_size(Array* a) {
    return a->numElts;
}

/*************************************************************************
 * Return a pointer to the i_th element of the given Array
 */
void* Array_get(Array* a, int i) {
    assert(i < a->numElts);
    return &(a->data[i*a->eltSize]);
}

/*************************************************************************
 * Set i_th element of the given Array to the given value.
 */
void Array_set(Array* a, int i, void* value) {
    assert(i < a->numElts);
    memcpy(&(a->data[i*a->eltSize]), value, a->eltSize);
}

/*************************************************************************
 * Set k elements, starting with the i_th element, of the given Array to the
 * given value.
 */
void Array_setK(Array* a, int i, int k, void* value) {
    assert(i+k-1 < a->numElts);
    memcpy(&(a->data[i*a->eltSize]), value, a->eltSize * k);
}

/*************************************************************************
 * Sort the first n elements of the array into ascending order.
 */
void Array_sort(Array* a, int n) {
    quicksort(a->data, n, a->eltSize);
}

/*************************************************************************
 * Destroy the given Array (free memory).
 */
void Array_destroy(Array* a) {
    if (!a->mapped) {
        if (a->ownMemory) {
            free(a->data);
            Roomy_ramFreed(a->totalBytes);
        }
    } else {
        if(munmap(a->data, a->totalBytes) == -1) {
            fprintf(stderr, "Error destroying Array, munmap failed.");
            exit(1);
        }
        close(a->file);
    }
}
