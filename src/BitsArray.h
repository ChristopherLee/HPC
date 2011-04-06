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
 * BitsArray.h
 *
 * An array with between 2 and 7 bits per element (i.e. storing values in the
 * range [0, 128]). See BitArray for elements with a single bit, and Array
 * for elements with one or more bytes.
 *****************************************************************************/

#ifndef _BITSARRAY_H
#define _BITSARRAY_H

#include "types.h"

#include <stdio.h>
#include <stdlib.h>
#include <assert.h>

/*************************************************************************
                             DATA TYPES 
*************************************************************************/

/* a bit array */
typedef struct {
    uint64 elts;    // number of elements
    uint8 bitsPer;  // number of bits per element
    int numBytes;   // number of bytes (ceil of size/8)
    uint8* array;   // ptr to head of the bit array
    int ownMemory;  // 1 if Array owns memory for data, 0 if owned by client
    int mapped;     // 1 if data is mmap'ed from a file
    int file;       // if mapped == 1, the file being used by mmap
} BitsArray;

/*************************************************************************
                          FUNCTION HEADERS 
*************************************************************************/

BitsArray BitsArray_make(uint8 bitsPerElt, uint64 numElts);
BitsArray BitsArray_makeInit(uint8 bitsPerElt, uint64 numElts, void* buffer);
BitsArray BitsArray_makeMapped(uint8 bitsPerElt, uint64 numElts,
                               char* filename, uint64 offset);
void BitsArray_fileLoad(BitsArray* ba, char* filename);
void BitsArray_fileSave(BitsArray* ba, char* filename);
uint8 BitsArray_get(BitsArray* ba, uint64 i);
void BitsArray_set(BitsArray* ba, uint64 i, uint8 val);
void BitsArray_destroy(BitsArray* ba);

#endif
