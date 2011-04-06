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
 * BitArray.h
 *
 * An array of bits, where individual bits can be set, cleared, or tested.
 *****************************************************************************/

#ifndef _BITARRAY_H
#define _BITARRAY_H

#include "types.h"

#include <stdio.h>
#include <stdlib.h>
#include <assert.h>

/*************************************************************************
                             DATA TYPES 
*************************************************************************/

/* a bit array */
typedef struct {
    uint64 size;      // number of bits
    uint64 numBytes;  // number of bytes (ceil of size/8)
    uint8* array;     // ptr to head of the bit array
    int ownMemory;    // 1 if Array owns memory for data, 0 if owned by client
    int mapped;       // 1 if data is mmap'ed from a file
    int file;         // if mapped == 1, the file being used by mmap
} BitArray;


/*************************************************************************
                          FUNCTION HEADERS 
*************************************************************************/

BitArray BitArray_make(uint64 size);
BitArray BitArray_makeInit(uint64 size, void* buffer);
BitArray BitArray_makeMapped(uint64 size, char* filename, uint64 offset);
int BitArray_test(BitArray* ba, uint64 i);
void BitArray_set(BitArray* ba, uint64 i);
void BitArray_clear(BitArray* ba, uint64 i);
void BitArray_destroy(BitArray* ba);
void BitArray_fileLoad(BitArray* ba, char* filename);
void BitArray_fileSave(BitArray* ba, char* filename);

#endif
