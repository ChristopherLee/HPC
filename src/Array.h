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
 * Array.h
 *
 * A fixed sized array containing uniformly sized elements, that size being an
 * arbitrary number of bytes.
 *****************************************************************************/

#ifndef _ARRAY_H
#define _ARRAY_H

#include "types.h"

#include <stdio.h>
#include <stdlib.h>
#include <assert.h>

/*************************************************************************
                             DATA TYPES 
*************************************************************************/

/* a general array */
typedef struct {
    int eltSize;        // number of bytes per element
    uint64 numElts;     // total number of elements
    uint64 totalBytes;  // total number of bytes
    uint8* data;        // ptr to head of the array
    int ownMemory;      // 1 if Array owns memory for data, 0 if owned by client
    int mapped;         // 1 if data is mmap'ed from a file
    int file;           // if mapped == 1, the file being used by mmap
} Array;


/*************************************************************************
                          FUNCTION HEADERS 
*************************************************************************/

Array Array_make(int eltSize, uint64 numElts);
Array Array_makeInit(int eltSize, uint64 numElts, void* data);
Array Array_makeMapped(int eltSize, uint64 numElts, char* filename, uint64 offset);
void Array_fileLoad(Array* a, char* filename);
void Array_fileDescLoad(Array* a, FILE* f, uint64 numElts);
void Array_fileSave(Array* a, char* filename);
void Array_fileDescSave(Array* a, FILE* f, uint64 numElts);
uint64 Array_size(Array* a);
void* Array_get(Array* a, int i);
void Array_set(Array* a, int i, void* value);
void Array_setK(Array* a, int i, int k, void* value);
void Array_destroy(Array* a);

#endif
