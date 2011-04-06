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
 * knusort.h
 *
 * Procedures for sorting, both in RAM and external sort.
 *****************************************************************************/

#ifndef _KNUSORT_H
#define _KNUSORT_H

#include "types.h"

void quicksort(void* buffer, int num, int size);

void extMergeSortByByte(char** inFileNames, int numFiles, int removeOriginals,
                  int removeDupes, char* outFileName, char* tmpDir,
                  int eltSize, uint64 numBytesAvail);
void extMergeSortByKey(char** inFileNames, int numFiles, int removeOriginals,
                  int removeDupes, char* outFileName, char* tmpDir,
                  int eltSize, int keyStart, int keySize, uint64 numBytesAvail);
void extMergeSortByFunc(char** inFileNames, int numFiles, int removeOriginals,
                  int removeDupes, char* outFileName, char* tmpDir,
                  int eltSize, int (*compareFunc)(void* p1, void* p2, int size),
                  uint64 numBytesAvail);
int isSortedFile(char* filename, int eltSize,
                 int (*compareFunc)(void* p1, void* p2, int size));
uint64 numBytesInFile(char* filename);
uint64 numBytesInAllFiles(char** fileNames, int numFiles);
int compare_uint64(void* p1, void* p2, int size);

#endif
