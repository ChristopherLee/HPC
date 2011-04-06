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
 * knustring.c
 *
 * A set of string utilities.
 *****************************************************************************/

#include "knustring.h"
#include "types.h"

#include <stdlib.h>
#include <string.h>

/*************************************************************************
 * Return a new array of strings holding size strings, each of length
 * maxStrLen.
 */
char** createStringArray(int size, int maxStrLen) {
    char** strs = malloc(sizeof(char*) * size);
    int i;
    for(i=0; i<size; i++) {
        strs[i] = malloc(sizeof(char) * maxStrLen);
    }
    return strs;
}

/*************************************************************************
 * Free all of the space used by the given array of strings
 */
void freeStringArray(char **array, int size) {
    int i;
    for(i=0; i<size; i++) {
        free(array[i]);
    }
    free(array);
}

/*************************************************************************
 * Given an array of strings: create a new array with double the
 * size; copy all of the elements to the new array; and free the old array.
 * Store the new array at the given pointer, and return the size of the new
 * array.
 */
int growStringArray(char ***array, int arraySize, int maxStrLen) {
    char **newArray = malloc(arraySize * 2 * sizeof(char*));
    int i;
    for(i=0; i<arraySize; i++) {
        newArray[i] = (*array)[i];
    }
    for(i=arraySize; i<arraySize*2; i++) {
        newArray[i] = malloc(maxStrLen * sizeof(char));
    }
    free(*array);
    *array = newArray;
    return arraySize * 2;
}

/*************************************************************************
 * Return a new array of strings holding a subset of the given array of
 * strings. If matchAll is true, strings matching all of the given substrings
 * are returned. Otherwise, strings matching any of the given substrings are
 * returned. The returned array will be of the same size as the input.  Note,
 * only pointers to strings are coppied to new array (strings are not cloned).
 * The caller is responsible for freeing the resulting array.
 */
int filterStringArray(char** inArray, int size, char** subStrs, int numSub,
                         int matchAll, char*** result) {
    *result = malloc(sizeof(char*) * size);
    int numMatches = 0;
    int i;
    for(i=0; i<size; i++) {
        if((matchAll  && matchesAll(inArray[i], subStrs, numSub)) ||
           (!matchAll && matchesAny(inArray[i], subStrs, numSub)))
            (*result)[numMatches++] = inArray[i];
    }
    return numMatches;
}

/*************************************************************************
 * Return true if the given string contains any of the given substrings.
 */
int matchesAny(char* str, char** substrs, int numSub) {
    int i;
    for(i=0; i<numSub; i++) {
        if(strstr(str, substrs[i]) != NULL) return 1;
    }
    return 0;
}

/*************************************************************************
 * Return true if the given string contains all of the given substrings.
 */
int matchesAll(char* str, char** substrs, int numSub) {
    int i;
    for(i=0; i<numSub; i++) {
        if(strstr(str, substrs[i]) == NULL) return 0;
    }
    return 1;
}
