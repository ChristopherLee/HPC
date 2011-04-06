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
 * knudirs.c
 *
 * A set of utilities for working with directories and files.
 *****************************************************************************/

#include "knudirs.h"
#include "knustring.h"
#include "knusort.h"  // for numBytesInFile
#include "types.h"

#include <dirent.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>

#define MAX_STR_LEN 1024

// The initial capacity of a dynamically sized array of strings
#define INITIAL_STRING_ARRAY_SIZE 16

/*************************************************************************
 * Return 1 if the given file exits, 0 otherwise
 */
int fileExists(char* fileName) {
    struct stat s;
    return (stat(fileName, &s) == 0);
}

/*************************************************************************
 * Return 1 if the given file is a directory, 0 otherwise.
 */
int isDirectory(char* fileName) {
    struct stat s;
    stat(fileName, &s);
    return S_ISDIR(s.st_mode);
}

/*************************************************************************
 * Find all of the file names in the given directory. The file names are
 * returned in the last argument (an unitialized pointer to an array of
 * strings). The number of file names is returned. The caller is responsible
 * for calling freeStringArray on the resulting string array.
 */
int allFilesInDir(char* dirName, char*** result) {
    DIR *dp;
    struct dirent *ep;
    
    // allocate string array
    int stringArrSize = INITIAL_STRING_ARRAY_SIZE;
    *result = createStringArray(stringArrSize, MAX_STR_LEN);

    // save each file name
    dp = opendir(dirName);
    int numEnts = 0;
    if (dp == NULL) {
        fprintf(stderr, "allFilesInDir couldn't open %s\n", dirName);
        perror ("error: ");
        exit(1);
    }  else {
        while ((ep = readdir(dp))) {
            if(stringArrSize == numEnts) {
                stringArrSize =
                    growStringArray(result, stringArrSize, MAX_STR_LEN);
            }
            strcpy((*result)[numEnts], ep->d_name);
            numEnts++;
        }
        closedir(dp);
    }

    // free unused strings
    int i;
    for (i = numEnts; i < stringArrSize; i++) {
        free((*result)[i]);
    }

    return numEnts;
}

/*************************************************************************
 * Get the full path name of all files in the given directory with the given
 * substring as part of the name. Names are passed back in the last argument,
 * number of file names is returned. The caller is responsible for calling
 * freeStringArray on the resulting string array.
 */
int getFileNamesWith(char* dir, char* subStr, char*** ans) {
    char** allFiles;
    int numAllFiles = allFilesInDir(dir, &allFiles);
    if (!numAllFiles) {
        freeStringArray(allFiles, numAllFiles);
        return 0;
    }
    char* subStrs[1];
    subStrs[0] = subStr;
    char** goodFiles;
    int numGoodFiles =
            filterStringArray(allFiles, numAllFiles, subStrs, 1, 1, &goodFiles);
    if (!numGoodFiles) {
        freeStringArray(allFiles, numAllFiles);
        free(goodFiles);
        return 0;
    }
    *ans = createStringArray(numGoodFiles, MAX_STR_LEN);
    int i;
    for (i=0; i<numGoodFiles; i++) {
        sprintf((*ans)[i], "%s/%s", dir, goodFiles[i]);
    }
    freeStringArray(allFiles, numAllFiles);
    free(goodFiles);
    return numGoodFiles;
}

/*************************************************************************
 * Recursively delete the given directory. Returns the total number of bytes
 * in all files deleted. No action if directory does not exist.
 */
uint64 recursivelyDeleteDir(char* dir) {
    if (!fileExists(dir)) {
        return 0;
    }

    uint64 bytesDel = 0;
    char** names;
    int numFiles = allFilesInDir(dir, &names);
    int i;
    char fn[MAX_STR_LEN];
    for(i=0; i<numFiles; i++) {
        if(strcmp(names[i], ".") != 0 && strcmp(names[i], "..") != 0) {
            sprintf(fn, "%s/%s", dir, names[i]);
            if(isDirectory(fn)) {
                bytesDel += recursivelyDeleteDir(fn);
            } else {
                bytesDel += numBytesInFile(fn);
                unlink(fn);
            }
        }
    }
    rmdir(dir);
    freeStringArray(names, numFiles);
    return bytesDel;
}
