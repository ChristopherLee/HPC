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
 * params.c
 *
 * User defined paramters for Roomy.
 *****************************************************************************/

#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <assert.h>

#include "params.h"
#include "knustring.h"

#define MAX_PATH_LEN 1024

uint64 RPAR_MB_RAM_PER_NODE;
uint64 RPAR_GB_DISK_PER_NODE;
uint64 RPAR_SHARED_DISK;
char* RPAR_DISK_DATA_PATH;

/* Return 1 if the first non-space character of the given string is '#'. */
int isComment(char* str) {
    int i = 0;
    while (str[i] == ' ') {
        i++;
    }
    return str[i] == '#';
}

/* Return 1 if the given string is an empty line */
int isEmpty(char* str) {
    return str[0] == '\n';
}

/* Return 1 if the given string is a param line */
int isParamLine(char* str) {
    if (isComment(str)) {
        return 0;
    }
    return strstr(str, "PARAM") != NULL;
}

/* Read in all node names. DEPRICATED: Roomy does not need to know the names
 * of all nodes any more. The rank map is now created by sorting dynamically
 * queried hostnames, instead of matching them to a user supplied list.
 */
char** readNodeNames(char* machineFN) {
    FILE* f = fopen(machineFN, "r");
    int arraySize = 64;
    char** nameArray = createStringArray(arraySize, MAX_PATH_LEN);
    int numNames = 0;

    char line[MAX_PATH_LEN];
    char* ret = fgets(line, MAX_PATH_LEN, f);
    char* name;
    while(ret != NULL && !isComment(line) && !isEmpty(line)) {
        name = strtok(line, " \n");
        strcpy(nameArray[numNames], name);
        numNames++;
        if(numNames > arraySize)
            arraySize = growStringArray(&nameArray, arraySize, MAX_PATH_LEN);
        ret = fgets(line, MAX_PATH_LEN, f);
    }
    fclose(f);

    return nameArray;
}

/* Parse the given param file and set global variables */
void parseParams(char* filename) {
    FILE* f = fopen(filename, "r");
    if(f == NULL) {
        perror("File open error in parseParams: ");
        exit(1);
    }
    char line[MAX_PATH_LEN];
    char* ret = fgets(line, MAX_PATH_LEN, f);
    while(ret != NULL) {
        if(isParamLine(line)) {
            char* key = strtok(line, " \n");
            assert(strstr(key, "PARAM") != NULL);
            char* varName = strtok(NULL, " \n");
            char* val = strtok(NULL, " \n");
            if(strstr(varName, "MB_RAM_PER_NODE") != NULL) {
                RPAR_MB_RAM_PER_NODE = atoi(val);
            } else if(strstr(varName, "GB_DISK_PER_NODE") != NULL) {
                RPAR_GB_DISK_PER_NODE = atoi(val);
            } else if(strstr(varName, "SHARED_DISK") != NULL) {
                RPAR_SHARED_DISK = atoi(val);
            } else if(strstr(varName, "DISK_DATA_PATH") != NULL) {
                RPAR_DISK_DATA_PATH = malloc(MAX_PATH_LEN);
                strcpy(RPAR_DISK_DATA_PATH, val); 
            } else {
                fprintf(stderr, "Illegal Parameter: %s\n", varName);
                exit(1);
            }
        }
        ret = fgets(line, MAX_PATH_LEN, f);
    }
    fclose(f);
}
