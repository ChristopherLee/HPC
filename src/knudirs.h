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
 * knudirs.h
 *
 * A set of utilities for working with directories and files.
 *****************************************************************************/

#ifndef _KNUDIRS_H
#define _KNUDIRS_H

#include "types.h"

int fileExists(char* fileName);
int isDirectory(char* fileName);
int allFilesInDir(char* dirName, char*** result);
int getFileNamesWith(char* dir, char* subStr, char*** ans);
uint64 recursivelyDeleteDir(char* dir);

#endif
