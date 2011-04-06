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
 * knustring.h
 *
 * A set of string utilities.
 *****************************************************************************/

#ifndef _KNUSTRING_H
#define _KNUSTRING_H

char** createStringArray(int size, int maxStrLen);
void freeStringArray(char **array, int size);
int growStringArray(char ***array, int arraySize, int maxStrLen);
int filterStringArray(char** inArray, int size, char** subStrs, int numSub,
                         int matchAll, char*** result);
int matchesAny(char* str, char** substrs, int numSub);
int matchesAll(char* str, char** substrs, int numSub);

#endif
