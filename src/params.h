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
 * params.h
 *
 * User defined paramters for Roomy.
 *****************************************************************************/

#ifndef _PARAMS_H
#define _PARAMS_H

#include "types.h"

extern uint64 RPAR_MB_RAM_PER_NODE;
extern uint64 RPAR_GB_DISK_PER_NODE;
extern uint64 RPAR_SHARED_DISK;
extern char* RPAR_DISK_DATA_PATH;

void parseParams(char* filename);

#endif
