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
 * Timer.c
 *
 * A simple stopwatch-like timer.
 *****************************************************************************/

#ifndef _TIMER_H
#define _TIMER_H

#include <types.h>

#include <sys/time.h>
#include <time.h>

typedef struct {
   struct timeval start;
   struct timeval stop;
} Timer;

void Timer_start(Timer* timer);
void Timer_stop(Timer* timer);
uint64 Timer_elapsed_usec(Timer* timer);
int Timer_print(Timer* timer, char* string, int maxStringLen);
int Timer_printUsec(uint64 usec, char* string, int maxStringLen);

#endif
