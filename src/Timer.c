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
 * Timer.h
 *
 * A simple stopwatch-like timer.
 *****************************************************************************/

#include "Timer.h"

#include <stdio.h>
#include <time.h>

/*************************************************************************
 * Start the given timer.
 */
void Timer_start(Timer* timer) {
    gettimeofday(&timer->start, NULL);
    timer->stop.tv_sec = 0;
    timer->stop.tv_usec = 0;
}

/*************************************************************************
 * Stop the given timer. Has no effect if there is no start time.
 */
void Timer_stop(Timer* timer) {
    gettimeofday(&timer->stop, NULL);
}

/*************************************************************************
 * If start has not been called: return 0.
 * If start has been called, but stop has not been called: return
 * the number of microseconds since Timer_start was called.
 * If start was called, followed by stop: return the number of
 * microseconds between the calls to start and stop.
 */
uint64 Timer_elapsed_usec(Timer* timer) {
    if (timer->start.tv_sec == 0) {
        return 0;
    } else if(timer->stop.tv_sec == 0) {
        struct timeval stop;
        gettimeofday(&stop, NULL);
        return ((stop.tv_sec - timer->start.tv_sec) * 1000000) + 
               ((stop.tv_usec - timer->start.tv_usec));
    } else {
        return ((timer->stop.tv_sec - timer->start.tv_sec) * 1000000) + 
               ((timer->stop.tv_usec - timer->start.tv_usec));
    }
}

/*************************************************************************
 * Print a human-readable representation of the current elapsed time (or a
 * given number of usec). The given char* must be pre-allocated by the caller.
 * If the generated string exceeds maxStringLen, only the first maxStringLen
 * characters are placed in the string. Returns the number of characters
 * printed (or would have been printed if truncation occurs).
 */
const uint64 DAY = 86400000000;
const uint64 HOUR = 3600000000;
const uint64 MINUTE = 60000000;
const uint64 SECOND =  1000000;
const uint64 MSECOND =    1000;
int Timer_printUsec(uint64 usec, char* string, int maxStringLen) {
    int numChars = 0;
    int charsLeft = maxStringLen;

    if (usec / DAY > 0) {
        int strLen = snprintf(&string[numChars], charsLeft, "%lli d, ", usec / DAY);
        numChars += strLen;
        charsLeft -= strLen;
        if (charsLeft < 0) charsLeft = 0;
        usec = usec % DAY;
    }

    if (usec / HOUR > 0) {
        int strLen =
            snprintf(&string[numChars], charsLeft, "%lli h, ", usec / HOUR);
        numChars += strLen;
        charsLeft -= strLen;
        if (charsLeft < 0) charsLeft = 0;
        usec = usec % HOUR;
    }

    if (usec / MINUTE > 0) {
        int strLen =
            snprintf(&string[numChars], charsLeft, "%lli m, ", usec / MINUTE);
        numChars += strLen;
        charsLeft -= strLen;
        if (charsLeft < 0) charsLeft = 0;
        usec = usec % MINUTE;
    }

    if (usec / SECOND > 0) {
        int strLen =
            snprintf(&string[numChars], charsLeft, "%lli s, ", usec / SECOND);
        numChars += strLen;
        charsLeft -= strLen;
        if (charsLeft < 0) charsLeft = 0;
        usec = usec % SECOND;
    }

    if (usec / MSECOND > 0) {
        int strLen =
            snprintf(&string[numChars], charsLeft, "%lli ms, ", usec / MSECOND);
        numChars += strLen;
        charsLeft -= strLen;
        if (charsLeft < 0) charsLeft = 0;
        usec = usec % MSECOND;
    }

    numChars += snprintf(&string[numChars], charsLeft, "%lli us", usec);

    return numChars;
}

int Timer_print(Timer* timer, char* string, int maxStringLen) {
    return Timer_printUsec(Timer_elapsed_usec(timer), string, maxStringLen);
}
