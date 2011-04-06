/*
 * Test the maximum aggregate bandwidth in both remote and local I/O modes.
 */

#include <stdio.h>
#include <stdlib.h>
#include <sys/utsname.h>

#include "knudirs.h"
#include "roomy.h"
#include "Timer.h"
#include "WriteBuffer.h"

/*
uint64 bytesToWrite = 10737418240;  // 10 GB
uint64 numToWrite = 1342177280;     // 10 GB / 8 byte values
*/
uint64 bytesToWrite = 5368709120;  // 5 GB
uint64 numToWrite = 671088640;     // 5 GB / 8 byte values

WriteBuffer* buffers;

void openBuffers() {
    // make directory for data
    char dir[RGLO_STR_SIZE];
    sprintf(dir, "%sbandwidthTest/", RGLO_DATA_PATH);
    Roomy_destroyDir(dir);
    Roomy_makeDir(dir);

    // open one buffer to each node
    buffers = malloc(sizeof(WriteBuffer) * RGLO_NUM_SLAVES);
    char name[RGLO_STR_SIZE];
    int node;
    for(node=0; node<RGLO_NUM_SLAVES; node++) {
        sprintf(name, "%sbandwidthTest/from%ito%i",
                RGLO_DATA_PATH, RGLO_MY_RANK, node);
        buffers[node] = Roomy_makeWriteBuffer(
            RGLO_BUFFER_SIZE, sizeof(uint64), name, node);
    } 
}

void closeBuffers() {
    int node;
    for(node=0; node<RGLO_NUM_SLAVES; node++) {
        WriteBuffer_destroy(&(buffers[node]));
    }
    free(buffers);
}

// write bytesToWrite data to the local buffer for this node
void testLocalBW() {
    uint64 i;
    for (i = 0; i < numToWrite; i++) {
        WriteBuffer_write(&(buffers[RGLO_MY_RANK]), &i);
    }
}

// write bytesToWrite data to the buffer of a random node
void testRemoteBW() {
    uint64 i;
    for (i = 0; i < numToWrite; i++) {
        WriteBuffer_write(&(buffers[rand() % RGLO_NUM_SLAVES]), &i);
    }
}

int main(int argc, char **argv) {
    // to make sure we are the only ones using the machine, we start one process
    // for each core, but only one process per node runs the experiment. The
    // others just 
    char* raceFile = "/tmp/roomybandwidthrace";
    FILE* f = fopen(raceFile, "wx");  // 'x' means create if it doesn't exist
    if (!f) {
        while(1) {
            sleep(120);
        }
    } else {
        fclose(f);
    }

    // to clean up previous roomy runs on requin
    recursivelyDeleteDir("/tmp/roomy");

    uint64 MB = 1048576;
    Roomy_init(&argc, &argv);

    Roomy_log("Beginning bandwidth test\n");
    Timer timer;
    uint64 totalBytes = bytesToWrite * RGLO_NUM_SLAVES;
    uint64 usec;
    double sec, MBps;
    
    Roomy_log("Beginning LOCAL bandwidth test\n");
    Timer_start(&timer);
    openBuffers();
    testLocalBW();
    closeBuffers();
    Roomy_waitForRemoteWrites();
    Timer_stop(&timer);
    usec = Timer_elapsed_usec(&timer);
    sec = usec / 1000000;
    MBps = (totalBytes / MB) / sec;
    Roomy_log("LOCAL done. %i nodes, %lli total bytes, %f seconds\n",
              RGLO_NUM_SLAVES, totalBytes, sec);
    Roomy_log("LOCAL BW: %f MB/s\n", MBps);
    
    Roomy_log("Beginning REMOTE bandwidth test\n");
    Timer_start(&timer);
    openBuffers();
    testRemoteBW();
    closeBuffers();
    Roomy_waitForRemoteWrites();
    Timer_stop(&timer);
    usec = Timer_elapsed_usec(&timer);
    sec = usec / 1000000;
    MBps = (totalBytes / MB) / sec;
    Roomy_log("REMOTE done. %i nodes, %lli total bytes, %f seconds\n",
              RGLO_NUM_SLAVES, totalBytes, sec);
    Roomy_log("REMOTE BW: %f MB/s\n", MBps);

    char dir[RGLO_STR_SIZE];
    sprintf(dir, "%sbandwidthTest/", RGLO_DATA_PATH);
    Roomy_destroyDir(dir);

    unlink(raceFile);

    //Roomy_finalize();
    return 0;
}
