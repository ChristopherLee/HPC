#include "Array.h"
#include "BitArray.h"
#include "HashTable.h"
#include "ReadBuffer.h"
#include "WriteBuffer.h"
#include "types.h"
#include "roomy.h"

#include <stdio.h>
#include <stdlib.h>
#include <sys/utsname.h>

int main(int argc, char **argv) {
    printf("Beginning basic test\n");
    fflush(stdout);
    Roomy_init(&argc, &argv);
    struct utsname nameStruct;
    uname(&nameStruct);
    printf("The rank of %s is %i\n", nameStruct.nodename, RGLO_MY_RANK);
    Roomy_finalize();
    return 0;
}
