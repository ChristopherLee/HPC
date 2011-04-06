AM_CPPFLAGS = -I$(top_srcdir)/src

LDADD = -lmpich -lpthread ../libroomy.a

noinst_PROGRAMS    = pancake pancakeList pancakeArray pancakeHashTable permute \
                     testRoomyBasic testPrimary testSort testFoundation \
                     bandwidth

EXTRA_DIST = machines params.in

pancake_SOURCES = pancake.c

pancakeList_SOURCES = pancakeList.c

pancakeArray_SOURCES = pancakeArray.c

pancakeHashTable_SOURCES = pancakeHashTable.c

permute_SOURCES = permute.c

testRoomyBasic_SOURCES = testRoomyBasic.c

testPrimary_SOURCES = testPrimary.c

testSort_SOURCES = testSort.c

testFoundation_SOURCES = testFoundation.c

bandwidth_SOURCES = bandwidth.c
