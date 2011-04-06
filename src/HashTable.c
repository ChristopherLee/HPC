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
 * HashTable.h
 *
 * An open addressing hash table. Uses linear probing and lazy deletion.
 *****************************************************************************/

#include "HashTable.h"

#include "Array.h"
#include "BitsArray.h"
#include "types.h"
#include "hash.h"

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

/*************************************************************************
                               GLOBALS
*************************************************************************/

/* "good" hash table sizes (primes w/ certain conditions)
 * list from: http://planetmath.org/encyclopedia/GoodHashTablePrimes.html
 */
int numGoodPrimes = 26;
int goodPrimes[26] = {53, 97, 193, 389, 769, 1543, 3079, 6151, 12289, 24593,
49157, 98317, 196613, 393241, 786433, 1572869, 3145739, 6291469, 12582917,
25165843, 50331653, 100663319, 201326611, 402653189, 805306457, 1610612741};

double DEFAULT_LOADFACTOR = 0.7;

#define MAX_STR_LEN 1024

#define OPEN 0
#define USED 1
#define DELETED 2

/*************************************************************************
 * Make a HashTable:
 *  keySize   - number of bytes in a key (must be > 0)
 *  valueSize - number of bytes in a value (can be 0)
 *  capacity  - initial number of slots (will be increased if table becomes
 *              too full)
 */
HashTable HashTable_make(int keySize, int valueSize, int capacity) {
    assert(keySize > 0);
    assert(valueSize >= 0);
    assert(capacity > 0);
    HashTable htb;
    htb.keySize = keySize;
    htb.valueSize = valueSize;
    htb.eltSize = keySize + valueSize;
    htb.capacity = HashTable_nextGoodCapacity(capacity);
    htb.size = 0;
    htb.nonOpenSlots = 0;
    htb.loadFactor = DEFAULT_LOADFACTOR;
    htb.array = Array_make(htb.eltSize, htb.capacity);
    htb.state = BitsArray_make(2, htb.capacity);
    htb.mapped = 0;
    htb.filename = NULL;

    return htb;
}

/*************************************************************************
 * Save the given HashTable to disk. Three files will be created, with the given
 * filename as a prefix: filename_meta, storing a small amount of meta-data;
 * filename_state, storing the state of each slot in the table; and filename_array,
 * storing the (key,value) pairs.
 */
void HashTable_fileSave(HashTable *htb, char* filename) {
    char meta_fn[MAX_STR_LEN];
    sprintf(meta_fn, "%s_meta", filename);
    HashTable_fileSaveMetaData(htb, meta_fn);

    char state_fn[MAX_STR_LEN];
    sprintf(state_fn, "%s_state", filename);
    BitsArray_fileSave(&htb->state, state_fn);

    char array_fn[MAX_STR_LEN];
    sprintf(array_fn, "%s_array", filename);
    Array_fileSave(&htb->array, array_fn);
}

/*************************************************************************
 * Create a hash table by loading on disk files. The given filename should
 * be the same given to a previous call to HashTable_fileSave().
 */
HashTable HashTable_makeFromFiles(char* filename) {
    HashTable htb;
    char meta_fn[MAX_STR_LEN];
    sprintf(meta_fn, "%s_meta", filename);
    HashTable_fileLoadMetaData(&htb, meta_fn);

    char state_fn[MAX_STR_LEN];
    sprintf(state_fn, "%s_state", filename);
    htb.state = BitsArray_make(2, htb.capacity);
    BitsArray_fileLoad(&htb.state, state_fn);

    char array_fn[MAX_STR_LEN];
    sprintf(array_fn, "%s_array", filename);
    htb.array = Array_make(htb.eltSize, htb.capacity);
    Array_fileLoad(&htb.array, array_fn);

    htb.mapped = 0;

    return htb;
}

/*************************************************************************
 * Create a hash table by mmap'ing on disk files. The given filename should
 * be the same given to a previous call to HashTable_fileSave().
 */
HashTable HashTable_makeMapped(char* filename) {
    HashTable htb;
    char meta_fn[MAX_STR_LEN];
    sprintf(meta_fn, "%s_meta", filename);
    HashTable_fileLoadMetaData(&htb, meta_fn);

    HashTable_mapArrays(&htb, filename);

    htb.mapped = 1;
    htb.filename = malloc(MAX_STR_LEN * sizeof(char));
    strcpy(htb.filename, filename);

    return htb;
}

/*************************************************************************
 * Delete the files created by HashTable_fileSave with the given filename.
 * Returns 0 on successful completion, and a negative integer on failure.
 */
int HashTable_removeFiles(char* filename) {
    int success = 0;
    char meta_fn[MAX_STR_LEN];
    sprintf(meta_fn, "%s_meta", filename);
    success += unlink(meta_fn);

    char state_fn[MAX_STR_LEN];
    sprintf(state_fn, "%s_state", filename);
    success += unlink(state_fn);

    char array_fn[MAX_STR_LEN];
    sprintf(array_fn, "%s_array", filename);
    success += unlink(array_fn);

    return success;
}

/*************************************************************************
 * Insert a (key,value) pair into the given HashTable. If the key already exists
 * in the table, its value is replaced with the given one.
 */
void HashTable_insert(HashTable *htb, void *key, void *value) {
    int state;
    int slot = HashTable_lookup(htb, key, &state);
    void *arraySlot = Array_get(&htb->array, slot);
    if (state != USED) {
        memcpy(arraySlot, key, htb->keySize);
        BitsArray_set(&htb->state, slot, USED);
        htb->size++;
    }
    if (state == OPEN) {
        htb->nonOpenSlots++;
    }
    memcpy(arraySlot + htb->keySize, value, htb->valueSize);

    if (((double)htb->nonOpenSlots) / htb->capacity > htb->loadFactor) {
        HashTable_doubleCapacity(htb);
    }
}

/*************************************************************************
 * Return a pointer to the value associated with the given key. If the key does
 * not exist in the table, NULL is returned. Also returns NULL if there are no
 * values (i.e., valueSize given to HashTable_make was 0).
 */
void* HashTable_get(HashTable *htb, void *key) {
    if (htb->valueSize == 0) {
        return NULL;
    }

    int state;
    int slot = HashTable_lookup(htb, key, &state);
    if (state == USED) {
        return Array_get(&htb->array, slot) + htb->keySize;
    } else {
        return NULL;
    }
}

/*************************************************************************
 * Return 1 if the HashTable contains the given key. Return 0 otherwise.
 */
int HashTable_hasKey(HashTable *htb, void *key) {
    int state;
    HashTable_lookup(htb, key, &state);
    if (state == USED) {
        return 1;
    } else {
        return 0;
    }
}

/*************************************************************************
 * Remove the given key from the table.
 */
void HashTable_delete(HashTable *htb, void *key) {
    int state;
    int slot = HashTable_lookup(htb, key, &state);
    if (state == USED) {
        BitsArray_set(&htb->state, slot, DELETED);
        htb->size--;
    }
}

/*************************************************************************
 * Return the current number of keys stored in the HashTable.
 */
int HashTable_size(HashTable *htb) {
    return htb->size;
}

/*************************************************************************
 * Return the current number of slot in the HashTable.
 */
int HashTable_capacity(HashTable *htb) {
    return htb->capacity;
}

/*************************************************************************
 * Return the current number of bytes being used to store the HashTable.
 */
int64 HashTable_bytesUsed(HashTable *htb) {
    // eltSize bytes for each slot, plus 2-bit meta-data each
    return htb->capacity * htb->eltSize + htb->capacity / 4;
}

/*************************************************************************
 * Destroy the given HashTable, freeing all assocated memory.
 */
void HashTable_destroy(HashTable *htb) {
    Array_destroy(&htb->array);
    BitsArray_destroy(&htb->state);

    // If using mmap for data, save meta-data to keep files consistent.
    if (htb->mapped) {
        char meta_fn[MAX_STR_LEN];
        sprintf(meta_fn, "%s_meta", htb->filename);
        HashTable_fileSaveMetaData(htb, meta_fn);
        free(htb->filename);
    }
}

/*************************************************************************
 * Apply the given function to each (key,value) pair in the HashTable.
 */
void HashTable_map(HashTable *htb, void (*mapFunc)(void *key, void *value)) {
    int i;
    for (i = 0; i < htb->capacity; i++) {
        if (BitsArray_get(&htb->state, i) == USED) {
            void *slot = Array_get(&htb->array, i);
            mapFunc(slot, slot + htb->keySize);
        }
    } 
}

/*************************************************************************
 * Use the given method to reduce all of the (key, value) pairs in the HashTable
 * into a single value.
 */
void HashTable_reduce(
        HashTable *htb,
        void* ansInOut,
        void (*mergeValAndAns)(void* ansInOut, void *key, void *value)) {
    int i;
    for (i = 0; i < htb->capacity; i++) {
        if (BitsArray_get(&htb->state, i) == USED) {
            void *slot = Array_get(&htb->array, i);
            mergeValAndAns(ansInOut, slot, slot + htb->keySize);
        }
    } 
}

/* Private methods */

/*************************************************************************
 * Save the meta-data of the HashTable to the given file.
 */
void HashTable_fileSaveMetaData(HashTable *htb, char* filename) {
    FILE* f = fopen(filename, "w");
    int success = 1;
    success = success && fwrite(&htb->keySize, sizeof(int), 1, f) == 1;
    success = success && fwrite(&htb->valueSize, sizeof(int), 1, f) == 1;
    success = success && fwrite(&htb->eltSize, sizeof(int), 1, f) == 1;
    success = success && fwrite(&htb->capacity, sizeof(int), 1, f) == 1;
    success = success && fwrite(&htb->size, sizeof(int), 1, f) == 1;
    success = success && fwrite(&htb->nonOpenSlots, sizeof(int), 1, f) == 1;
    success = success && fwrite(&htb->loadFactor, sizeof(double), 1, f) == 1;
    if (!success) {
        fprintf(stderr, "ERROR: HashTable_fileSaveMetaData failed write to %s\n",
                filename);
        exit(1);
    }
    fclose(f);
}

/*************************************************************************
 * Load the meta-data of the HashTable from the given file.
 */
void HashTable_fileLoadMetaData(HashTable *htb, char* filename) {
    FILE* f = fopen(filename, "r");
    int success = 1;
    success = success && fread(&htb->keySize, sizeof(int), 1, f) == 1;
    success = success && fread(&htb->valueSize, sizeof(int), 1, f) == 1;
    success = success && fread(&htb->eltSize, sizeof(int), 1, f) == 1;
    success = success && fread(&htb->capacity, sizeof(int), 1, f) == 1;
    success = success && fread(&htb->size, sizeof(int), 1, f) == 1;
    success = success && fread(&htb->nonOpenSlots, sizeof(int), 1, f) == 1;
    success = success && fread(&htb->loadFactor, sizeof(double), 1, f) == 1;
    if (!success) {
        fprintf(stderr, "ERROR: HashTable_fileLoadMetaData failed read from %s\n",
                filename);
        exit(1);
    }
    fclose(f);
}

/*************************************************************************
 * mmap the array and state for this HashTable from files with the given name.
 */
void HashTable_mapArrays(HashTable *htb, char *filename) {
    char state_fn[MAX_STR_LEN];
    sprintf(state_fn, "%s_state", filename);
    htb->state = BitsArray_makeMapped(2, htb->capacity, state_fn, 0);

    char array_fn[MAX_STR_LEN];
    sprintf(array_fn, "%s_array", filename);
    htb->array = Array_makeMapped(htb->eltSize, htb->capacity, array_fn, 0);
}

/*************************************************************************
 * Find the slot index corresponding to the given key. If the key exists in the
 * table, the returned value is the index of that element. Otherwise, the
 * chosen slot is the first open (or deleted) slot at or after the hash value of
 * the key.  Also performs delayed deletion: if the key exists in the table,
 * and a deleted slot is found during the lookup, the element is moved up to
 * the first deleted slot. The last argument is set to the state of the returned
 * slot number.
 */
int HashTable_lookup(HashTable *htb, void *key, int *state) {
    int hash = hsiehHash((char *)key, htb->keySize) % htb->capacity;
    int i = hash;
    int pos = -1;
    int first_delete_index = -1;
    int steps = 0;

    // find the first open slot, or used slot with a matching key
    while (pos == -1) {
        int s = BitsArray_get(&htb->state, i); 
        if (s == OPEN) {
            pos = i;
            *state = OPEN;
        } else if (s == USED) {
            if (memcmp(key, Array_get(&htb->array, i), htb->keySize) == 0) {
                pos = i;
                *state = USED;
            }
        } else { // s == DELETED
            if (first_delete_index == -1) {
                first_delete_index = i;
            }
        }
        i = (i + 1) % htb->capacity;
        steps++;
        if (steps > htb->capacity) {
            assert(first_delete_index != -1);
            *state = DELETED;
            break;
        }
    }

    if (first_delete_index != -1) {
        if (*state == USED) {
            // move matching element to first deleted slot
            Array_set(&htb->array, first_delete_index, Array_get(&htb->array, pos));
            BitsArray_set(&htb->state, first_delete_index, USED);
            BitsArray_set(&htb->state, pos, DELETED);
            pos = first_delete_index;
        } else { // *state == OPEN
            // not found, use deleted slot
            pos = first_delete_index;
            *state = DELETED;
        }
    }

    return pos;
}

/*************************************************************************
 * Return the first good prime larger than n.
 */
int HashTable_nextGoodCapacity(int n) {
    int i = 0;
    while(goodPrimes[i] < n) i++;
    return goodPrimes[i];
}

/*************************************************************************
 * Double the capacity of the given HashTable and rehash all of the elements.
 */
void HashTable_doubleCapacity(HashTable *htb) {
    Array old_array = htb->array;
    BitsArray old_state = htb->state;
    int old_capacity = htb->capacity;

    htb->capacity = HashTable_nextGoodCapacity(htb->capacity + 1);
    htb->array = Array_make(htb->eltSize, htb->capacity);
    htb->state = BitsArray_make(2, htb->capacity);
    htb->size = 0;
    htb->nonOpenSlots = 0;

    int i;
    for (i = 0; i < old_capacity; i++) {
        if (BitsArray_get(&old_state, i) == USED) {
            void *slot = Array_get(&old_array, i);
            HashTable_insert(htb, slot, slot + htb->keySize);
        }
    }

    Array_destroy(&old_array);
    BitsArray_destroy(&old_state);

    if (htb->mapped) {
        HashTable_fileSave(htb, htb->filename);
        Array_destroy(&htb->array);
        BitsArray_destroy(&htb->state);
        HashTable_mapArrays(htb, htb->filename);
    }
}
