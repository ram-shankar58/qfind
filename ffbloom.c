#include "qfind.h"
#include <stdlib.h>
#include <string.h>
#include <xxhash.h>

struct ffbloom_s {
    uint8_t* primary;
    uint8_t* secondary;
    size_t primary_size;     // In bytes
    size_t secondary_size;   // In bytes
    uint8_t num_hash_funcs;
};

/* Different hash functions for primary/secondary */
static uint64_t primary_hash(const void *data, size_t len, uint8_t idx) {
    return XXH3_64bits_withSeed(data, len, idx);
}

static uint64_t secondary_hash(const void *data, size_t len, uint8_t idx) {
    return XXH3_64bits_withSeed(data, len, idx + 0xA5A5A5A5);
}

ffbloom_t ffbloom_create(size_t primary_size, size_t secondary_size) {
    ffbloom_t bloom = malloc(sizeof(ffbloom_t));
    if (!bloom) return NULL;
    
    bloom->primary = calloc(1, primary_size);
    bloom->secondary = calloc(1, secondary_size);
    
    if (!bloom->primary || !bloom->secondary) {
        free(bloom->primary);
        free(bloom->secondary);
        free(bloom);
        return NULL;
    }
    
    bloom->primary_size = primary_size;
    bloom->secondary_size = secondary_size;
    bloom->num_hash_funcs = MAX_HASH_FUNCS;  // From qfind.h
    
    return bloom;
}

void ffbloom_destroy(ffbloom_t bloom) {
    if (!bloom) return;
    free(bloom->primary);
    free(bloom->secondary);
    free(bloom);
}

void ffbloom_add(ffbloom_t bloom, const void *data, size_t len) {
    for (uint8_t i = 0; i < bloom->num_hash_funcs; i++) {
        uint64_t hash = primary_hash(data, len, i) % (bloom->primary_size * 8);
        bloom->primary[hash / 8] |= (1 << (hash % 8));
    }
}

bool ffbloom_check(ffbloom_t bloom, const void *data, size_t len) {
    bool maybe_exists = false;
    
    for (uint8_t i = 0; i < bloom->num_hash_funcs; i++) {
        uint64_t hash = primary_hash(data, len, i) % (bloom->primary_size * 8);
        if (!(bloom->primary[hash / 8] & (1 << (hash % 8)))) {
            return false;
        }
        maybe_exists = true;
    }

    /* Feed-forward: Update secondary filter */
    if (maybe_exists) {
        for (uint8_t i = 0; i < bloom->num_hash_funcs; i++) {
            uint64_t hash = secondary_hash(data, len, i) % (bloom->secondary_size * 8);
            bloom->secondary[hash / 8] |= (1 << (hash % 8));
        }
    }
    
    return maybe_exists;
}

void ffbloom_get_candidates(const ffbloom_t bloom, 
                           trigram_t *patterns, 
                           uint32_t num_patterns,
                           trigram_t *output, 
                           uint32_t *num_found) {
    *num_found = 0;
    
    for (uint32_t i = 0; i < num_patterns; i++) {
        bool candidate = true;
        for (uint8_t j = 0; j < bloom->num_hash_funcs; j++) {
            uint64_t hash = secondary_hash(&patterns[i], sizeof(trigram_t), j);
            hash %= bloom->secondary_size * 8;
            
            if (!(bloom->secondary[hash / 8] & (1 << (hash % 8)))) {
                candidate = false;
                break;
            }
        }
        
        if (candidate && (*num_found < MAX_RESULTS)) {
            output[(*num_found)++] = patterns[i];
        }
    }
}

/* Update secondary filter with a single item */
void ffbloom_update_secondary(ffbloom_t bloom, const void *data, size_t len) {
    for (uint8_t i = 0; i < bloom->num_hash_funcs; i++) {
        uint64_t hash = secondary_hash(data, len, i) % (bloom->secondary_size * 8);
        bloom->secondary[hash / 8] |= (1 << (hash % 8));
    }
}

/* Check primary filter + auto-update secondary on hit */
bool ffbloom_check_and_update(ffbloom_t bloom, const void *data, size_t len) {
    bool maybe_exists = false;
    
    for (uint8_t i = 0; i < bloom->num_hash_funcs; i++) {
        uint64_t hash = primary_hash(data, len, i) % (bloom->primary_size * 8);
        if (!(bloom->primary[hash / 8] & (1 << (hash % 8)))) {
            return false;
        }
        maybe_exists = true;
    }

    if (maybe_exists) {
        ffbloom_update_secondary(bloom, data, len);
    }
    
    return maybe_exists;
}

