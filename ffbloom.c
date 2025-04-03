#include "qfind.h"
#include <stdlib.h>
#include <string.h>
#include <xxhash.h>

struct ffbloom_s {
    uint8_t* primary;
    uint8_t* secondary;
    size_t primary_size;
    size_t secondary_size;
    uint8_t num_hash_funcs;
};

static uint64_t primary_hash(const void *data, size_t len, uint8_t idx) {
    return XXH3_64bits_withSeed(data, len, idx);
}

static uint64_t secondary_hash(const void *data, size_t len, uint8_t idx) {
    return XXH3_64bits_withSeed(data, len, idx + 0xA5A5A5A5);
}

ffbloom_t ffbloom_create(size_t primary_size, size_t secondary_size) {
    struct ffbloom_s* bloom = malloc(sizeof(struct ffbloom_s));
    if (!bloom) return NULL;

    bloom->primary = calloc(1, primary_size);
    if (!bloom->primary) {
        free(bloom);
        return NULL;
    }

    bloom->secondary = calloc(1, secondary_size);
    if (!bloom->secondary) {
        free(bloom->primary);
        free(bloom);
        return NULL;
    }

    bloom->primary_size = primary_size;
    bloom->secondary_size = secondary_size;
    bloom->num_hash_funcs = MAX_HASH_FUNCS;
    
    return bloom;
}

void ffbloom_destroy(ffbloom_t bloom) {
    if (!bloom) return;
    free(bloom->primary);
    free(bloom->secondary);
    free(bloom);
}

void ffbloom_add(ffbloom_t bloom, const void *data, size_t len) {
    if (!bloom || !data) return;
    
    for (uint8_t i = 0; i < bloom->num_hash_funcs; i++) {
        uint64_t hash = primary_hash(data, len, i) % (bloom->primary_size * 8);
        bloom->primary[hash / 8] |= (1 << (hash % 8));
    }
}

bool ffbloom_check(const ffbloom_t bloom, const void *data, size_t len) {
    if (!bloom || !data) return false;
    
    for (uint8_t i = 0; i < bloom->num_hash_funcs; i++) {
        uint64_t hash = primary_hash(data, len, i) % (bloom->primary_size * 8);
        if (!(bloom->primary[hash / 8] & (1 << (hash % 8)))) {
            return false;
        }
    }
    return true;
}

void ffbloom_update_secondary(ffbloom_t bloom, const void *data, size_t len) {
    if (!bloom || !data) return;
    
    for (uint8_t i = 0; i < bloom->num_hash_funcs; i++) {
        uint64_t hash = secondary_hash(data, len, i) % (bloom->secondary_size * 8);
        bloom->secondary[hash / 8] |= (1 << (hash % 8));
    }
}

bool ffbloom_check_and_update(ffbloom_t bloom, const void *data, size_t len) {
    bool exists = ffbloom_check(bloom, data, len);
    if (exists) ffbloom_update_secondary(bloom, data, len);
    return exists;
}

void ffbloom_get_candidates(const ffbloom_t bloom, 
                            trigram_t *patterns, 
                            uint32_t num_patterns, 
                            trigram_t *output, 
                            uint32_t *num_found) {
    if (!bloom || !patterns || !output || !num_found) return;

    *num_found = 0; // Initialize the count of found candidates

    // Iterate over all input patterns (trigrams)
    for (uint32_t i = 0; i < num_patterns; i++) {
        bool candidate = true; // Assume the pattern is a candidate

        // Check each hash function's corresponding bit in the secondary Bloom filter
        for (uint8_t j = 0; j < bloom->num_hash_funcs; j++) {
            uint64_t hash = secondary_hash(&patterns[i], sizeof(trigram_t), j);
            uint64_t bit_offset = hash % (bloom->secondary_size * 8); // Map hash to bit array

            if (!(bloom->secondary[bit_offset / 8] & (1 << (bit_offset % 8)))) {
                // If any bit is not set, this pattern is definitely not a candidate
                candidate = false;
                break;
            }
        }

        // If all bits are set, add the pattern to the output array
        if (candidate && *num_found < MAX_RESULTS) {
            output[(*num_found)++] = patterns[i];
        }
    }
}