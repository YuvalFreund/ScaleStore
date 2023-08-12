//
// Created by YuvalFreund on 10.03.23.
//

#include "Bucket.h"

struct BucketsCompare
{
    bool operator()(const Bucket& lhs, const Bucket& rhs)
    {
        return lhs.freeSlots.size() < rhs.freeSlots.size();
    }
};

static inline uint64_t FasterHash(uint64_t input) {
    uint64_t local_rand = input;
    uint64_t local_rand_hash = 8;
    local_rand_hash = 40343 * local_rand_hash + ((local_rand) & 0xFFFF);
    local_rand_hash = 40343 * local_rand_hash + ((local_rand >> 16) & 0xFFFF);
    local_rand_hash = 40343 * local_rand_hash + ((local_rand >> 32) & 0xFFFF);
    local_rand_hash = 40343 * local_rand_hash + (local_rand >> 48);
    local_rand_hash = 40343 * local_rand_hash;
    return local_rand_hash; // if 64 do not rotate
    // else:
    // return Rotr64(local_rand_hash, 56);
}

uint64_t tripleHash(uint64_t input){
    return FasterHash(FasterHash(FasterHash(input)));
}

