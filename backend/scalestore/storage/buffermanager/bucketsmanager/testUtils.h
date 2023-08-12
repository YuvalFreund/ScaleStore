//
// Created by YuvalFreund on 05.05.23.
//

#ifndef BUCKETS_TESTUTILS_H
#define BUCKETS_TESTUTILS_H

#include "BucketManager.h"

void insertPagesToNode(BucketManager* node, int* numPages){
    for(int i = 0; i<*numPages; i++){
        node->addNewPage();
    }
}

uint64_t getIndexOfRingPosition(uint64_t toFind, std::vector<uint64_t> locationsVec){
    uint64_t l = 0;
    uint64_t r = locationsVec.size()-1;
    // edge case for cyclic operation
    if(toFind < locationsVec[l] || toFind > locationsVec[r]) return locationsVec[r];
    uint64_t m;
    while (l <= r && r <  locationsVec.size()) {
        m = l + ((r - l) / 2);
        if (locationsVec[m] <= toFind && locationsVec[m + 1] > toFind) {
            uint64_t res = locationsVec[m];
            return res;
        }
        if (locationsVec[m] < toFind) {
            l = m + 1;
        } else{
            r = m - 1;
        }
    }
    return locationsVec[m];
}
#endif //BUCKETS_TESTUTILS_H
