//
// Created by YuvalFreund on 19.01.24.
//

#ifndef LOCALTHESIS_LOCALBUCKETSMERGEJOB_H
#define LOCALTHESIS_LOCALBUCKETSMERGEJOB_H

#include <cstdint>

struct LocalBucketsMergeJob{
    uint64_t bigBucket;
    uint64_t smallBucket;
    bool lastMerge = false;
    bool needMerge = false;

    LocalBucketsMergeJob(uint64_t bigBucket, uint64_t smallBucket) : bigBucket(bigBucket), smallBucket(smallBucket) {
    }

    LocalBucketsMergeJob(){

    }


};
#endif //LOCALTHESIS_LOCALBUCKETSMERGEJOB_H
