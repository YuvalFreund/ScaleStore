//
// Created by YuvalFreund on 19.01.24.
//

#ifndef LOCALTHESIS_REMOTEBUCKETSHUFFLEJOB_H
#define LOCALTHESIS_REMOTEBUCKETSHUFFLEJOB_H

#include <cstdint>

struct RemoteBucketShuffleJob{
    uint64_t bucketId;
    uint64_t nodeId;

    RemoteBucketShuffleJob(uint64_t bucketId, uint64_t nodeId) : bucketId(
            bucketId), nodeId(nodeId){
    }
    RemoteBucketShuffleJob() {  }
};
#endif //LOCALTHESIS_REMOTEBUCKETSHUFFLEJOB_H
