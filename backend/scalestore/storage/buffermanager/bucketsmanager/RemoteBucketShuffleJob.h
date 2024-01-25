//
// Created by YuvalFreund on 19.01.24.
//

#ifndef LOCALTHESIS_REMOTEBUCKETSHUFFLEJOB_H
#define LOCALTHESIS_REMOTEBUCKETSHUFFLEJOB_H

#include <cstdint>

struct RemoteBucketShuffleJob{
    uint64_t bucketId;
    uint64_t nodeId;
    uint64_t ssdAddressStartAtReceivingNode;
    bool needShuffle;

    RemoteBucketShuffleJob(uint64_t bucketId, uint64_t nodeId, uint64_t ssdAddressStartAtReceivingNode) : bucketId(
            bucketId), nodeId(nodeId), ssdAddressStartAtReceivingNode(ssdAddressStartAtReceivingNode) {
        needShuffle = false;
    }
    RemoteBucketShuffleJob() {  }
};
#endif //LOCALTHESIS_REMOTEBUCKETSHUFFLEJOB_H
