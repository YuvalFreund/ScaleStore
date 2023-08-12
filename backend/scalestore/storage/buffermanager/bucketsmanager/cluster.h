//
// Created by YuvalFreund on 10.03.23.
//

#ifndef RANGESATTEMPT_CLUSTER_H
#define RANGESATTEMPT_CLUSTER_H

#include <cstdint>
#include <atomic>
#include <iostream>
#include <thread>
#include <vector>
struct Cluster{
public:
    uint64_t nodeNum;
    std::atomic_int atomicBucketId =0;
    Cluster(){}
    uint64_t getNewBucketId(){
        atomicBucketId++;
        return atomicBucketId;
    }

};
#endif //RANGESATTEMPT_CLUSTER_H
