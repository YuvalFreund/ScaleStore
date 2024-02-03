//
// Created by YuvalFreund on 28.10.23.
//

#ifndef LOCALTHESIS_GENERICDISJOINTSETS_H
#define LOCALTHESIS_GENERICDISJOINTSETS_H

#include <map>
#include <cstdint>
#include <iostream>

#include "BucketManagerDefs.h"


using namespace std;


class BucketsDisjointSets {
    std::map<uint64_t,uint64_t> disjointMap;
    std::mutex mtxForUnionFind;
public:
    BucketsDisjointSets() = default;

    void makeInitialSet(const uint64_t values[]){
        mtxForUnionFind.lock();
        for(uint64_t i=0;i<BUCKETS_NUM_TO_INIT;i++) {
            disjointMap.insert(std::pair<uint64_t,uint64_t>(values[i], values[i]));
        }
        mtxForUnionFind.unlock();
    }

    void addToUnionFind(uint64_t x){
        mtxForUnionFind.lock();
        disjointMap.insert(std::pair<uint64_t,uint64_t>(x, x));
        mtxForUnionFind.unlock();
    }

    void addPairToUnionFind(uint64_t x, uint64_t y){
        mtxForUnionFind.lock();
        disjointMap.insert(std::pair<uint64_t,uint64_t>(x, y));
        mtxForUnionFind.unlock();
    }

    uint64_t find(uint64_t x){
        std::cout<<"in disjoint sets!";
        mtxForUnionFind.lock();
        if (disjointMap.find(x)->second != x) {
            disjointMap.find(x)->second = find(disjointMap.at(x));
        }
        mtxForUnionFind.unlock();
        return disjointMap.find(x)->second;
    }

    void Union(uint64_t big, uint64_t small){
        mtxForUnionFind.lock();
        auto it = disjointMap.find(small);
        if (it != disjointMap.end()){
            it->second = big;
        }
        mtxForUnionFind.unlock();
    }

    std::map<uint64_t,uint64_t>  getMap(){
        return  disjointMap;
    }
};
#endif //LOCALTHESIS_GENERICDISJOINTSETS_H
