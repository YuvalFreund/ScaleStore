//
// Created by YuvalFreund on 28.10.23.
//

#ifndef LOCALTHESIS_GENERICDISJOINTSETS_H
#define LOCALTHESIS_GENERICDISJOINTSETS_H

#include <map>
#include <cstdint>
#include <iostream>

#define MAX_BUCKETS 420
#define BUCKETS_NUM_TO_INIT 60

using namespace std;


class BucketsDisjointSets {
    std::map<uint64_t,uint64_t> disjointMap;

public:
    BucketsDisjointSets()= default;

    void makeInitialSet(const uint64_t values[]){
        for(uint64_t i=0;i<BUCKETS_NUM_TO_INIT;i++) {
            disjointMap.insert(std::pair<uint64_t,uint64_t>(values[i], values[i]));
        }
    }

    void addToUnionFind(uint64_t x){
        disjointMap.insert(std::pair<uint64_t,uint64_t>(x, x));
    }

    void addPairToUnionFind(uint64_t x, uint64_t y){
        disjointMap.insert(std::pair<uint64_t,uint64_t>(x, y));
    }

    uint64_t find(uint64_t x){
        if (disjointMap.find(x)->second != x) {
            disjointMap.find(x)->second = find(disjointMap.at(x));
        }
        return disjointMap.find(x)->second;
    }

    void Union(uint64_t big, uint64_t small){
        auto it = disjointMap.find(small);
        if (it != disjointMap.end()){
            it->second = big;
        }
    }

    void printUnionFindToPrompt(){
        for (auto const& x : disjointMap){
            std::cout << x.first  // string (key)
                      << ':'
                      << x.second // string's value
                      << std::endl;
        }
    }

    uint64_t getUnionFindSize(){
        return disjointMap.size();
    }

    std::map<uint64_t,uint64_t>  getMap(){
        return  disjointMap;
    }
};
#endif //LOCALTHESIS_GENERICDISJOINTSETS_H
