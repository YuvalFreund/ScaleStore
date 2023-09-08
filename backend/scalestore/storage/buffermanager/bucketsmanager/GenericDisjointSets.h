//
// Created by YuvalFreund on 20.03.23.
//

#ifndef RANGESATTEMPT_GENERICDISJOINTSETS_H
#define RANGESATTEMPT_GENERICDISJOINTSETS_H

#include <map>
#include <cstdint>
#define MAX_BUCKETS 100

using namespace std;


template < typename T >
class BucketsDisjointSets {
    std::map<T,T> disjointMap;

public:
    BucketsDisjointSets()= default;

    void makeInitialSet(const T values[]){
        for(T i=0;i<MAX_BUCKETS;i++) {
            disjointMap.insert(std::pair<uint64_t,uint64_t>(values[i], values[i]));
        }
    };
    T find(T x)
    {
        if (disjointMap.find(x)->second != x) {
            disjointMap.find(x)->second = find(disjointMap.at(x));
        }
        return disjointMap.find(x)->second;
    }

    void Union(T big, T small)
    {
        auto it = disjointMap.find(small);
        if (it != disjointMap.end())
            it->second = big;
    }
};
#endif //RANGESATTEMPT_GENERICDISJOINTSETS_H
