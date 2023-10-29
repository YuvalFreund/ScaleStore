
#include <map>
#include <cstdint>
#include <iostream>

#define MAX_BUCKETS 420

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

    void addToUnionFind(T x){
        disjointMap.insert(std::pair<uint64_t,uint64_t>(x, x));

    }

    T find(T x)
    {
        if (disjointMap.find(x)->second != x) {
            disjointMap.find(x)->second = find(disjointMap.at(x));
        }
        return disjointMap.find(x)->second;
    }

    void Union(T big, T small){
        auto it = disjointMap.find(small);
        if (it != disjointMap.end())
            it->second = big;
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

    std::map<T,T>  getMap(){
        return  disjointMap;
    }
};
