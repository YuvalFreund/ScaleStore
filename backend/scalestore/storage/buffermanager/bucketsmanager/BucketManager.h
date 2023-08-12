//
// Created by YuvalFreund on 09.03.23.
//

#ifndef RANGESATTEMPT_NODE_H
#define RANGESATTEMPT_NODE_H

//libraries
#include <queue>
#include <map>
#include <vector>
#include <mutex>
#include <set>
#include <bitset>

//files
#include "Bucket.h"
#include "tools.h"
#include "cluster.h"
#include "GenericDisjointSets.h"

//Macros
#define MAX_BUCKETS 4096
#define BUCKETS_NUM_TO_INIT 2048
#define SLOT_SIZE_IN_BYTE 268435456
#define RAND_MASK 0x0000000000000FFF
#define NUM_NODES 8
#define INVALID_FREE_SLOTS 1000000000
#define INVALID_NODE_ID 1000000000
#define BULK_BUCKET_AMOUNT 10
#define MAX_TRY_TO_ADD_TO_BUCKET 5
#define ZERO 0

struct BucketManager{
public:

    ///////////////////////////////////////////////////
    //////////////INNER VARIABLES /////////////////////
    ///////////////////////////////////////////////////
    //locks
    std::mutex newBucketMtx;

    //variables
    uint64_t bucketsNum = 0;
    uint64_t nodeId;
    uint64_t freeBucketIdIndex = 0;

    //data structures for bucketsManagement
    std::map<uint64_t,Bucket> bucketsMap; // bucket id to Bucket mapping
    BucketsDisjointSets<uint64_t> disjointSets; // disjoints sets of buckets id
    uint64_t bucketIds[MAX_BUCKETS]; // the ids of the available buckets in the node
    stack<uint64_t> bucketsFreeSSDSlots; // stack of possible starts for SSD slots
    bitset<MAX_BUCKETS> availableBucketsBitSet; // bit set to check if bucket is full

    //data structures for consistent hashing
    std::map<uint64_t, uint64_t> nodesRingLocationMap; // what location belongs to what node
    std::vector<uint64_t> nodeRingLocationsVector; // the location on the ring
    std::vector<uint64_t> nodeIdsInCluster;
    uint64_t nodeNum = NUM_NODES;
    uint64_t consistentHashingWeight = 10;

    //caching
    std::map<uint64_t, uint64_t> bucketIdToNodeCache;



    //node constructor
    BucketManager(uint64_t nodeId, std::vector<uint64_t> nodeIdsInput): nodeId(nodeId){
        fullNodeInit(nodeIdsInput);
    }

    ///////////////////////////////////////////////////
    //////////////NODE FUNCTIONALITIES/////////////////
    ///////////////////////////////////////////////////


    uint64_t addNewPage(){
        bool availableBucketFound = false;
        uint64_t retVal;
        int tryNum=1;
        while(availableBucketFound == false){
            uint64_t randIndex = rand() % bucketsNum;
            if(availableBucketsBitSet.test(randIndex)){
                availableBucketFound = true;
                uint64_t chosenBucketId = bucketIds[randIndex];
                auto bucketIter = bucketsMap.find(chosenBucketId);
                try{
                    retVal = bucketIter->second.requestNewPageId();
                    if(bucketIter->second.isBucketFull()){
                        availableBucketsBitSet.reset(randIndex);
                        tryNum++;
                    }
                }catch (const runtime_error& error){
                    availableBucketFound = false;
                    tryNum++;
                }
            }
            if(tryNum == MAX_TRY_TO_ADD_TO_BUCKET){
                // todo - how to lock here the whole system when adding a bucket
                createNewBucket(false,ZERO);
            }
        }
        return retVal;
    }

    void removePage(uint64_t pageId){
        uint64_t bucketId = pageId & BUCKET_ID_MASK;
        uint64_t realBucketId = disjointSets.find(bucketId);
        auto wantedBucket = bucketsMap.find(realBucketId);
        wantedBucket->second.removePageId(pageId);
        // for now we leave bucket there.. why to remove it anyway?
        if(wantedBucket->second.isBucketEmpty()){
            //removeBucket(realBucketId);
        }
    }

    uint64_t getPageSSDSlotInSelfNode(uint64_t pageId){
        uint64_t retVal;
        uint64_t bucketId = pageId & BUCKET_ID_MASK;
        uint64_t realBucketId = disjointSets.find(bucketId);
        retVal = bucketsMap.find(realBucketId)->second.getPageSSDSlotByPageId(pageId); // this is the actual mapping
        return retVal;
    }

    void getPageInCluster(uint64_t wantedPageId){
        uint64_t bucketIdOfWantedPage = wantedPageId & BUCKET_ID_MASK;
        uint64_t nodeIdOfWantedPage = getNodeIdOfBucket(bucketIdOfWantedPage);
        // check if node is not already hot
        if(nodeIdOfWantedPage == this->nodeId){
            uint64_t ssdSlotOfRequestedPage= getPageSSDSlotInSelfNode(wantedPageId);
            //read page or whatever
        }else{
            requestPageFromOtherNode(wantedPageId);
        }
    }

    uint64_t getNodeIdOfBucket(uint64_t bucketId){
        uint64_t res = INVALID_NODE_ID;
        auto pageInCacheIter = bucketIdToNodeCache.find(bucketId);
        if(pageInCacheIter != bucketIdToNodeCache.end()){
            return pageInCacheIter->second;
        }

        uint64_t l = 0;
        uint64_t r = nodeRingLocationsVector.size() - 1;
        // edge case for cyclic operation
        if(bucketId < nodeRingLocationsVector[l] || bucketId > nodeRingLocationsVector[r]) return nodesRingLocationMap[nodeRingLocationsVector[r]];
        // binary search
        while (l <= r) {
            uint64_t m = l + (r - l) / 2;
            if (nodeRingLocationsVector[m] <= bucketId && nodeRingLocationsVector[m + 1] > bucketId) {
                res = nodesRingLocationMap[nodeRingLocationsVector[r]];
                break;
            }
            if (nodeRingLocationsVector[m] < bucketId) {
                l = m + 1;
            } else{
                r = m - 1;
            }
        }
        bucketIdToNodeCache[bucketId] = res;
        return res;
    }

    ///////////////////////////////////////////////////
    //////////////BUCKETS MANAGEMENT///////////////////
    ///////////////////////////////////////////////////


    // we merge small bucket into the big bucket
    void mergeSmallBucketIntoBigBucket(uint64_t bigBucketId, uint64_t smallBucketId){
        auto bigBucket = bucketsMap.find(bigBucketId);
        auto smallBucket = bucketsMap.find(smallBucketId);
        std::cout << "Merging buckets! Big bucket:  " << bigBucketId << " Small bucket: " << smallBucketId << std::endl;

        bigBucket->second.mergeBucketIn(&smallBucket->second);
        disjointSets.Union(bigBucketId,smallBucketId);
        removeBucket(smallBucketId);
    }

    void removeBucket(uint64_t bucketId){
        std::cout << "Bucket removed! bucket id:  " << bucketId << std::endl;
        auto bucketToRemove = bucketsMap.find(bucketId);
        bucketsMap.erase(bucketToRemove);
    }

    void createNewBucket(bool isNewBucketIdNeeded, uint64_t givenBucketId){
        newBucketMtx.lock();
        uint64_t newBucketId;
        if(isNewBucketIdNeeded){
            bucketsFreeSSDSlots.pop();
            newBucketId = bucketIds[freeBucketIdIndex];
            newBucketId = newBucketId& BUCKET_ID_MASK;
            freeBucketIdIndex++;
        }else{
            newBucketId = givenBucketId;
        }
        uint64_t SSDSlotStart = bucketsFreeSSDSlots.top();

        // create new bucket
        bucketsMap.try_emplace(newBucketId, newBucketId, SSDSlotStart).first;
        bucketsNum++;
        newBucketMtx.unlock();
    }

    set<pair<uint64_t, uint64_t>> findMergableBuckets(vector<pair<uint64_t, uint64_t>> bucketsSizes){
        set<pair<uint64_t, uint64_t>> retVal;
        sort(bucketsSizes.rbegin(), bucketsSizes.rend());
        for(uint64_t i = 0; i<bucketsNum; i++){
            if(bucketsSizes[i].first == INVALID_FREE_SLOTS){
                break; // this mean this bucket was already matched, so there are no more possible matches
            }
            for(uint64_t j = i+1; j > bucketsNum; j++){
                if(bucketsSizes[j].first == INVALID_FREE_SLOTS){
                    continue; // this mean this bucket was already matched, therefore continue
                }
                if(bucketsSizes[i].first + bucketsSizes[j].first < MAX_PAGES){ // buckets can be merged
                    bucketsSizes[i].first = INVALID_FREE_SLOTS;
                    bucketsSizes[j].first = INVALID_FREE_SLOTS;
                    retVal.insert(make_pair(bucketsSizes[i].second,bucketsSizes[j].second));
                }
            }
        }
        return retVal;
    }

    ///////////////////////////////////////////////////
    ///////COMMUNICATIONS WITH OTHER BUCKETS///////////
    ///////////////////////////////////////////////////

    void nodeLeftOrJoinedCluster(bool nodeJoined, int leftOrJoinedNodeId){
        updateConsistentHashingData(nodeJoined,leftOrJoinedNodeId);
        auto mergableBucketsByNode = getBucketsIdsAndSizeToSendToNodes();
        for (auto & [key, value] : mergableBucketsByNode) {
            set<pair<uint64_t, uint64_t>> mergableBucketsForNode = findMergableBuckets(value);
            // todo here- first merge than move?
        }
    }

    void requestPageFromOtherNode(uint64_t wantedPageId){
        uint64_t nodeIdHoldingWantedPage = getNodeIdOfBucket(wantedPageId & BUCKET_ID_MASK);
        //TODO - ASK NODE FOR PAGE
    }

    void receiveListOfBucketsFromOtherNode(){
        //todo - recieve a list of buckets
        //todo - put each one in a new ssd location

    }

    void sendBucketToNode(){}

    void selfNodeLeavingCluster(){
        auto buckets = getBucketsIdsAndSizeToSendToNodes();
    }
    // this function returns a map from node id to a vector of pairs,
    // where each pair is a bucket id and his size (page num in bucket)
    std::map<uint64_t,std::vector<pair<uint64_t,uint64_t>>> getBucketsIdsAndSizeToSendToNodes(){
        std::map<uint64_t,std::vector<pair<uint64_t,uint64_t>>> retVal;
        uint64_t tempNodeId;
        for ( auto & [key, value] : bucketsMap){
            tempNodeId = getNodeIdOfBucket(value.getBucketId());
            retVal[tempNodeId].emplace_back(make_pair(value.getBucketId(),value.getPagesNumInBucket()));
        }
        return retVal;
    }

    ///////////////////////////////////////////////////
    /////////////////INIT FUNCTIONS////////////////////
    ///////////////////////////////////////////////////


    void fullNodeInit(const std::vector<uint64_t> nodeIdsInput){
        for(const unsigned long long & i : nodeIdsInput){
            nodeIdsInCluster.emplace_back(i);
        }
        initConsistentHashingInfo();
        initFreeListOfBucketIds();
        makeStackOfSSDSlotsForBuckets();
        disjointSets.makeInitialSet(bucketIds);
        initAllBuckets();
    }

    void initFreeListOfBucketIds(){
        uint64_t temp;
        uint64_t bucketIdToEnter;
        uint64_t iter = MAX_BUCKETS+1;
        for(int i = 0; i< MAX_BUCKETS; i++){
            iter++;
            temp = tripleHash(iter);
            while(getNodeIdOfBucket(temp & BUCKET_ID_MASK) != nodeId){
                temp = tripleHash(iter);
                iter++;
            }
            bucketIdToEnter = temp & BUCKET_ID_MASK;
            bucketIds[i]= bucketIdToEnter;
        }
    }

    void initConsistentHashingInfo(){
        for(uint64_t i = 0; i<nodeIdsInCluster.size(); i++){
            for(uint64_t j = 0; j<consistentHashingWeight; j++){
                nodesRingLocationMap[tripleHash(nodeIdsInCluster[i] * consistentHashingWeight + j) ] = nodeIdsInCluster[i];
            }
        }
        for(auto it = nodesRingLocationMap.begin(); it != nodesRingLocationMap.end(); ++it ) {
            nodeRingLocationsVector.push_back(it->first );
        }
        std::sort (nodeRingLocationsVector.begin(), nodeRingLocationsVector.end());
    }

    void makeStackOfSSDSlotsForBuckets() {
        for(int i = 0; i<MAX_BUCKETS; i++){
            bucketsFreeSSDSlots.push(i*SLOT_SIZE_IN_BYTE);
        }
    }

    void initAllBuckets(){
        for(int i = 0; i<BUCKETS_NUM_TO_INIT; i++){
            createNewBucket(true,ZERO);
            availableBucketsBitSet.set(i);
            if(i % 200 == 0){
                std::cout<<"finished " << i << " buckets"<<std::endl;
            }
        }
    }

    ///////////////////////////////////////////////////
    ///////////////GENERAL UTILS //////////////////////
    ///////////////////////////////////////////////////

    void updateConsistentHashingData(bool newNodeJoined,int nodeId){
        if(newNodeJoined){
            nodeNum++;
            nodeIdsInCluster.emplace_back(nodeId);
        }else{
            nodeNum--;
            nodeIdsInCluster.erase(std::remove(nodeIdsInCluster.begin(), nodeIdsInCluster.end(), nodeId), nodeIdsInCluster.end());
        }
        initConsistentHashingInfo();
    }

    void printNodeData() {
        std::cout << "BucketManager " << nodeId << " has " << bucketsMap.size() << " buckets. " << std::endl;
        std::cout<<endl;
        std::cout << "Printing buckets data: " << std::endl;
        for (auto &itr: bucketsMap) {
            itr.second.printBucketData();
        }
    }


};
#endif //RANGESATTEMPT_NODE_H
