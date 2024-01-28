//
// Created by YuvalFreund on 28.10.23.
//

#ifndef LOCALTHESIS_BUCKETMANAGER_H
#define LOCALTHESIS_BUCKETMANAGER_H
#include <queue>
#include <map>
#include <vector>
#include <mutex>
#include <set>
#include <bitset>
#include <algorithm>
#include <atomic>
#include <cstdlib>
#include <ctime>

//files
#include "Bucket.h"
#include "BucketsDisjointSets.h"
#include "MessagesEnum.h"
#include "LocalBucketsMergeJob.h"
#include "BucketManagerDefs.h"



struct BucketManager {


public:

    //////////////INNER VARIABLES /////////////////////
    ///////// locks /////////
    std::mutex bucketMapMtx;
    std::mutex bucketCacheMtx;

    ///////// variables /////////
    uint64_t bucketsNum = 0;
    uint64_t nodeId;
    uint64_t freeBucketIdIndex = 0;
    int64_t aggregatedBucketNum = 0;
    int64_t bucketsLeavingNum = 0;
    std::set<uint64_t> nodeIdsInCluster;
    uint32_t maxPagesByParameter;
    uint64_t bucketIdMaskByParameter;
    //caching
    std::map<uint64_t, uint64_t> bucketIdToNodeCache;


    ///////// data structures for bucketsManagement /////////

    std::map<uint64_t, Bucket> bucketsMap; // bucket id to Bucket mapping
    BucketsDisjointSets disjointSets; // disjoints sets of buckets id
    uint64_t bucketIds[MAX_BUCKETS]; // the ids of the available buckets in the node
    stack<uint64_t> bucketsFreeSSDSlots; // stack of possible starts for SSD slots
    bitset<MAX_BUCKETS> availableBucketsBitSet; // bit set to check if bucket is full

    ///////// data structures for consistent hashing /////////

    std::map<uint64_t, uint64_t> nodesRingLocationMap; // what location belongs to what node
    std::vector<uint64_t> nodeRingLocationsVector; // the location on the ring

    ///////// data structures for transitional state /////////

    // consistent hashing
    std::map<uint64_t, uint64_t> newNodesRingLocationMap; // locations to swap
    std::vector<uint64_t> newNodeRingLocationsVector; // the location on the ring

    // buckets amounts
    std::map<uint64_t, map<uint64_t, uint64_t>> mergableBucketsForEachNode;
    std::map<uint64_t, uint64_t> bucketAmountsToSendEachNodeAfterMerging;

    // disjoint sets
    BucketsDisjointSets newDisjointSets;

    //state changes
    enum ManagerState {initiated,normal, synchronizing, shuffling, finished}; //todo DFD - REMOVE FINSIHED
    std::atomic<ManagerState> managerState;
    bool nodeIsToBeDeleted;

    //node constructor
    BucketManager(uint64_t nodeId, std::vector<uint64_t> nodeIdsInput) : nodeId(nodeId) {
        std::srand (std::time (0));
        // todo yuval - read from flag
        initBucketSizeDataByParameter(6);
        managerState.store(normal);
        fullBucketManagerInit(nodeIdsInput);
        nodeIsToBeDeleted = false;
    }

    BucketManager() {}
    //////////////NODE FUNCTIONALITIES/////////////////

    uint64_t addNewPage();

    void removePage(uint64_t pageId);

    uint64_t getPageSSDSlotInSelfNode(uint64_t pageId);

    uint64_t getNodeIdOfPage(uint64_t pageId);

    uint64_t getNodeIdOfBucket(uint64_t bucketId,bool fromInitStage, bool forceNewState);

    //////////////BUCKETS MANAGEMENT///////////////////

    // we merge small bucket into the big bucket
    void mergeSmallBucketIntoBigBucket(LocalBucketsMergeJob localBucketMergeJob);

    uint64_t createNewBucket(bool isNewBucketIdNeeded, uint64_t givenBucketId);

    map<uint64_t, uint64_t> findMergableBuckets(vector<pair<uint64_t, uint64_t>> bucketsSizes, int * bucketsNumberNotMatched);

    void deleteBucket(uint64_t bucketId);

    ///////COMMUNICATIONS WITH OTHER BUCKETS///////////

    void nodeLeftOrJoinedCluster(bool nodeJoined, uint64_t leftOrJoinedNodeId);

    // this function returns a map from node id to a vector of pairs,
    // where each pair is a bucket id and his size (page num in bucket)
    std::map<uint64_t, std::vector<pair<uint64_t, uint64_t>>> getBucketsIdsAndSizeToSendToNodes();

    /////////////////INIT FUNCTIONS////////////////////

    void fullBucketManagerInit(const std::vector<uint64_t> nodeIdsInput);

    void initFreeListOfBucketIds();

    void initConsistentHashingInfo(bool firstInit);

    void makeStackOfSSDSlotsForBuckets();

    void initAllBuckets();

    ///////////////GENERAL UTILS //////////////////////

    void updateConsistentHashingData(bool newNodeJoined, uint64_t nodeIdChanged);

    void getOldConsistentHashingInfoForNewNode();

    void printNodeData();

    static inline uint64_t FasterHash(uint64_t input);

    uint64_t tripleHash(uint64_t input);

    // this is just for testing - this wil NOT be called
    uint64_t getNumberOfPagesInNode();


    void putBucketIdToUnionFind(uint64_t newBucketId);

    void putNewUnionFindPairData(uint64_t child, uint64_t root);

    void duplicateDisjointSetsAndMergeNew();

    bool updateRequestedBucketNumAndIsMergeNeeded(uint64_t newBucketsAmount);

    std::queue<LocalBucketsMergeJob> getJobsForMergingOwnBuckets();

    void atomicallyMoveToNormal();
    std::map<uint64_t, Bucket> ::iterator getIterToBucket(uint64_t bucketId);

    ///////////////shuffling functions //////////////////////

    vector<pair<uint64_t, uint64_t>> getBucketsShufflePrioritiesAndNodes();
    void initBucketSizeDataByParameter(int bucketIdByteSize);
    BucketsDisjointSets getDisjointSets();


};
#endif //LOCALTHESIS_BUCKETMANAGER_H
