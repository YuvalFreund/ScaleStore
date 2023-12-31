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
#include "GenericDisjointSets.h"
#include "MessagesEnum.h"

//Macros
#define MAX_BUCKETS 420
#define BUCKETS_NUM_TO_INIT 60
#define SLOT_SIZE_IN_BYTE 65536
#define MAX_NUM_NODES 8
#define BUCKET_ALREADY_MERGED 1000000000
#define INVALID_NODE_ID 1000000000
#define MAX_TRY_TO_ADD_TO_BUCKET 5
#define ZERO 0
#define CONSISTENT_HASHING_WEIGHT 10



struct BucketManager {


public:

    //////////////INNER VARIABLES /////////////////////
    ///////// locks /////////
    std::mutex bucketManagerMtx; //todo yuval - how to get this lock without causing problem

    ///////// variables /////////
    uint64_t bucketsNum = 0;
    uint64_t nodeId;
    uint64_t freeBucketIdIndex = 0;
    uint64_t maxSlot;
    int64_t aggregatedBucketNum = 0;
    int64_t bucketsLeavingNum = 0;
    std::set<uint64_t> nodeIdsInCluster;

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
    std::map<uint64_t, uint64_t> newNodesRingLocationMap; // locations to swap YUYU
    std::vector<uint64_t> newNodeRingLocationsVector; // the location on the ring YUYU

    // buckets amounts
    std::map<uint64_t, map<uint64_t, uint64_t>> mergableBucketsForEachNode; // YUYU
    std::map<uint64_t, uint64_t> bucketAmountsToSendEachNodeAfterMerging;

    // disjoint sets
    BucketsDisjointSets newDisjointSets; // YUYU

    //state changes
    enum ManagerState {initiated,normal, synchronizing, shuffling, finished}; //todo DFD - REMOVE FINSIHED
    std::atomic<ManagerState> managerState;
    bool nodeIsToBeDeleted;

    //node constructor
    BucketManager(uint64_t nodeId, std::vector<uint64_t> nodeIdsInput) : nodeId(nodeId) {
        std::srand (std::time (0));

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

    uint64_t getNodeIdOfPage(PID pid);

    //////////////BUCKETS MANAGEMENT///////////////////

    // we merge small bucket into the big bucket
    void mergeSmallBucketIntoBigBucket(uint64_t bigBucketId, uint64_t smallBucketId);

    void createNewBucket(bool isNewBucketIdNeeded, uint64_t givenBucketId);

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

    BucketsDisjointSets getDisjointSets();

    void putBucketIdToUnionFind(uint64_t newBucketId);

    void putNewUnionFindPairData(uint64_t child, uint64_t root);

    void duplicateDisjointSetsAndMergeNew();

    bool updateAndCheckRequestedBucketNum(uint64_t newBucketsAmount);

    void mergeOwnBuckets();

    void atomicallyMoveToNormal();

    ///////////////shuffling functions //////////////////////

    vector<pair<uint64_t,uint64_t>>  getBucketsShufflePrioritiesAndNodes();


};
#endif //LOCALTHESIS_BUCKETMANAGER_H
