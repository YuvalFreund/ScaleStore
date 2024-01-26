//
// Created by YuvalFreund on 29.11.23.
//

#ifndef LOCALTHESIS_BUCKETMANAGERMESSAGEHANDLER_H
#define LOCALTHESIS_BUCKETMANAGERMESSAGEHANDLER_H
#include <bitset>
#include <string>
#include <iostream>
#include <cstdio>
#include <ios>
#include <fstream>
#include <cmath>
#include "BucketManager.h"
#include "BucketMessage.h"
#include "RemoteBucketShuffleJob.h"
#include "LocalBucketsMergeJob.h"
#include "BucketManagerDefs.h"


struct BucketManagerMessageHandler{
    std::bitset<CONSENSUS_VEC_SIZE> consensusVec[MESSAGE_ENUM_AMOUNT];
    BucketManager& bucketManager;
    vector<uint64_t> nodeIdsForMessages;
    set<uint64_t> bucketsToReceiveFromNodes;
    int unionFindTotalAmount;
    std::queue<LocalBucketsMergeJob> localBucketsMergeJobQueue;
    std::queue<RemoteBucketShuffleJob> remoteBucketShufflingQueue;
    std::atomic<unsigned long> localMergeJobsCounter; // todo yuval - replace with an optimisitic lock!
    std::atomic<unsigned long> remoteShuffleJobsCounter; // todo yuval - replace with an optimisitic lock!
    std::map<uint64_t,queue<pair<uint64_t,uint64_t>>> unionFindDataForNodes;
    int unionFindDataArrived = 0;
    std::map<uint64_t,queue<uint64_t>> bucketShuffleDataForNodes;
    uint64_t leavingNode;


public:
    BucketManagerMessageHandler(BucketManager& bucketManager) : bucketManager(bucketManager){
        for (auto & element : this->bucketManager.nodeIdsInCluster) {
            nodeIdsForMessages.emplace_back(element);
        }
        unionFindTotalAmount = 0;
    }
    BucketManagerMessageHandler();
    std::mutex mtxForLocalJobsQueue;
    std::mutex mtxForShuffleJobsQueue;
    std::mutex * firstMtx;//todo DFD
    std::mutex * secondMtx;//todo DFD
    std::mutex * thirdMtx;//todo DFD
    queue<BucketMessage> * firstMessageQueue; //todo DFD
    queue<BucketMessage> * secondMessageQueue; //todo DFD
    queue<BucketMessage> * thirdMessageQueue; //todo DFD


    vector<BucketMessage> handleIncomingMessage(BucketMessage msg);
    //node leaving handlers

    vector<BucketMessage> handleNodeLeftTheClusterLeave(BucketMessage msg);

    vector<BucketMessage> handleNewHashingStateSynchronizedLeave(BucketMessage msg);
    vector<BucketMessage> handleUnionFindDataAmount(BucketMessage msg);
    vector<BucketMessage> handleIncomingUnionFindDataLeave(BucketMessage msg);

    vector<BucketMessage> handleUnionFindDataFinishedAllNodesLeave(BucketMessage msg);

    vector<BucketMessage> handleBucketAmountsDataLeave(BucketMessage msg);


    vector<BucketMessage> handleBucketAmountsApprovedLeave(BucketMessage msg);
    //sending buckets handlers

    vector<BucketMessage> handleRequestToStartSendingBucket(BucketMessage msg);

    vector<BucketMessage> handleApproveNewBucketReadyToReceive(BucketMessage msg);

    vector<BucketMessage> handleNodeFinishedReceivingBuckets(BucketMessage msg);

    vector<BucketMessage> handleBucketMovedToNewNode(BucketMessage msg);
    vector<BucketMessage> handleAddPageIdToBucket(BucketMessage msg);
    ///////// Node joined / leaving functions /////////


    vector<BucketMessage> gossipNodeJoined();

    vector<BucketMessage> gossipNodeLeft();


///////// Buckets amounts functions /////////

    vector<BucketMessage> prepareBucketAmountsToNodesMessages(MessagesEnum msgEnum);

    vector<BucketMessage> gossipBucketAmountFinishedEnter();

    vector<BucketMessage> gossipBucketAmountFinishedLeave();
///////// Union Find functions /////////

    vector<BucketMessage> gossipLocalUnionFindData(MessagesEnum msgEnum);
    vector<BucketMessage> prepareGossipUnionFindAmountsMessages();
    vector<BucketMessage> addIncomingUnionFindData(BucketMessage msg) ;
    vector<BucketMessage> handleUnionFindDataSendMore(BucketMessage msg);

    vector<BucketMessage> gossipFinishedUnionFind(MessagesEnum msgEnum);

///////// Consensus Vector functions /////////

    bool markBitAndReturnAreAllNodesExcludingSelfTrue(const BucketMessage msg);

    bool markBitAndReturnAreAllNodesIncludingSelfTrue(const BucketMessage msg);
///////// buckets sending functions functions /////////
    vector<BucketMessage> handleIncomingShuffledBucketData(BucketMessage msg);
    vector<BucketMessage> handleIncomingShuffledBucketDataSendMore(BucketMessage msg);
    vector<BucketMessage>handleIncomingShuffledBucketDataReceivedAll(BucketMessage msg);
    vector<BucketMessage> prepareOtherNodesForIncomingBuckets();

    void sendBucketToNode(RemoteBucketShuffleJob bucketShuffleJob);

    vector<BucketMessage> gossipBucketMoved(uint64_t bucketId, uint64_t nodeId);


///////// Misc functions /////////

    vector<BucketMessage> collectMessagesToGossip(BucketMessage msg);
    void checkMailbox();
    void sendMessage(BucketMessage msg);

    void logActivity(string const str);

    void moveAtomicallyToNormalState();
    // utils

    static void breakDownUint64ToBytes(uint64_t input, uint8_t retVal[8]);
    static void breakDownBucketIdToBytes(uint64_t input, uint8_t retVal[6]);
    uint64_t convertBytesBackToUint64(uint8_t input[8]);
    uint64_t convertBytesBackToBucketId(uint8_t input[6]);
    BucketMessage prepareNextUnionFindDataToSendToNode(uint64_t nodeId);

    LocalBucketsMergeJob getMergeJob();
    RemoteBucketShuffleJob getShuffleJob();
    void prepareIncomingBucketsDataForOtherNodes();

};

#endif //LOCALTHESIS_BUCKETMANAGERMESSAGEHANDLER_H
