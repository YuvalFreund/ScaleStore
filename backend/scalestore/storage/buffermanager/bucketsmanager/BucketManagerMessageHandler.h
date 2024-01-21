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
    BucketManager* bucketManager;
    vector<uint64_t> nodeIdsForMessages;
    set<uint64_t> bucketsToReceiveFromNodes;
    int unionFindTotalAmount;
    bool bucketWereMerged;

public:
    BucketManagerMessageHandler(BucketManager* bucketManager){
        this->bucketManager = bucketManager;
        for (auto & element : this->bucketManager->nodeIdsInCluster) {
            nodeIdsForMessages.emplace_back(element);
        }
        unionFindTotalAmount = 0;
        bucketWereMerged = false;
    }
    std::mutex * firstMtx;//todo DFD
    std::mutex * secondMtx;//todo DFD
    std::mutex * thirdMtx;//todo DFD
    queue<BucketMessage> * firstMessageQueue; //todo DFD
    queue<BucketMessage> * secondMessageQueue; //todo DFD
    queue<BucketMessage> * thirdMessageQueue; //todo DFD


    void handleIncomingMessage(BucketMessage msg);
    //node joining handlers

    void handleNewNodeJoinedEnter(BucketMessage msg);

    void handleNewHashingStateSynchronizedEnter(BucketMessage msg);

    void handleIncomingUnionFindDataEnter(BucketMessage msg);

    void handleUnionFindDataFinishedEnter(BucketMessage msg);

    void handleBucketAmountsDataEnter(BucketMessage msg);

    void handleBucketAmountsApprovedEnter(BucketMessage msg);
    //node leaving handlers

    void handleNodeLeftTheCLusterLeave(BucketMessage msg);

    void handleNewHashingStateSynchronizedLeave(BucketMessage msg);
    void handleUnionFindDataAmount(BucketMessage msg);
    void handleIncomingUnionFindDataLeave(BucketMessage msg);

    void handleUnionFindDataFinishedAllNodesLeave(BucketMessage msg);

    void handleBucketAmountsDataLeave(BucketMessage msg);


    void handleBucketAmountsApprovedLeave(BucketMessage msg);
    //sending buckets handlers

    void handleRequestToStartSendingBucket(BucketMessage msg);

    void handleApproveNewBucketReadyToReceive(BucketMessage msg);

    void handleFinishBucketReceive(BucketMessage msg);

    void handleNodeFinishedReceivingBuckets(BucketMessage msg);

    void handleBucketMovedToNewNode(BucketMessage msg);
    ///////// Node joined / leaving functions /////////


    void gossipNodeJoined();

    void gossipNodeLeft();


///////// Buckets amounts functions /////////

    void spreadBucketAmountsToNodes(MessagesEnum msgEnum);

    void gossipBucketAmountFinishedEnter();

    void gossipBucketAmountFinishedLeave();
///////// Union Find functions /////////

    void gossipLocalUnionFindData(MessagesEnum msgEnum);
    void gossipUnionFindAmounts();
    vector<pair<uint64_t,uint64_t>>* prepareUnionFindData();
    void addIncomingUnionFindData(BucketMessage msg) ;

    void gossipFinishedUnionFind(MessagesEnum msgEnum);

///////// Consensus Vector functions /////////

    bool markBitAndReturnAreAllNodesExcludingSelfTrue(const BucketMessage msg);

    bool markBitAndReturnAreAllNodesIncludingSelfTrue(const BucketMessage msg);
///////// buckets sending functions functions /////////

    void prepareOtherNodesForIncomingBuckets();

    void sendBucketToNode(RemoteBucketShuffleJob bucketShuffleJob);

    void gossipBucketMoved(uint64_t bucketId, uint64_t nodeId);


///////// Misc functions /////////

    void gossipMessage(BucketMessage msg);
    void checkMailbox();
    void sendMessage(BucketMessage msg);

    void logActivity(string const str);

    void moveAtomicallyToNormalState();
    // utils

    void breakDownUint64ToBytes(uint64_t input, byte retVal[8]);
    void breakDownBucketIdToBytes(uint64_t input, byte retVal[6]);
    uint64_t convertBytesBackToUint64(byte input[8]);
    uint64_t convertBytesBackToBucketId(byte input[6]);

    void doStuff();
};

#endif //LOCALTHESIS_BUCKETMANAGERMESSAGEHANDLER_H
