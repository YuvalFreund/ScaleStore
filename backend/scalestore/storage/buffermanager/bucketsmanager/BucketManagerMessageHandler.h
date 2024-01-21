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


    vector<BucketMessage> handleIncomingMessage(BucketMessage msg);
    //node joining handlers

    vector<BucketMessage> handleNewNodeJoinedEnter(BucketMessage msg);

    vector<BucketMessage> handleNewHashingStateSynchronizedEnter(BucketMessage msg);

    vector<BucketMessage> handleIncomingUnionFindDataEnter(BucketMessage msg);

    vector<BucketMessage> handleUnionFindDataFinishedEnter(BucketMessage msg);

    vector<BucketMessage> handleBucketAmountsDataEnter(BucketMessage msg);

    vector<BucketMessage> handleBucketAmountsApprovedEnter(BucketMessage msg);
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

    vector<BucketMessage> handleFinishBucketReceive(BucketMessage msg);

    vector<BucketMessage> handleNodeFinishedReceivingBuckets(BucketMessage msg);

    vector<BucketMessage> handleBucketMovedToNewNode(BucketMessage msg);
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
    vector<pair<uint64_t,uint64_t>>* prepareUnionFindData();
    vector<BucketMessage> addIncomingUnionFindData(BucketMessage msg) ;

    vector<BucketMessage> gossipFinishedUnionFind(MessagesEnum msgEnum);

///////// Consensus Vector functions /////////

    bool markBitAndReturnAreAllNodesExcludingSelfTrue(const BucketMessage msg);

    bool markBitAndReturnAreAllNodesIncludingSelfTrue(const BucketMessage msg);
///////// buckets sending functions functions /////////

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

    void breakDownUint64ToBytes(uint64_t input, uint8_t retVal[8]);
    void breakDownBucketIdToBytes(uint64_t input, uint8_t retVal[6]);
    uint64_t convertBytesBackToUint64(uint8_t input[8]);
    uint64_t convertBytesBackToBucketId(uint8_t input[6]);
};

#endif //LOCALTHESIS_BUCKETMANAGERMESSAGEHANDLER_H
