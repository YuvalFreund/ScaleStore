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
#include "Message.h"
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
    queue<Message> * firstMessageQueue; //todo DFD
    queue<Message> * secondMessageQueue; //todo DFD
    queue<Message> * thirdMessageQueue; //todo DFD


    void handleIncomingMessage(Message msg);
    //node joining handlers

    void handleNewNodeJoinedEnter(Message msg);

    void handleNewHashingStateSynchronizedEnter(Message msg);

    void handleIncomingUnionFindDataEnter(Message msg);

    void handleUnionFindDataFinishedEnter(Message msg);

    void handleBucketAmountsDataEnter(Message msg);

    void handleBucketAmountsApprovedEnter(Message msg);
    //node leaving handlers

    void handleNodeLeftTheCLusterLeave(Message msg);

    void handleNewHashingStateSynchronizedLeave(Message msg);
    void handleUnionFindDataAmount(Message msg);
    void handleIncomingUnionFindDataLeave(Message msg);

    void handleUnionFindDataFinishedAllNodesLeave(Message msg);

    void handleBucketAmountsDataLeave(Message msg);


    void handleBucketAmountsApprovedLeave(Message msg);
    //sending buckets handlers

    void handleRequestToStartSendingBucket(Message msg);

    void handleApproveNewBucketReadyToReceive(Message msg);

    void handleFinishBucketReceive(Message msg);

    void handleNodeFinishedReceivingBuckets(Message msg);

    void handleBucketMovedToNewNode(Message msg);
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
    void addIncomingUnionFindData(Message msg) ;

    void gossipFinishedUnionFind(MessagesEnum msgEnum);

///////// Consensus Vector functions /////////

    bool markBitAndReturnAreAllNodesExcludingSelfTrue(const Message msg);

    bool markBitAndReturnAreAllNodesIncludingSelfTrue(const Message msg);
///////// buckets sending functions functions /////////

    void prepareOtherNodesForIncomingBuckets();

    void sendBucketToNode(RemoteBucketShuffleJob bucketShuffleJob);

    void gossipBucketMoved(uint64_t bucketId, uint64_t nodeId);


///////// Misc functions /////////

    void gossipMessage(Message msg);
    void checkMailbox();
    void sendMessage(Message msg);

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
