
#include "BucketManagerMessageHandler.h"

vector<BucketMessage> BucketManagerMessageHandler::handleIncomingMessage(BucketMessage msg){
    vector<BucketMessage> retVal;
    string logMsg = "Node " + std::to_string(bucketManager->nodeId) + " log: " + "handleIncomingMessage. \n" ;//todo DFD
    logActivity(logMsg);
    switch(msg.messageEnum){
        // node joining

        case NODE_JOINED_THE_CLUSTER_ENTER:
            retVal = handleNewNodeJoinedEnter(msg);
            break;

        case CONSISTENT_HASHING_INFORMATION_SYNCED_ENTER:
            retVal = handleNewHashingStateSynchronizedEnter(msg);
            break;

        case UNION_FIND_DATA_ENTER:
            retVal = handleIncomingUnionFindDataEnter(msg);
            break;

        case UNION_FIND_DATA_FINISHED_ENTER:
            retVal = handleUnionFindDataFinishedEnter(msg);
            break;

        case BUCKET_AMOUNTS_DATA_ENTER:
            retVal = handleBucketAmountsDataEnter(msg);
            break;

        case BUCKETS_AMOUNTS_APPROVED_ENTER:
            retVal = handleBucketAmountsApprovedEnter(msg);
            break;
            // node leaving

        case NODE_LEAVING_THE_CLUSTER_LEAVE:
            retVal = handleNodeLeftTheClusterLeave(msg);
            break;

        case CONSISTENT_HASHING_INFORMATION_SYNCED_LEAVE:
            retVal = handleNewHashingStateSynchronizedLeave(msg);
            break;

        case UNION_FIND_BUCKETS_AMOUNT:
            retVal = handleUnionFindDataAmount(msg);
            break;

        case UNION_FIND_DATA_LEAVE:
            retVal = handleIncomingUnionFindDataLeave(msg);
            break;

         case UNION_FIND_NODE_RECEIVED_ALL_LEAVE:
            retVal = handleUnionFindDataFinishedAllNodesLeave(msg);
            break;

        case BUCKET_AMOUNTS_DATA_LEAVE:
            retVal = handleBucketAmountsDataLeave(msg);
            break;

        case BUCKETS_AMOUNTS_APPROVED_LEAVE:
            handleBucketAmountsApprovedLeave(msg);
            break;

            // buckets shuffling messages

        case REQUEST_TO_START_BUCKET_SEND:
            retVal = handleRequestToStartSendingBucket(msg);
            break;

        case APPROVE_NEW_BUCKET_READY_TO_RECEIVE:
            retVal = handleApproveNewBucketReadyToReceive(msg);
            break;
        case FINISHED_BUCKET:
            retVal = handleFinishBucketReceive(msg);
            break;
        case BUCKET_MOVED_TO_NEW_NODE:
            retVal = handleBucketMovedToNewNode(msg);
            break;

        case NODE_FINISHED_RECEIVING_BUCKETS:
            retVal =  handleNodeFinishedReceivingBuckets(msg);
            break;
    }
    return retVal;
}

//node joining handlers

vector<BucketMessage> BucketManagerMessageHandler::handleNewNodeJoinedEnter(BucketMessage msg){
    string logMsg = "Node " + std::to_string(bucketManager->nodeId) + " log: " + "handleNewNodeJoinedEnter. \n" ;//todo DFD
    logActivity(logMsg);
    vector<BucketMessage> retVal;

    auto newNodeId = (uint64_t) msg.messageData[MSG_DATA_START_IDX];
    bucketManager->nodeLeftOrJoinedCluster(true,newNodeId);
    nodeIdsForMessages.emplace_back(newNodeId);
    uint8_t messageData[MESSAGE_SIZE];
    messageData[MSG_ENUM_IDX] = (uint8_t) CONSISTENT_HASHING_INFORMATION_SYNCED_ENTER;
    messageData[MSG_SND_IDX] = (uint8_t) bucketManager->nodeId;
    auto consistentHashingCompleted = BucketMessage(messageData);
    retVal = collectMessagesToGossip(consistentHashingCompleted);

    return retVal;
}

vector<BucketMessage> BucketManagerMessageHandler::handleNewHashingStateSynchronizedEnter(BucketMessage msg){
    string logMsg = "Node " + std::to_string(bucketManager->nodeId) + " log: " + "handleNewHashingStateSynchronizedEnter. \n" ;//todo DFD
    logActivity(logMsg);
    vector<BucketMessage> retVal;

    bool allNodesUpdated = markBitAndReturnAreAllNodesExcludingSelfTrue(msg);
    if(allNodesUpdated){
        retVal = gossipLocalUnionFindData(UNION_FIND_DATA_ENTER);
    }
    return retVal;

}

vector<BucketMessage> BucketManagerMessageHandler::handleIncomingUnionFindDataEnter(BucketMessage msg){
    string logMsg = "Node " + std::to_string(bucketManager->nodeId) + " log: " + "handleIncomingUnionFindDataEnter. \n" ;//todo DFD
    logActivity(logMsg);
    vector<BucketMessage> retVal;

    addIncomingUnionFindData(msg);
    bool allNodesUpdated = markBitAndReturnAreAllNodesExcludingSelfTrue(msg);
    if(allNodesUpdated){
        retVal = gossipFinishedUnionFind(UNION_FIND_DATA_FINISHED_ENTER);
    }
    return retVal;

}

vector<BucketMessage> BucketManagerMessageHandler::handleUnionFindDataFinishedEnter(BucketMessage msg){
    string logMsg = "Node " + std::to_string(bucketManager->nodeId) + " log: " + "handleUnionFindDataFinishedEnter. \n" ;//todo DFD
    logActivity(logMsg);
    vector<BucketMessage> retVal;

    bool allNodesUpdated = markBitAndReturnAreAllNodesExcludingSelfTrue(msg);
    if(allNodesUpdated){
        retVal = prepareBucketAmountsToNodesMessages(BUCKET_AMOUNTS_DATA_ENTER);
    }
    return retVal;

}

vector<BucketMessage> BucketManagerMessageHandler::handleBucketAmountsDataEnter(BucketMessage msg){
    string logMsg = "Node " + std::to_string(bucketManager->nodeId) + " log: " + "handleBucketAmountsDataEnter. \n" ;//todo DFD
    logActivity(logMsg);
    vector<BucketMessage> retVal;

    bucketManager->updateRequestedBucketNumAndIsMergeNeeded((uint64_t) msg.messageData[MSG_DATA_START_IDX]);
    bool allNodesUpdated = markBitAndReturnAreAllNodesExcludingSelfTrue(msg);
    if(allNodesUpdated){
        retVal = gossipBucketAmountFinishedEnter();
    }
    return retVal;

}

vector<BucketMessage> BucketManagerMessageHandler::handleBucketAmountsApprovedEnter(BucketMessage msg){
    string logMsg = "Node " + std::to_string(bucketManager->nodeId) + " log: " + "handleBucketAmountsApprovedEnter. \n" ;//todo DFD
    logActivity(logMsg);
    vector<BucketMessage> retVal;

    bool allNodesUpdated = markBitAndReturnAreAllNodesExcludingSelfTrue(msg);
    if(allNodesUpdated){
        string finished = "Node " + std::to_string(bucketManager->nodeId) + " log: " + "finished synchronizing stage!. \n" ;//todo DFD
        logActivity(finished);
        retVal = prepareOtherNodesForIncomingBuckets();
    }
    return retVal;

}

//node leaving handlers

vector<BucketMessage> BucketManagerMessageHandler::handleNodeLeftTheClusterLeave(BucketMessage msg){
    string logMsg = "Node " + std::to_string(bucketManager->nodeId) + " log: " + "handleNodeLeftTheClusterLeave. \n" ;//todo DFD
    logActivity(logMsg);
    vector<BucketMessage> retVal;

    auto leavingNodeId = (uint64_t) msg.messageData[MSG_DATA_START_IDX];
    bucketManager->nodeLeftOrJoinedCluster(false,leavingNodeId);

    uint8_t messageData[MESSAGE_SIZE];
    messageData[MSG_ENUM_IDX] = (uint8_t) CONSISTENT_HASHING_INFORMATION_SYNCED_LEAVE;
    messageData[MSG_SND_IDX] = (uint8_t) bucketManager->nodeId;
    auto consistentHashingCompleted = BucketMessage(messageData);
    retVal = collectMessagesToGossip(consistentHashingCompleted);
    return retVal;

}

vector<BucketMessage> BucketManagerMessageHandler::handleNewHashingStateSynchronizedLeave(BucketMessage msg){
    string logMsg = "Node " + std::to_string(bucketManager->nodeId) + " log: " + "handleNewHashingStateSynchronizedLeave. \n" ;//todo DFD
    logActivity(logMsg);
    vector<BucketMessage> retVal;

    bool allNodesUpdated = markBitAndReturnAreAllNodesExcludingSelfTrue(msg);
    if(allNodesUpdated){
        retVal = prepareGossipUnionFindAmountsMessages();
        vector<BucketMessage> unionFindMessages = gossipLocalUnionFindData(UNION_FIND_DATA_LEAVE);
        retVal.insert( retVal.end(), unionFindMessages.begin(), unionFindMessages.end() );
    }
    return retVal;

}

vector<BucketMessage> BucketManagerMessageHandler::handleIncomingUnionFindDataLeave(BucketMessage msg){
    // if this is the leaving bucket, there is no need for to add the union find data to it - it will be deleted
    string logMsg = "Node " + std::to_string(bucketManager->nodeId) + " log: " + "handleIncomingUnionFindDataLeave. \n" ;//todo DFD
    logActivity(logMsg);
    vector<BucketMessage> retVal;

    if(bucketManager -> nodeIsToBeDeleted == false){
        retVal = addIncomingUnionFindData(msg);
    }
    return retVal;

}

vector<BucketMessage> BucketManagerMessageHandler::handleUnionFindDataAmount(BucketMessage msg){
    string logMsg = "Node " + std::to_string(bucketManager->nodeId) + " log: " + "handleUnionFindDataAmount. \n" ;//todo DFD
    logActivity(logMsg);
    vector<BucketMessage> retVal;

    uint64_t amount = convertBytesBackToUint64(&msg.messageData[MSG_DATA_START_IDX]);
    this->unionFindTotalAmount += (int) amount;
    return retVal;

}

vector<BucketMessage> BucketManagerMessageHandler::handleUnionFindDataFinishedAllNodesLeave(BucketMessage msg){
    string logMsg = "Node " + std::to_string(bucketManager->nodeId) + " log: " + "handleUnionFindDataFinishedAllNodesLeave. \n" ;//todo DFD
    logActivity(logMsg);
    vector<BucketMessage> retVal;

    bool allNodesUpdated = markBitAndReturnAreAllNodesExcludingSelfTrue(msg);
    if(allNodesUpdated){
        retVal = prepareBucketAmountsToNodesMessages(BUCKET_AMOUNTS_DATA_LEAVE);
    }
    return retVal;

}

vector<BucketMessage> BucketManagerMessageHandler::handleBucketAmountsDataLeave(BucketMessage msg){
    string logMsg = "Node " + std::to_string(bucketManager->nodeId) + " log: " + "handleBucketAmountsDataLeave. \n" ;//todo DFD
    logActivity(logMsg);
    vector<BucketMessage> retVal;

    bool isMergeNeeded = bucketManager->updateRequestedBucketNumAndIsMergeNeeded(
            (uint64_t) msg.messageData[MSG_DATA_START_IDX]);
    if (isMergeNeeded && bucketWereMerged == false){
        // todo yuval - add this to the
        // todo yuval - consider with atomic
        // todo yuval - when mergign is done, send message
        bucketWereMerged = true;
        bucketManager-> mergeOwnBuckets();
    }
    bool allNodesUpdated = markBitAndReturnAreAllNodesExcludingSelfTrue(msg);
    if(allNodesUpdated){
        retVal = gossipBucketAmountFinishedLeave();
    }
    return retVal;

}

vector<BucketMessage> BucketManagerMessageHandler::handleBucketAmountsApprovedLeave(BucketMessage msg){
    string logMsg = "Node " + std::to_string(bucketManager->nodeId) + " log: " + "handleBucketAmountsApprovedLeave. \n" ;//todo DFD
    logActivity(logMsg);
    vector<BucketMessage> retVal;

    bool allNodesUpdated = markBitAndReturnAreAllNodesExcludingSelfTrue(msg);
    if(allNodesUpdated){
        string finished = "Node " + std::to_string(bucketManager->nodeId) + " log: " + "finished synchronizing stage!. \n" ;//todo DFD
        logActivity(finished);
        retVal = prepareOtherNodesForIncomingBuckets();
    }
    return retVal;

}

//sending buckets handlers

vector<BucketMessage> BucketManagerMessageHandler::handleRequestToStartSendingBucket(BucketMessage msg){
    uint64_t newBucketId = convertBytesBackToUint64(&msg.messageData[BUCKET_ID_START_INDEX]);
    string logMsg = "Node " + std::to_string(bucketManager->nodeId) + " log: " + "handleBeginNewBucket. Bucket id:" +std::to_string(newBucketId)+ " \n" ;//todo DFD
    logActivity(logMsg);
    vector<BucketMessage> retVal;

    uint64_t ssdStartingAddressForNewNode = bucketManager->createNewBucket(false, newBucketId);
    bucketsToReceiveFromNodes.insert(newBucketId);
    uint8_t messageData[MESSAGE_SIZE];
    messageData[MSG_ENUM_IDX] = (uint8_t) APPROVE_NEW_BUCKET_READY_TO_RECEIVE;
    messageData[MSG_SND_IDX] = (uint8_t) bucketManager->nodeId;
    messageData[MSG_RCV_IDX] = msg.messageData[MSG_SND_IDX];
    breakDownUint64ToBytes(newBucketId,&messageData[BUCKET_ID_START_INDEX]);
    breakDownUint64ToBytes(ssdStartingAddressForNewNode,&messageData[SSD_SLOT_START_INDEX]);
    auto approveBucketMsg = BucketMessage(messageData);
    sendMessage(approveBucketMsg); // todo - DFD
    retVal.emplace_back(approveBucketMsg);
    return retVal;

}

vector<BucketMessage> BucketManagerMessageHandler::handleApproveNewBucketReadyToReceive(BucketMessage msg) {
    string logMsg = "Node " + std::to_string(bucketManager->nodeId) + " log: " + "handleApproveNewBucketReadyToReceive. \n" ;//todo DFD
    logActivity(logMsg);
    vector<BucketMessage> retVal;

    uint64_t bucketToSend = convertBytesBackToUint64(&msg.messageData[BUCKET_ID_START_INDEX]);
    uint64_t newNodeSsdStartingAddress = convertBytesBackToUint64(&msg.messageData[SSD_SLOT_START_INDEX]);
    auto receivingNode = (uint64_t) msg.messageData[MSG_SND_IDX];
    RemoteBucketShuffleJob remoteBucketShuffleJob = RemoteBucketShuffleJob(bucketToSend,receivingNode,newNodeSsdStartingAddress);
    sendBucketToNode(remoteBucketShuffleJob); // todo yuval - this needs to be replaced with adding to some queue
    return retVal;

}

vector<BucketMessage> BucketManagerMessageHandler::handleFinishBucketReceive(BucketMessage msg){
    string logMsg = "Node " + std::to_string(bucketManager->nodeId) + " log: " + "handleFinishBucketReceive. Buckets left: " + std::to_string( bucketsToReceiveFromNodes.size()) +"\n";//todo DFD
    logActivity(logMsg);
    vector<BucketMessage> retVal;

    uint64_t finishedBucket = convertBytesBackToUint64(&msg.messageData[BUCKET_ID_START_INDEX]);
    bucketsToReceiveFromNodes.erase(finishedBucket);
    if(bucketsToReceiveFromNodes.empty()){
        string finishMsg = "Node " + std::to_string(bucketManager->nodeId) + " log: " + "FINISHED RECEIVING ALL BUCKETS!. \n" ;//todo DFD
        logActivity(finishMsg);
        uint8_t messageData[MESSAGE_SIZE];
        messageData[MSG_ENUM_IDX] = (uint8_t) NODE_FINISHED_RECEIVING_BUCKETS;
        messageData[MSG_SND_IDX] = (uint8_t) bucketManager->nodeId;
        auto finishedReceivingBucketsMsg = BucketMessage(messageData);
        retVal = collectMessagesToGossip(finishedReceivingBucketsMsg);
        //todo case for the last one - maybe DFD?
        consensusVec[NODE_FINISHED_RECEIVING_BUCKETS].set(bucketManager->nodeId - 1);
        if(consensusVec[NODE_FINISHED_RECEIVING_BUCKETS].all()) {
            moveAtomicallyToNormalState();
        }
    }
    return retVal;

}

vector<BucketMessage> BucketManagerMessageHandler::handleBucketMovedToNewNode(BucketMessage msg){
    string logMsg = "Node " + std::to_string(bucketManager->nodeId) + " log: " + "handleBucketMovedToNewNode \n";//todo DFD
    logActivity(logMsg);
    vector<BucketMessage> retVal;

    uint64_t bucketId = convertBytesBackToUint64(&msg.messageData[BUCKET_ID_START_INDEX]);
    uint64_t nodeId = convertBytesBackToUint64(&msg.messageData[NODE_ID_START_INDEX]);
    // todo - this to be locked!
    //bucketManager->bucketIdToNodeCache[bucketId] = nodeId;
    return retVal;
}

vector<BucketMessage> BucketManagerMessageHandler::handleNodeFinishedReceivingBuckets(BucketMessage msg){
    string logMsg = "Node " + std::to_string(bucketManager->nodeId) + " log: " + "handleNodeFinishedReceivingBuckets. \n" ;//todo DFD
    logActivity(logMsg);
    vector<BucketMessage> retVal;

    bool allNodesUpdated = markBitAndReturnAreAllNodesExcludingSelfTrue(msg);
    if(allNodesUpdated){
        moveAtomicallyToNormalState();
    }
    return retVal;

}




// Node joined / leaving functions


vector<BucketMessage> BucketManagerMessageHandler::gossipNodeJoined() {

    string logMsg = "Node " + std::to_string(bucketManager->nodeId) + " log: " + "gossipNodeJoined. \n" ;//todo DFD
    logActivity(logMsg);
    vector<BucketMessage> retVal;

    uint8_t messageDataForJoinedNode[MESSAGE_SIZE];
    messageDataForJoinedNode[MSG_ENUM_IDX] = (uint8_t) NODE_JOINED_THE_CLUSTER_ENTER;
    messageDataForJoinedNode[MSG_SND_IDX] = (uint8_t) bucketManager->nodeId;
    messageDataForJoinedNode[MSG_DATA_START_IDX] = (uint8_t) bucketManager->nodeId;
    auto bucketAmountFinishedMsg = BucketMessage(messageDataForJoinedNode);
    retVal = collectMessagesToGossip(bucketAmountFinishedMsg);
    bucketManager->nodeLeftOrJoinedCluster(true,bucketManager->nodeId);
    bucketManager->getOldConsistentHashingInfoForNewNode();
    uint8_t messageDataForConsistentHashing[MESSAGE_SIZE];
    messageDataForConsistentHashing[MSG_ENUM_IDX] = (uint8_t) CONSISTENT_HASHING_INFORMATION_SYNCED_ENTER;
    messageDataForConsistentHashing[MSG_SND_IDX] = (uint8_t) bucketManager->nodeId;
    auto consistentHashingCompleted = BucketMessage(messageDataForConsistentHashing);
    collectMessagesToGossip(consistentHashingCompleted); // todo - that doesnt matter right now

    return retVal;

}

vector<BucketMessage> BucketManagerMessageHandler::gossipNodeLeft(){

    string logMsg = "Node " + std::to_string(bucketManager->nodeId) + " log: " + "gossipNodeLeft. \n" ;//todo DFD
    logActivity(logMsg);
    vector<BucketMessage> retVal;

    uint8_t messageDataForLeavingNode[MESSAGE_SIZE];
    messageDataForLeavingNode[MSG_ENUM_IDX] = (uint8_t) NODE_LEAVING_THE_CLUSTER_LEAVE;
    messageDataForLeavingNode[MSG_SND_IDX] = (uint8_t) bucketManager->nodeId;
    messageDataForLeavingNode[MSG_DATA_START_IDX] = (uint8_t) bucketManager->nodeId;
    auto bucketAmountFinishedMsg = BucketMessage(messageDataForLeavingNode);
    retVal = collectMessagesToGossip(bucketAmountFinishedMsg);
    bucketManager->nodeLeftOrJoinedCluster(false,bucketManager->nodeId);
    uint8_t messageDataForConsistentHashing[MESSAGE_SIZE];
    messageDataForConsistentHashing[MSG_ENUM_IDX] = (uint8_t) CONSISTENT_HASHING_INFORMATION_SYNCED_LEAVE;
    messageDataForConsistentHashing[MSG_SND_IDX] = (uint8_t) bucketManager->nodeId;
    auto consistentHashingCompleted = BucketMessage(messageDataForConsistentHashing);
    vector<BucketMessage> gossipMessageCompleted = collectMessagesToGossip(consistentHashingCompleted);
    retVal.insert( retVal.end(), gossipMessageCompleted.begin(), gossipMessageCompleted.end() );
    return retVal;

}


// Buckets amounts functions

vector<BucketMessage> BucketManagerMessageHandler::prepareBucketAmountsToNodesMessages(MessagesEnum msgEnum){
    vector<BucketMessage> retVal;

    for (auto itr : nodeIdsForMessages){
        if(itr == bucketManager->nodeId) continue;
        uint8_t messageData[MESSAGE_SIZE];
        messageData[MSG_ENUM_IDX] = (uint8_t) msgEnum;
        messageData[MSG_SND_IDX] = (uint8_t) bucketManager->nodeId;
        messageData[MSG_RCV_IDX] = (uint8_t) itr;
        auto bucketAmountToNodeMsg = BucketMessage(messageData);

        // if this a node that just joined he will have 0 buckets to send to others. it is planned this way.
        uint8_t value = (uint8_t) 0;
        if(bucketManager->bucketAmountsToSendEachNodeAfterMerging.find(itr) !=  bucketManager->bucketAmountsToSendEachNodeAfterMerging.end()) {
            value = (uint8_t) bucketManager->bucketAmountsToSendEachNodeAfterMerging[itr];
        }
        messageData[MSG_DATA_START_IDX] = (uint8_t) value;
        string amountMsg = "Node " + std::to_string(bucketManager->nodeId) + " sends : " + std::to_string((int)value) + " buckets to node: " + std::to_string(itr) + "\n" ;//todo DFD
        logActivity(amountMsg);
        retVal.emplace_back(bucketAmountToNodeMsg);
        sendMessage(bucketAmountToNodeMsg); // todo - dfd
    }
    return retVal;

}

vector<BucketMessage> BucketManagerMessageHandler::gossipBucketAmountFinishedEnter(){
    string logMsg = "Node " + std::to_string(bucketManager->nodeId) + " log: " + "gossipBucketAmountFinishedEnter. \n" ;//todo DFD
    logActivity(logMsg);
    vector<BucketMessage> retVal;

    uint8_t messageData[MESSAGE_SIZE];
    messageData[MSG_ENUM_IDX] = (uint8_t) BUCKETS_AMOUNTS_APPROVED_ENTER;
    messageData[MSG_SND_IDX] = (uint8_t) bucketManager->nodeId;

    auto bucketAmountFinishedMsg = BucketMessage(messageData);
    retVal = collectMessagesToGossip(bucketAmountFinishedMsg);
    return retVal;

}

vector<BucketMessage> BucketManagerMessageHandler::gossipBucketAmountFinishedLeave(){
    string logMsg = "Node " + std::to_string(bucketManager->nodeId) + " log: " + "gossipBucketAmountFinishedLeave. \n" ;//todo DFD
    logActivity(logMsg);
    vector<BucketMessage> retVal;

    uint8_t messageData[MESSAGE_SIZE];
    messageData[MSG_ENUM_IDX] = (uint8_t) BUCKETS_AMOUNTS_APPROVED_LEAVE;
    messageData[MSG_SND_IDX] = (uint8_t) bucketManager->nodeId;

    auto bucketAmountFinishedMsg = BucketMessage(messageData);
    retVal = collectMessagesToGossip(bucketAmountFinishedMsg);
    return retVal;

}

//Union Find functions
vector<BucketMessage> BucketManagerMessageHandler::prepareGossipUnionFindAmountsMessages() {
    string logMsg = "Node " + std::to_string(bucketManager->nodeId) + " log: " + "prepareGossipUnionFindAmountsMessages. \n";//todo DFD
    logActivity(logMsg);
    vector<BucketMessage> retVal;

    uint8_t messageData[MESSAGE_SIZE];
    messageData[MSG_ENUM_IDX] = (uint8_t) UNION_FIND_BUCKETS_AMOUNT;
    messageData[MSG_SND_IDX] = (uint8_t) bucketManager->nodeId;
    uint64_t unionFindAmount = bucketManager->getDisjointSets().getUnionFindSize();
    breakDownUint64ToBytes(unionFindAmount, &messageData[MSG_DATA_START_IDX]);
    auto unionFindDataToGossip = BucketMessage(messageData);
    retVal = collectMessagesToGossip(unionFindDataToGossip);

    if (bucketManager->nodeIsToBeDeleted) {
        vector<BucketMessage> finishedMessages = gossipFinishedUnionFind(UNION_FIND_NODE_RECEIVED_ALL_LEAVE);
        retVal.insert( retVal.end(), finishedMessages.begin(), finishedMessages.end() );
    }
    return retVal;

}
vector<BucketMessage> BucketManagerMessageHandler::gossipLocalUnionFindData(MessagesEnum msgEnum){

    string logMsg = "Node " + std::to_string(bucketManager->nodeId) + " log: " + "gossipLocalUnionFindData. \n" ;//todo DFD
    logActivity(logMsg);
    vector<BucketMessage> retVal;
    vector<pair<uint64_t,uint64_t>>* unionFindLocalData = prepareUnionFindData();
    int runningVectorIndex = 0;
    int messageNumRequired = ceil((double)unionFindLocalData->size() / UNION_FIND_DATA_MAX_AMOUNT);
    for(int i = 0; i < messageNumRequired; i++){
        uint8_t messageData[MESSAGE_SIZE];
        messageData[MSG_ENUM_IDX] = (uint8_t) msgEnum;
        messageData[MSG_SND_IDX] = (uint8_t) bucketManager->nodeId;
        int amountLeftToSend = unionFindLocalData->size() - (i * UNION_FIND_DATA_MAX_AMOUNT);
        int amountOfBucketToSend = amountLeftToSend < UNION_FIND_DATA_MAX_AMOUNT ? amountLeftToSend : UNION_FIND_DATA_MAX_AMOUNT;
        messageData[UNION_FIND_BUCKET_DATA_SENT_IDX] = (uint8_t) amountOfBucketToSend;
        for(int j = 0; j < amountOfBucketToSend; j++){
            breakDownBucketIdToBytes(unionFindLocalData->at(runningVectorIndex).first,&messageData[MSG_DATA_START_IDX + (6 * j)]);
            breakDownBucketIdToBytes(unionFindLocalData->at(runningVectorIndex).second,&messageData[MSG_DATA_START_IDX + (6 * j) + 6]);
            runningVectorIndex++;
        }
        auto unionFindDataToGossip = BucketMessage(messageData);
        vector<BucketMessage> currentMsgs = collectMessagesToGossip(unionFindDataToGossip);
        retVal.insert( retVal.end(), currentMsgs.begin(), currentMsgs.end() );
    }

    return retVal;

}

vector<pair<uint64_t,uint64_t>>* BucketManagerMessageHandler::prepareUnionFindData(){

    string logMsg = "Node " + std::to_string(bucketManager->nodeId) + " log: " + "prepareUnionFindData. \n" ;//todo DFD
    logActivity(logMsg);
    auto *retVal = new vector<pair<uint64_t,uint64_t>>();
    for ( const auto &unionFindPair :   bucketManager->getDisjointSets().getMap() ) {
        retVal->emplace_back(unionFindPair.first,unionFindPair.second);
    }
    return retVal;
}

vector<BucketMessage> BucketManagerMessageHandler::addIncomingUnionFindData(BucketMessage msg) {

    vector<BucketMessage> retVal;

    uint8_t numberOfBucketDataInMessage = msg.messageData[UNION_FIND_BUCKET_DATA_SENT_IDX];
    string logMsg = "Node " + std::to_string(bucketManager->nodeId) + " log: " + "addIncomingUnionFindData. Buckets data left: " + std::to_string(unionFindTotalAmount) + "number of buckets arriving: " +  std::to_string((int)numberOfBucketDataInMessage) + "\n" ;//todo DFD
    logActivity(logMsg);
    for(int i = 0; i < (int)numberOfBucketDataInMessage; i++){
        uint64_t bucketId = convertBytesBackToBucketId(&msg.messageData[MSG_DATA_START_IDX+ (i * 6)]);
        uint64_t rootId = convertBytesBackToBucketId(&msg.messageData[(MSG_DATA_START_IDX + (i * 6)) + 6 ]);
        bucketManager->putNewUnionFindPairData(bucketId, rootId);
    }
    unionFindTotalAmount -= (int)numberOfBucketDataInMessage;
    if( unionFindTotalAmount == 0){
        retVal = gossipFinishedUnionFind(UNION_FIND_NODE_RECEIVED_ALL_LEAVE);
    }
    return retVal;

}

vector<BucketMessage> BucketManagerMessageHandler::gossipFinishedUnionFind(MessagesEnum msgEnum){
    string logMsg = "Node " + std::to_string(bucketManager->nodeId) + " log: " + "gossipFinishedUnionFind. \n" ;//todo DFD
    logActivity(logMsg);
    vector<BucketMessage> retVal;

    uint8_t messageData[MESSAGE_SIZE];
    messageData[MSG_ENUM_IDX] = (uint8_t) msgEnum;
    messageData[MSG_SND_IDX] = (uint8_t) bucketManager->nodeId;
    auto unionFindFinishedMsg = BucketMessage(messageData);
    retVal = collectMessagesToGossip(unionFindFinishedMsg);
    return retVal;

}


///////// Consensus Vector functions /////////

bool BucketManagerMessageHandler::markBitAndReturnAreAllNodesExcludingSelfTrue(const BucketMessage msg){

    int sendingNode = (int)msg.messageData[MSG_SND_IDX];
    consensusVec[msg.messageEnum].set(sendingNode - 1);
    consensusVec[msg.messageEnum].set(bucketManager->nodeId - 1);
    if(consensusVec[msg.messageEnum].all()){
        consensusVec[msg.messageEnum].reset();
        return true;
    }else{
        return false;
    }
}

bool BucketManagerMessageHandler::markBitAndReturnAreAllNodesIncludingSelfTrue(const BucketMessage msg){
    int sendingNode = (int)msg.messageData[MSG_SND_IDX];
    consensusVec[msg.messageEnum].set(sendingNode - 1);
    if(consensusVec[msg.messageEnum].all()){
        consensusVec[msg.messageEnum].reset();
        return true;
    }else{
        return false;
    }
}

///////// buckets sending functions functions /////////

vector<BucketMessage> BucketManagerMessageHandler::prepareOtherNodesForIncomingBuckets(){ //pair is <bucket id , node>
    string logMsg = "Node " + std::to_string(bucketManager->nodeId) + " log: " + "prepareOtherNodesForIncomingBuckets. \n" ;//todo DFD
    logActivity(logMsg);
    vector<BucketMessage> retVal;

    vector<pair<uint64_t,uint64_t>> bucketsAndNodes =  bucketManager->getBucketsShufflePrioritiesAndNodes();
    for(auto pair : bucketsAndNodes){
        uint8_t messageData[MESSAGE_SIZE];
        messageData[MSG_ENUM_IDX] = (uint8_t) REQUEST_TO_START_BUCKET_SEND;
        messageData[MSG_SND_IDX] = (uint8_t) bucketManager->nodeId;
        messageData[MSG_RCV_IDX] = (uint8_t) pair.second;
        breakDownUint64ToBytes(pair.first,&messageData[BUCKET_ID_START_INDEX]);
        auto newBucketIdToNodeMsg = BucketMessage(messageData);
        retVal.emplace_back(newBucketIdToNodeMsg);
        sendMessage(newBucketIdToNodeMsg); // todo - dfd
    }
    // this is to ensure that consensus is reached also considering the leaving node -
    // it also needs to report that it finished receiving nodes (empty manner)
    if(bucketManager->nodeIsToBeDeleted){
        uint8_t messageData[MESSAGE_SIZE];
        messageData[MSG_ENUM_IDX] = (uint8_t) NODE_FINISHED_RECEIVING_BUCKETS;
        messageData[MSG_SND_IDX] = (uint8_t) bucketManager->nodeId;
        auto finishedReceivingBucketsMsg = BucketMessage(messageData);
        vector<BucketMessage> finishedMessages = collectMessagesToGossip(finishedReceivingBucketsMsg);
        retVal.insert( retVal.end(), finishedMessages.begin(), finishedMessages.end() );
    }
    return retVal;

}

void BucketManagerMessageHandler::sendBucketToNode(RemoteBucketShuffleJob bucketShuffleJob){
    uint64_t bucketId = bucketShuffleJob.bucketId;
    uint64_t nodeId = bucketShuffleJob.nodeId;
    map<uint64_t, uint64_t> mapOfNode = bucketManager->mergableBucketsForEachNode[nodeId];
    Bucket* bigBucket = &(bucketManager->bucketsMap.find(bucketId)->second);
    Bucket* smallBucket;

    // dealing with big bucket first
    bigBucket->bucketLock.lock();
    uint64_t bigBucketSsdSlotsStart = bigBucket-> SSDSlotStart;
    for (auto const& [key, val] : bigBucket->pageIdToSlot){
        //uint64_t oldSsdSlot = bigBucketSsdSlotsStart + val;
        // todo yuval -  actually do something with it here
    }
    bigBucket->destroyBucketData();
    bigBucket->bucketLock.unlock();
    bucketManager->deleteBucket(bucketId);

    if(mapOfNode[bucketId] != BUCKET_ALREADY_MERGED){
        smallBucket = &(bucketManager->bucketsMap.find(mapOfNode[bucketId])->second);
        smallBucket->bucketLock.lock();
        uint64_t smallBucketSsdSlotsStart = smallBucket-> SSDSlotStart;
        for (auto const& [key, val] : smallBucket->pageIdToSlot){
            //uint64_t oldSsdSlot = smallBucketSsdSlotsStart + val;
            // todo yuval -  actually do something with it here
        }
        smallBucket->destroyBucketData();
        smallBucket->bucketLock.unlock();
        bucketManager->deleteBucket(smallBucket->getBucketId());
    }

    string logMsg = "Node " + std::to_string(bucketManager->nodeId) + " log: " + "finished sending bucket " + std::to_string(bucketId)+ " to node : " + std::to_string(nodeId)+  " \n" ;//todo DFD
    logActivity(logMsg);
    uint8_t messageData[MESSAGE_SIZE];
    breakDownUint64ToBytes(bucketId,&messageData[BUCKET_ID_START_INDEX]);
    messageData[MSG_ENUM_IDX] = (uint8_t) FINISHED_BUCKET;
    messageData[MSG_SND_IDX] = (uint8_t) bucketManager->nodeId;
    messageData[MSG_RCV_IDX] = (uint8_t) nodeId;
    auto finishedBucketMsg = BucketMessage(messageData);
    sendMessage(finishedBucketMsg);
    gossipBucketMoved(bucketId,nodeId);
}

vector<BucketMessage> BucketManagerMessageHandler::gossipBucketMoved(uint64_t bucketId, uint64_t nodeId){
    vector<BucketMessage> retVal;

    uint8_t messageData[MESSAGE_SIZE];
    breakDownUint64ToBytes(bucketId,&messageData[BUCKET_ID_START_INDEX]);
    messageData[MSG_ENUM_IDX] = (uint8_t) BUCKET_MOVED_TO_NEW_NODE;
    messageData[MSG_SND_IDX] = (uint8_t) bucketManager->nodeId;
    breakDownUint64ToBytes(bucketId,&messageData[BUCKET_ID_START_INDEX]);
    breakDownUint64ToBytes(nodeId, &messageData[NODE_ID_START_INDEX]);
    auto bucketMovedMsg = BucketMessage(messageData);
    retVal = collectMessagesToGossip(bucketMovedMsg);
    return retVal;

}

///////// Misc functions /////////

vector<BucketMessage> BucketManagerMessageHandler::collectMessagesToGossip(BucketMessage msg){
    vector<BucketMessage> retVal;

    for(uint64_t nodeId: nodeIdsForMessages){
        BucketMessage bucketMsg = BucketMessage(msg.messageData);
        msg.messageData[MSG_RCV_IDX] = (uint8_t) nodeId;
        retVal.emplace_back(bucketMsg);
        sendMessage(msg); // todo -DFD
    }
    return retVal;

}

void BucketManagerMessageHandler::checkMailbox() { //todo DFD
    uint64_t nodeId = bucketManager->nodeId;
    if(nodeId == 1){
        firstMtx->lock();

        if(firstMessageQueue->empty() == false){

            BucketMessage msg = firstMessageQueue->front();
            auto receivingNodeId = (uint64_t) msg.messageData[MSG_SND_IDX];//todo DFD
            string readMsg = "\nread msg from node: " + std::to_string(receivingNodeId);
            string logMsg = "Node " + std::to_string(bucketManager->nodeId) + " log: " + "read msg from node: " +  std::to_string(receivingNodeId) + "\n" ;//todo DFD
            logActivity(logMsg);
            firstMessageQueue->pop();
            firstMtx->unlock();
            handleIncomingMessage(msg);
        }else{
            firstMtx->unlock();
        }
    }
    if(nodeId == 2){
        secondMtx->lock();
        if(secondMessageQueue->empty() == false){

            BucketMessage msg = secondMessageQueue->front();
            auto receivingNodeId = (uint64_t) msg.messageData[MSG_SND_IDX];//todo DFD
            string logMsg = "Node " + std::to_string(bucketManager->nodeId) + " log: " + "read msg from node: " +  std::to_string(receivingNodeId) + "\n" ;//todo DFD
            logActivity(logMsg);

            secondMessageQueue->pop();
            secondMtx->unlock();
            handleIncomingMessage(msg);
        }else{
            secondMtx->unlock();
        }
    }
    if(nodeId == 3){
        thirdMtx->lock();
        if(thirdMessageQueue->empty() == false){

            BucketMessage msg = thirdMessageQueue->front();
            auto receivingNodeId = (uint64_t) msg.messageData[MSG_SND_IDX];//todo DFD
            string logMsg = "Node " + std::to_string(bucketManager->nodeId) + " log: " + "read msg from node: " +  std::to_string(receivingNodeId) + "\n" ;//todo DFD
            logActivity(logMsg);

            thirdMessageQueue->pop();
            thirdMtx->unlock();
            handleIncomingMessage(msg);
        }else{
            thirdMtx->unlock();
        }
    }
}

void BucketManagerMessageHandler::sendMessage(BucketMessage msg) { //todo DFD
    auto receivingNodeId = (uint64_t) msg.messageData[MSG_RCV_IDX];
    string logMsg = "Node " + std::to_string(bucketManager->nodeId) + " log: " + "write msg to node: " +  std::to_string(receivingNodeId) + "\n" ;//todo DFD
    if(receivingNodeId == bucketManager->nodeId) return; // avoiding sending self messages
    if(receivingNodeId == 1) {

        logActivity(logMsg);

        firstMtx->lock();
        firstMessageQueue->push(msg);
        firstMtx->unlock();
    }
    if(receivingNodeId == 2){
        logActivity(logMsg);

        secondMtx->lock();
        secondMessageQueue->push(msg);
        secondMtx->unlock();
    }
    if(receivingNodeId == 3) {
        logActivity(logMsg);

        thirdMtx->lock();
        thirdMessageQueue->push(msg);
        thirdMtx->unlock();
    }

}

void BucketManagerMessageHandler::logActivity(string const str){


    uint64_t nodeId = bucketManager->nodeId;
    if(nodeId ==1){
        std::ofstream log("/Users/yuvalfreund/Desktop/MasterThesis/localThesis/logs/log1.txt", std::ios_base::app | std::ios_base::out);
        log << str;
    }
    if(nodeId == 2){
        std::ofstream log("/Users/yuvalfreund/Desktop/MasterThesis/localThesis/logs/log2.txt", std::ios_base::app | std::ios_base::out);
        log << str;        }
    if(nodeId == 3){
        std::ofstream log("/Users/yuvalfreund/Desktop/MasterThesis/localThesis/logs/log3.txt", std::ios_base::app | std::ios_base::out);
        log << str;
    }
}

void BucketManagerMessageHandler::moveAtomicallyToNormalState(){
    string finishedMsg = "Node " + std::to_string(bucketManager->nodeId) + " log: " + "moveAtomicallyToNormalState. \n" ;//todo DFD
    logActivity(finishedMsg);
    bucketsToReceiveFromNodes.clear();
    bucketManager->atomicallyMoveToNormal();
}

// utils

void BucketManagerMessageHandler::breakDownUint64ToBytes(uint64_t input, uint8_t retVal[8]){
    for ( int i = 0; i < 8; i++ ){
        retVal[i] = (uint8_t)(input >> (8 * i) & 0xFF);
    }
}


void BucketManagerMessageHandler::breakDownBucketIdToBytes(uint64_t input, uint8_t retVal[6]){
    input >>= 2 * 8;
    for ( int i = 0; i < 6; i++ ){
        retVal[i] = (uint8_t)(input >> (8 * i) & 0xFF);
    }
}

uint64_t BucketManagerMessageHandler::convertBytesBackToUint64(uint8_t input[8]){
    uint64_t retVal = 0 ;
    for(int i = 0; i<8; i++){
        retVal += ((uint64_t) input[i] << i * 8);
    }
    return retVal;
}
uint64_t BucketManagerMessageHandler::convertBytesBackToBucketId(uint8_t input[6]){
    uint64_t retVal = 0 ;
    for(int i = 0; i<6; i++){
        retVal += ((uint64_t) input[i] << i * 8);
    }
    retVal <<= 16;
    return retVal;
}


