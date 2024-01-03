
#include "BucketManagerMessageHandler.h"

void BucketManagerMessageHandler::handleIncomingMessage(Message msg){
    string logMsg = "Node " + std::to_string(bucketManager->nodeId) + " log: " + "handleIncomingMessage. \n" ;//todo DFD
    logActivity(logMsg);
    switch(msg.messageEnum){
        // node joining

        case NODE_JOINED_THE_CLUSTER_ENTER:
            handleNewNodeJoinedEnter(msg);
            break;

        case CONSISTENT_HASHING_INFORMATION_SYNCED_ENTER:
            handleNewHashingStateSynchronizedEnter(msg);
            break;

        case UNION_FIND_DATA_ENTER:
            handleIncomingUnionFindDataEnter(msg);
            break;

        case UNION_FIND_DATA_FINISHED_ENTER:
            handleUnionFindDataFinishedEnter(msg);
            break;

        case BUCKET_AMOUNTS_DATA_ENTER:
            handleBucketAmountsDataEnter(msg);
            break;

        case BUCKETS_AMOUNTS_APPROVED_ENTER:
            handleBucketAmountsApprovedEnter(msg);
            break;
            // node leaving

        case NODE_LEAVING_THE_CLUSTER_LEAVE:
            handleNodeLeftTheCLusterLeave(msg);
            break;

        case CONSISTENT_HASHING_INFORMATION_SYNCED_LEAVE:
            handleNewHashingStateSynchronizedLeave(msg);
            break;

        case UNION_FIND_DATA_LEAVE:
            handleIncomingUnionFindDataLeave(msg);
            break;

        case UNION_FIND_DATA_FINISHED_LEAVE:
            handleUnionFindDataFinishedLeave(msg);
            break;

        case BUCKET_AMOUNTS_DATA_LEAVE:
            handleBucketAmountsDataLeave(msg);
            break;

        case BUCKETS_AMOUNTS_APPROVED_LEAVE:
            handleBucketAmountsApprovedLeave(msg);
            break;

            // buckets shuffling messages

        case BEGIN_NEW_BUCKET:
            handleBeginNewBucket(msg);
            break;

        case APPROVE_NEW_BUCKET_READY_TO_RECEIVE:
            handleApproveNewBucketReadyToReceive(msg);
            break;
        case FINISHED_BUCKET:
            handleFinishBucketReceive(msg);
            break;

        case NODE_FINISHED_RECEIVING_BUCKETS:
            handleNodeFinishedReceivingBuckets(msg);
            break;
    }
}

//node joining handlers

void BucketManagerMessageHandler::handleNewNodeJoinedEnter(Message msg){
    string logMsg = "Node " + std::to_string(bucketManager->nodeId) + " log: " + "handleNewNodeJoinedEnter. \n" ;//todo DFD
    logActivity(logMsg);

    auto newNodeId = (uint64_t) msg.messageData[MSG_DATA_START_IDX];
    bucketManager->nodeLeftOrJoinedCluster(true,newNodeId);
    nodeIdsForMessages.emplace_back(newNodeId);
    bucketManager->mergeOwnBuckets();
    byte messageData[MESSAGE_SIZE];
    messageData[MSG_ENUM_IDX] = (byte) CONSISTENT_HASHING_INFORMATION_SYNCED_ENTER;
    messageData[MSG_SND_IDX] = (byte) bucketManager->nodeId;
    auto consistentHashingCompleted = Message(messageData);
    gossipMessage(consistentHashingCompleted);
}

void BucketManagerMessageHandler::handleNewHashingStateSynchronizedEnter(Message msg){
    string logMsg = "Node " + std::to_string(bucketManager->nodeId) + " log: " + "handleNewHashingStateSynchronizedEnter. \n" ;//todo DFD
    logActivity(logMsg);

    bool allNodesUpdated = markBitAndReturnAreAllNodesExcludingSelfTrue(msg);
    if(allNodesUpdated){
        gossipLocalUnionFindData(UNION_FIND_DATA_ENTER);
    }
}

void BucketManagerMessageHandler::handleIncomingUnionFindDataEnter(Message msg){
    string logMsg = "Node " + std::to_string(bucketManager->nodeId) + " log: " + "handleIncomingUnionFindDataEnter. \n" ;//todo DFD
    logActivity(logMsg);

    addIncomingUnionFindData(msg);
    bool allNodesUpdated = markBitAndReturnAreAllNodesExcludingSelfTrue(msg);
    if(allNodesUpdated){
        gossipFinishedUnionFind(UNION_FIND_DATA_FINISHED_ENTER);
    }
}

void BucketManagerMessageHandler::handleUnionFindDataFinishedEnter(Message msg){
    string logMsg = "Node " + std::to_string(bucketManager->nodeId) + " log: " + "handleUnionFindDataFinishedEnter. \n" ;//todo DFD
    logActivity(logMsg);

    bool allNodesUpdated = markBitAndReturnAreAllNodesExcludingSelfTrue(msg);
    if(allNodesUpdated){
        spreadBucketAmountsToNodes(BUCKET_AMOUNTS_DATA_ENTER);
    }
}

void BucketManagerMessageHandler::handleBucketAmountsDataEnter(Message msg){
    string logMsg = "Node " + std::to_string(bucketManager->nodeId) + " log: " + "handleBucketAmountsDataEnter. \n" ;//todo DFD
    logActivity(logMsg);
    bucketManager->updateAndCheckRequestedBucketNum((uint64_t) msg.messageData[MSG_DATA_START_IDX]);
    bool allNodesUpdated = markBitAndReturnAreAllNodesExcludingSelfTrue(msg);
    if(allNodesUpdated){
        gossipBucketAmountFinishedEnter();
    }
}

void BucketManagerMessageHandler::handleBucketAmountsApprovedEnter(Message msg){
    string logMsg = "Node " + std::to_string(bucketManager->nodeId) + " log: " + "handleBucketAmountsApprovedEnter. \n" ;//todo DFD
    logActivity(logMsg);

    bool allNodesUpdated = markBitAndReturnAreAllNodesExcludingSelfTrue(msg);
    if(allNodesUpdated){
        string finished = "Node " + std::to_string(bucketManager->nodeId) + " log: " + "finished synchronizing stage!. \n" ;//todo DFD
        logActivity(finished);
        prepareNodesForIncomingBuckets();
    }
}

//node leaving handlers

void BucketManagerMessageHandler::handleNodeLeftTheCLusterLeave(Message msg){
    string logMsg = "Node " + std::to_string(bucketManager->nodeId) + " log: " + "handleNodeLeftTheCLusterLeave. \n" ;//todo DFD
    logActivity(logMsg);

    auto leavingNodeId = (uint64_t) msg.messageData[MSG_DATA_START_IDX];
    bucketManager->nodeLeftOrJoinedCluster(false,leavingNodeId);

    byte messageData[MESSAGE_SIZE];
    messageData[MSG_ENUM_IDX] = (byte) CONSISTENT_HASHING_INFORMATION_SYNCED_LEAVE;
    messageData[MSG_SND_IDX] = (byte) bucketManager->nodeId;
    auto consistentHashingCompleted = Message(messageData);
    gossipMessage(consistentHashingCompleted);
}

void BucketManagerMessageHandler::handleNewHashingStateSynchronizedLeave(Message msg){
    string logMsg = "Node " + std::to_string(bucketManager->nodeId) + " log: " + "handleNewHashingStateSynchronizedLeave. \n" ;//todo DFD
    logActivity(logMsg);
    bool allNodesUpdated = markBitAndReturnAreAllNodesExcludingSelfTrue(msg);
    if(allNodesUpdated){
        gossipLocalUnionFindData(UNION_FIND_DATA_LEAVE);
    }
}

void BucketManagerMessageHandler::handleIncomingUnionFindDataLeave(Message msg){
    // if this is the leaving buckets, there is no need for to add the union find data to it - it will be deleted
    string logMsg = "Node " + std::to_string(bucketManager->nodeId) + " log: " + "handleIncomingUnionFindDataLeave. \n" ;//todo DFD
    logActivity(logMsg);
    if(bucketManager -> nodeIsToBeDeleted == false){
        addIncomingUnionFindData(msg);
    }

    bool allNodesUpdated = markBitAndReturnAreAllNodesExcludingSelfTrue(msg);
    if(allNodesUpdated){
        gossipFinishedUnionFind(UNION_FIND_DATA_FINISHED_LEAVE);
    }
}

void BucketManagerMessageHandler::handleUnionFindDataFinishedLeave(Message msg){
    string logMsg = "Node " + std::to_string(bucketManager->nodeId) + " log: " + "handleUnionFindDataFinishedLeave. \n" ;//todo DFD
    logActivity(logMsg);

    bool allNodesUpdated = markBitAndReturnAreAllNodesExcludingSelfTrue(msg);
    if(allNodesUpdated){
        spreadBucketAmountsToNodes(BUCKET_AMOUNTS_DATA_LEAVE);
    }
}

void BucketManagerMessageHandler::handleBucketAmountsDataLeave(Message msg){
    string logMsg = "Node " + std::to_string(bucketManager->nodeId) + " log: " + "handleBucketAmountsDataLeave. \n" ;//todo DFD
    logActivity(logMsg);

    bucketManager->updateAndCheckRequestedBucketNum((uint64_t) msg.messageData[MSG_DATA_START_IDX]);
    bool allNodesUpdated = markBitAndReturnAreAllNodesExcludingSelfTrue(msg);
    if(allNodesUpdated){
        gossipBucketAmountFinishedLeave();
    }
}

void BucketManagerMessageHandler::handleBucketAmountsApprovedLeave(Message msg){
    string logMsg = "Node " + std::to_string(bucketManager->nodeId) + " log: " + "handleBucketAmountsApprovedLeave. \n" ;//todo DFD
    logActivity(logMsg);

    bool allNodesUpdated = markBitAndReturnAreAllNodesExcludingSelfTrue(msg);
    if(allNodesUpdated){
        string finished = "Node " + std::to_string(bucketManager->nodeId) + " log: " + "finished synchronizing stage!. \n" ;//todo DFD
        logActivity(finished);
        prepareNodesForIncomingBuckets();
    }
}

//sending buckets handlers

void BucketManagerMessageHandler::handleBeginNewBucket(Message msg){
    uint64_t newBucketId = convertBytesBackToUint64(&msg.messageData[BUCKET_ID_START_INDEX]);
    string logMsg = "Node " + std::to_string(bucketManager->nodeId) + " log: " + "handleBeginNewBucket. Bucket id:" +std::to_string(newBucketId)+ " \n" ;//todo DFD
    logActivity(logMsg);
    bucketManager->createNewBucket(false, newBucketId);
    bucketsToReceiveFromNodes.insert(newBucketId);
    byte messageData[MESSAGE_SIZE];
    messageData[MSG_ENUM_IDX] = (byte) APPROVE_NEW_BUCKET_READY_TO_RECEIVE;
    messageData[MSG_SND_IDX] = (byte) bucketManager->nodeId;
    messageData[MSG_RCV_IDX] = msg.messageData[MSG_SND_IDX];
    breakDownUint64ToBytes(newBucketId,&messageData[BUCKET_ID_START_INDEX]);
    auto approveBucketMsg = Message(messageData);
    sendMessage(approveBucketMsg);
}

void BucketManagerMessageHandler::handleApproveNewBucketReadyToReceive(Message msg) {
    string logMsg = "Node " + std::to_string(bucketManager->nodeId) + " log: " + "handleApproveNewBucketReadyToReceive. \n" ;//todo DFD
    logActivity(logMsg);
    uint64_t bucketToSend = convertBytesBackToUint64(&msg.messageData[BUCKET_ID_START_INDEX]);
    auto receivingNode = (uint64_t) msg.messageData[MSG_SND_IDX];
    sendBucketToNode(bucketToSend,receivingNode);

}

void BucketManagerMessageHandler::handleFinishBucketReceive(Message msg){
    string logMsg = "Node " + std::to_string(bucketManager->nodeId) + " log: " + "handleFinishBucketReceive. Buckets left: " + std::to_string( bucketsToReceiveFromNodes.size()) +"\n";//todo DFD

    logActivity(logMsg);
    uint64_t finishedBucket = convertBytesBackToUint64(&msg.messageData[BUCKET_ID_START_INDEX]);
    bucketsToReceiveFromNodes.erase(finishedBucket);
    if(bucketsToReceiveFromNodes.empty()){
        string finishMsg = "Node " + std::to_string(bucketManager->nodeId) + " log: " + "FINISHED RECEIVING ALL BUCKETS!. \n" ;//todo DFD
        logActivity(finishMsg);
        byte messageData[MESSAGE_SIZE];
        messageData[MSG_ENUM_IDX] = (byte) NODE_FINISHED_RECEIVING_BUCKETS;
        messageData[MSG_SND_IDX] = (byte) bucketManager->nodeId;
        auto finishedReceivingBucketsMsg = Message(messageData);
        gossipMessage(finishedReceivingBucketsMsg);
        //todo case for the last one - maybe DFD?
        consensusVec[NODE_FINISHED_RECEIVING_BUCKETS].set(bucketManager->nodeId - 1);
        if(consensusVec[NODE_FINISHED_RECEIVING_BUCKETS].all()) {
            moveAtomicallyToNormalState();
        }
    }
}

void BucketManagerMessageHandler::handleNodeFinishedReceivingBuckets(Message msg){
    string logMsg = "Node " + std::to_string(bucketManager->nodeId) + " log: " + "handleNodeFinishedReceivingBuckets. \n" ;//todo DFD
    logActivity(logMsg);
    bool allNodesUpdated = markBitAndReturnAreAllNodesIncludingSelfTrue(msg);
    if(allNodesUpdated){
        moveAtomicallyToNormalState();
    }
}


// Node joined / leaving functions


void BucketManagerMessageHandler::gossipNodeJoined() {

    string logMsg = "Node " + std::to_string(bucketManager->nodeId) + " log: " + "gossipNodeJoined. \n" ;//todo DFD
    logActivity(logMsg);
    byte messageDataForJoinedNode[MESSAGE_SIZE];
    messageDataForJoinedNode[MSG_ENUM_IDX] = (byte) NODE_JOINED_THE_CLUSTER_ENTER;
    messageDataForJoinedNode[MSG_SND_IDX] = (byte) bucketManager->nodeId;
    messageDataForJoinedNode[MSG_DATA_START_IDX] = (byte) bucketManager->nodeId;
    auto bucketAmountFinishedMsg = Message(messageDataForJoinedNode);
    gossipMessage(bucketAmountFinishedMsg);
    bucketManager->nodeLeftOrJoinedCluster(true,bucketManager->nodeId);
    bucketManager->getOldConsistentHashingInfoForNewNode();
    byte messageDataForConsistentHashing[MESSAGE_SIZE];
    messageDataForConsistentHashing[MSG_ENUM_IDX] = (byte) CONSISTENT_HASHING_INFORMATION_SYNCED_ENTER;
    messageDataForConsistentHashing[MSG_SND_IDX] = (byte) bucketManager->nodeId;
    auto consistentHashingCompleted = Message(messageDataForConsistentHashing);
    gossipMessage(consistentHashingCompleted);
}

void BucketManagerMessageHandler::gossipNodeLeft(){

    string logMsg = "Node " + std::to_string(bucketManager->nodeId) + " log: " + "gossipNodeLeft. \n" ;//todo DFD
    logActivity(logMsg);
    byte messageDataForLeavingNode[MESSAGE_SIZE];
    messageDataForLeavingNode[MSG_ENUM_IDX] = (byte) NODE_LEAVING_THE_CLUSTER_LEAVE;
    messageDataForLeavingNode[MSG_SND_IDX] = (byte) bucketManager->nodeId;
    messageDataForLeavingNode[MSG_DATA_START_IDX] = (byte) bucketManager->nodeId;
    auto bucketAmountFinishedMsg = Message(messageDataForLeavingNode);
    gossipMessage(bucketAmountFinishedMsg);
    bucketManager->nodeLeftOrJoinedCluster(false,bucketManager->nodeId);
    byte messageDataForConsistentHashing[MESSAGE_SIZE];
    messageDataForConsistentHashing[MSG_ENUM_IDX] = (byte) CONSISTENT_HASHING_INFORMATION_SYNCED_LEAVE;
    messageDataForConsistentHashing[MSG_SND_IDX] = (byte) bucketManager->nodeId;
    auto consistentHashingCompleted = Message(messageDataForConsistentHashing);
    gossipMessage(consistentHashingCompleted);
}


// Buckets amounts functions

void BucketManagerMessageHandler::spreadBucketAmountsToNodes(MessagesEnum msgEnum){

    for (auto itr : nodeIdsForMessages){
        if(itr == bucketManager->nodeId) continue;
        byte messageData[MESSAGE_SIZE];
        messageData[MSG_ENUM_IDX] = (byte) msgEnum;
        messageData[MSG_SND_IDX] = (byte) bucketManager->nodeId;
        messageData[MSG_RCV_IDX] = (byte) itr;
        auto bucketAmountToNodeMsg = Message(messageData);

        // if this a node that just joined he will have 0 buckets to send to others. it is planned this way.
        byte value = (byte) 0;
        if(bucketManager->bucketAmountsToSendEachNodeAfterMerging.find(itr) !=  bucketManager->bucketAmountsToSendEachNodeAfterMerging.end()) {
            value = (byte) bucketManager->bucketAmountsToSendEachNodeAfterMerging[itr];
        }
        messageData[MSG_DATA_START_IDX] = (byte) value;
        string amountMsg = "Node " + std::to_string(bucketManager->nodeId) + " sends : " + std::to_string((int)value) + " buckets to node: " + std::to_string(itr) + "\n" ;//todo DFD
        logActivity(amountMsg);
        sendMessage(bucketAmountToNodeMsg);
    }
}

void BucketManagerMessageHandler::gossipBucketAmountFinishedEnter(){
    string logMsg = "Node " + std::to_string(bucketManager->nodeId) + " log: " + "gossipBucketAmountFinishedEnter. \n" ;//todo DFD
    logActivity(logMsg);

    byte messageData[MESSAGE_SIZE];
    messageData[MSG_ENUM_IDX] = (byte) BUCKETS_AMOUNTS_APPROVED_ENTER;
    messageData[MSG_SND_IDX] = (byte) bucketManager->nodeId;

    auto bucketAmountFinishedMsg = Message(messageData);
    gossipMessage(bucketAmountFinishedMsg);
}

void BucketManagerMessageHandler::gossipBucketAmountFinishedLeave(){
    string logMsg = "Node " + std::to_string(bucketManager->nodeId) + " log: " + "gossipBucketAmountFinishedLeave. \n" ;//todo DFD
    logActivity(logMsg);

    byte messageData[MESSAGE_SIZE];
    messageData[MSG_ENUM_IDX] = (byte) BUCKETS_AMOUNTS_APPROVED_LEAVE;
    messageData[MSG_SND_IDX] = (byte) bucketManager->nodeId;

    auto bucketAmountFinishedMsg = Message(messageData);
    gossipMessage(bucketAmountFinishedMsg);
}

//Union Find functions

void BucketManagerMessageHandler::gossipLocalUnionFindData(MessagesEnum msgEnum){

    string logMsg = "Node " + std::to_string(bucketManager->nodeId) + " log: " + "gossipLocalUnionFindData. \n" ;//todo DFD
    logActivity(logMsg);

    vector<pair<uint64_t,uint64_t>>* localData = prepareUnionFindData();
    byte messageData[MESSAGE_SIZE];
    messageData[MSG_ENUM_IDX] = (byte) msgEnum;
    messageData[MSG_SND_IDX] = (byte) bucketManager->nodeId;
    auto unionFindDataToGossip = Message(messageData);
    unionFindDataToGossip.unionFindVec = localData; // todo dfd
    gossipMessage(unionFindDataToGossip);
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

void BucketManagerMessageHandler::addIncomingUnionFindData(Message msg) {

    string logMsg = "Node " + std::to_string(bucketManager->nodeId) + " log: " + "addIncomingUnionFindData. \n" ;//todo DFD
    logActivity(logMsg);


    auto * vectorPtr = (vector<pair<uint64_t,uint64_t>> *) msg.unionFindVec;
    for(auto newPair : *vectorPtr){
        bucketManager->putNewUnionFindPairData(newPair.first, newPair.second);
    }
}

void BucketManagerMessageHandler::gossipFinishedUnionFind(MessagesEnum msgEnum){
    string logMsg = "Node " + std::to_string(bucketManager->nodeId) + " log: " + "gossipFinishedUnionFind. \n" ;//todo DFD
    logActivity(logMsg);

    byte messageData[MESSAGE_SIZE];
    messageData[MSG_ENUM_IDX] = (byte) msgEnum;
    messageData[MSG_SND_IDX] = (byte) bucketManager->nodeId;
    auto unionFindFinishedMsg = Message(messageData);
    gossipMessage(unionFindFinishedMsg);
}


///////// Consensus Vector functions /////////

bool BucketManagerMessageHandler::markBitAndReturnAreAllNodesExcludingSelfTrue(const Message msg){
    consensusVec[msg.messageEnum].set(msg.sendingNode - 1);
    consensusVec[msg.messageEnum].set(bucketManager->nodeId - 1);
    if(consensusVec[msg.messageEnum].all()){
        consensusVec[msg.messageEnum].reset();
        return true;
    }else{
        return false;
    }
}

bool BucketManagerMessageHandler::markBitAndReturnAreAllNodesIncludingSelfTrue(const Message msg){
    consensusVec[msg.messageEnum].set(msg.sendingNode - 1);
    if(consensusVec[msg.messageEnum].all()){
        consensusVec[msg.messageEnum].reset();
        return true;
    }else{
        return false;
    }
}

///////// buckets sending functions functions /////////

void BucketManagerMessageHandler::prepareNodesForIncomingBuckets(){ //pair is <bucket id , node>
    string logMsg = "Node " + std::to_string(bucketManager->nodeId) + " log: " + "prepareNodesForIncomingBuckets. \n" ;//todo DFD
    logActivity(logMsg);
    vector<pair<uint64_t,uint64_t>> bucketsAndNodes =  bucketManager->getBucketsShufflePrioritiesAndNodes();
    for(auto pair : bucketsAndNodes){
        byte messageData[MESSAGE_SIZE];
        messageData[MSG_ENUM_IDX] = (byte) BEGIN_NEW_BUCKET;
        messageData[MSG_SND_IDX] = (byte) bucketManager->nodeId;
        messageData[MSG_RCV_IDX] = (byte) pair.second;
        breakDownUint64ToBytes(pair.first,&messageData[BUCKET_ID_START_INDEX]);
        auto newBucketIdToNodeMsg = Message(messageData);
        sendMessage(newBucketIdToNodeMsg);
    }
}

void BucketManagerMessageHandler::sendBucketToNode(uint64_t bucketId, uint64_t nodeId){

    map<uint64_t, uint64_t> mapOfNode = bucketManager->mergableBucketsForEachNode[nodeId];
    Bucket* bigBucket = &(bucketManager->bucketsMap.find(bucketId)->second);
    Bucket* smallBucket;

    // dealing with big bucket first
    bigBucket->bucketLock.lock();
    //uint64_t bigBucketSsdSlotsStart = bigBucket-> SSDSlotStart;
    //for (auto const& [key, val] : bigBucket->pageIdToSlot){
        //uint64_t oldSsdSlot = bigBucketSsdSlotsStart + val;
        // todo yuval -  actually do something with it here
    //}
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
    byte messageData[MESSAGE_SIZE];
    breakDownUint64ToBytes(bucketId,&messageData[BUCKET_ID_START_INDEX]);
    messageData[MSG_ENUM_IDX] = (byte) FINISHED_BUCKET;
    messageData[MSG_SND_IDX] = (byte) bucketManager->nodeId;
    messageData[MSG_RCV_IDX] = (byte) nodeId;
    auto finishedBucketMsg = Message(messageData);
    sendMessage(finishedBucketMsg);
    // todo - implmenet properly bucketManager->deleteBucket(0);
    // todo - this is to be called after buckets send is actually done
}


///////// Misc functions /////////

void BucketManagerMessageHandler::gossipMessage(Message msg){
    for(uint64_t nodeId: nodeIdsForMessages){
        msg.messageData[MSG_RCV_IDX] = (byte) nodeId;
        sendMessage(msg);
    }
}

void BucketManagerMessageHandler::checkMailbox() { //todo DFD
    uint64_t nodeId = bucketManager->nodeId;
    if(nodeId == 1){
        firstMtx->lock();

        if(firstMessageQueue->empty() == false){

            Message msg = firstMessageQueue->front();
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

            Message msg = secondMessageQueue->front();
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

            Message msg = thirdMessageQueue->front();
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

void BucketManagerMessageHandler::sendMessage(Message msg) { //todo DFD
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

void BucketManagerMessageHandler::breakDownUint64ToBytes(uint64_t input, byte retVal[8]){
    for ( int i = 0; i < 8; i++ ){
        retVal[i] = (byte)(input >> (8 * i) & 0xFF);
    }
}

uint64_t BucketManagerMessageHandler::convertBytesBackToUint64(byte input[8]){
    uint64_t retVal = 0 ;
    for(int i = 0; i<8; i++){
        retVal += ((uint64_t) input[i] << i * 8);
    }
    return retVal;
}

void BucketManagerMessageHandler::doStuff(){}