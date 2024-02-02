
#include "BucketManagerMessageHandler.h"

vector<BucketMessage> BucketManagerMessageHandler::handleIncomingMessage(BucketMessage msg){
    vector<BucketMessage> retVal;
    string logMsg = "Node " + std::to_string(bucketManager.nodeId) + " log: " + "handleIncomingMessage. \n" ;//todo DFD
    logActivity(logMsg);
    switch(msg.messageEnum){
            // node leaving

        case NODE_LEAVING_THE_CLUSTER_LEAVE:
            retVal = handleNodeLeftTheClusterLeave(msg);
            break;

        case CONSISTENT_HASHING_INFORMATION_SYNCED_LEAVE:
            retVal = handleNewHashingStateSynchronizedLeave(msg);
            break;

        case UNION_FIND_DATA_LEAVE:
            retVal = handleIncomingUnionFindDataLeave(msg);
            break;

        case UNION_FIND_DATA_SEND_MORE:
            retVal = handleUnionFindDataSendMore(msg);
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
        case INCOMING_SHUFFLED_BUCKET_DATA:
            handleIncomingShuffledBucketData(msg);
            break;

        case INCOMING_SHUFFLED_BUCKET_DATA_SEND_MORE:
            handleIncomingShuffledBucketDataSendMore(msg);
            break;

        case INCOMING_SHUFFLED_BUCKET_DATA_RECEIVED_ALL:
            handleIncomingShuffledBucketDataReceivedAll(msg);
            break;

        case ADD_PAGE_ID_TO_BUCKET:
            retVal = handleAddPageIdToBucket(msg);
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

//node leaving handlers

vector<BucketMessage> BucketManagerMessageHandler::handleNodeLeftTheClusterLeave(BucketMessage msg){
    string logMsg = "Node " + std::to_string(bucketManager.nodeId) + " log: " + "handleNodeLeftTheClusterLeave. \n" ;//todo DFD
    logActivity(logMsg);
    vector<BucketMessage> retVal;

    int sendingNode = (int)msg.messageData[MSG_SND_IDX];
    leavingNode = sendingNode;
    consensusVec[CONSISTENT_HASHING_INFORMATION_SYNCED_LEAVE].set(sendingNode - 1);
    auto leavingNodeId = (uint64_t) msg.messageData[MSG_DATA_START_IDX];
    bucketManager.nodeLeftOrJoinedCluster(false,leavingNodeId);

    uint8_t messageData[MESSAGE_SIZE];
    messageData[MSG_ENUM_IDX] = (uint8_t) CONSISTENT_HASHING_INFORMATION_SYNCED_LEAVE;
    messageData[MSG_SND_IDX] = (uint8_t) bucketManager.nodeId;
    auto consistentHashingCompleted = BucketMessage(messageData);
    retVal = collectMessagesToGossip(consistentHashingCompleted);
    return retVal;

}

vector<BucketMessage> BucketManagerMessageHandler::handleNewHashingStateSynchronizedLeave(BucketMessage msg){
    string logMsg = "Node " + std::to_string(bucketManager.nodeId) + " log: " + "handleNewHashingStateSynchronizedLeave. \n" ;//todo DFD
    logActivity(logMsg);
    vector<BucketMessage> retVal;

    bool allNodesUpdated = markBitAndReturnAreAllNodesExcludingSelfTrue(msg);
    if(allNodesUpdated){
        for(auto node : nodeIdsForMessages){ // to kick start the data-more cycle
            unionFindDataForNodes[node] = prepareUnionFindData();
        }
        for(auto node : nodeIdsForMessages){ // to kick start the data-more cycle
            BucketMessage toSend = prepareNextUnionFindDataToSendToNode(node);
            sendMessage(toSend);
            retVal.emplace_back(toSend);
        }
    }
    return retVal;

}

vector<BucketMessage> BucketManagerMessageHandler::handleIncomingUnionFindDataLeave(BucketMessage msg){
    // if this is the leaving bucket, there is no need for to add the union find data to it - it will be deleted
    string logMsg = "Node " + std::to_string(bucketManager.nodeId) + " log: " + "handleIncomingUnionFindDataLeave. \n" ;//todo DFD
    logActivity(logMsg);
    vector<BucketMessage> retVal;
    if(bucketManager.nodeIsToBeDeleted == false){
        addIncomingUnionFindData(msg);
    }
    if(msg.messageData[UNION_FIND_LAST_DATA] == 0){
        uint8_t messageData[MESSAGE_SIZE];
        messageData[MSG_ENUM_IDX] = (uint8_t) UNION_FIND_DATA_SEND_MORE;
        messageData[MSG_SND_IDX] = (uint8_t) bucketManager.nodeId;
        messageData[MSG_RCV_IDX] = (uint8_t) msg.messageData[MSG_SND_IDX];
        auto askForMoreDataMsg = BucketMessage(messageData);
        sendMessage(askForMoreDataMsg);
        retVal.emplace_back(askForMoreDataMsg);
    }else{
        bool receivedFromAllNodes = markBitAndReturnAreAllNodesExcludingSelfTrue(msg);
        if(receivedFromAllNodes){
            retVal = gossipFinishedUnionFind(UNION_FIND_NODE_RECEIVED_ALL_LEAVE);
        }
    }
    return retVal;

}

vector<BucketMessage> BucketManagerMessageHandler::handleUnionFindDataSendMore(BucketMessage msg){
    string logMsg = "Node " + std::to_string(bucketManager.nodeId) + " log: " + "handleUnionFindDataSendMore. \n" ;//todo DFD
    logActivity(logMsg);
    vector<BucketMessage> retVal;
    BucketMessage toSend = prepareNextUnionFindDataToSendToNode(msg.messageData[MSG_SND_IDX]);
    sendMessage(toSend);
    return retVal;
}


vector<BucketMessage> BucketManagerMessageHandler::handleUnionFindDataFinishedAllNodesLeave(BucketMessage msg){
    string logMsg = "Node " + std::to_string(bucketManager.nodeId) + " log: " + "handleUnionFindDataFinishedAllNodesLeave. \n" ;//todo DFD
    logActivity(logMsg);
    vector<BucketMessage> retVal;

    bool allNodesUpdated = markBitAndReturnAreAllNodesExcludingSelfTrue(msg);
    if(allNodesUpdated){
        retVal = prepareBucketAmountsToNodesMessages(BUCKET_AMOUNTS_DATA_LEAVE);
    }
    return retVal;

}

vector<BucketMessage> BucketManagerMessageHandler::handleBucketAmountsDataLeave(BucketMessage msg){
    string logMsg = "Node " + std::to_string(bucketManager.nodeId) + " log: " + "handleBucketAmountsDataLeave. \n" ;//todo DFD
    logActivity(logMsg);
    vector<BucketMessage> retVal;
    bool allNodesUpdated = markBitAndReturnAreAllNodesExcludingSelfTrue(msg);
    if(allNodesUpdated){
        bool isMergeNeeded = bucketManager.updateRequestedBucketNumAndIsMergeNeeded(
                (uint64_t) msg.messageData[MSG_DATA_START_IDX]);
        if (isMergeNeeded){
            mtxForLocalJobsQueue.lock();
            localBucketsMergeJobQueue = bucketManager.getJobsForMergingOwnBuckets();
            localMergeJobsCounter.store(localBucketsMergeJobQueue.size());
            mtxForLocalJobsQueue.unlock();
        }else{
            retVal = gossipBucketAmountFinishedLeave();
        }
    }

    return retVal;

}

vector<BucketMessage> BucketManagerMessageHandler::handleBucketAmountsApprovedLeave(BucketMessage msg){
    string logMsg = "Node " + std::to_string(bucketManager.nodeId) + " log: " + "handleBucketAmountsApprovedLeave. \n" ;//todo DFD
    logActivity(logMsg);
    vector<BucketMessage> retVal;

    bool allNodesUpdated = markBitAndReturnAreAllNodesExcludingSelfTrue(msg);
    if(allNodesUpdated){
        string finished = "Node " + std::to_string(bucketManager.nodeId) + " log: " + "finished synchronizing stage!. \n" ;//todo DFD
        logActivity(finished);
        prepareIncomingBucketsDataForOtherNodes();
        for(auto node : nodeIdsForMessages){
            if(node == bucketManager.nodeId) continue;
            uint8_t messageData[MESSAGE_SIZE];
            messageData[MSG_ENUM_IDX] = (uint8_t) INCOMING_SHUFFLED_BUCKET_DATA;
            messageData[MSG_SND_IDX] = (uint8_t) bucketManager.nodeId;
            messageData[MSG_RCV_IDX] = (uint8_t) node;
            auto dataQueue = bucketShuffleDataForNodes.find(node);
            if(dataQueue == bucketShuffleDataForNodes.end()) continue; // the leaving node will not have data here
            breakDownUint64ToBytes(dataQueue->second.front(),&messageData[BUCKET_ID_START_INDEX]);
            remoteBucketShufflingQueue.push(RemoteBucketShuffleJob(dataQueue->second.front(),node));
            dataQueue->second.pop();
            auto askForMoreDataMsg = BucketMessage(messageData);
            sendMessage(askForMoreDataMsg);  //todo dfd
            retVal.emplace_back(askForMoreDataMsg);
        }
    }
    return retVal;
}

//sending buckets handlers

vector<BucketMessage> BucketManagerMessageHandler::handleAddPageIdToBucket(BucketMessage msg){
    string logMsg = "Node " + std::to_string(bucketManager.nodeId) + " log: " + "handleAddPageIdToBucket. \n" ;//todo DFD
    logActivity(logMsg);
    vector<BucketMessage> retVal;

    uint64_t bucketId = convertBytesBackToUint64(&msg.messageData[BUCKET_ID_START_INDEX]);
    uint64_t pageId = convertBytesBackToUint64(&msg.messageData[PAGE_ID_START_INDEX]);
    auto bucketIter = bucketManager.bucketsMap.find(bucketId);
    bucketIter->second.addNewPageWithPageId(pageId);
    return retVal;
}


vector<BucketMessage> BucketManagerMessageHandler::handleBucketMovedToNewNode(BucketMessage msg){
    string logMsg = "Node " + std::to_string(bucketManager.nodeId) + std::to_string(msg.messageEnum) + " log: " + "handleBucketMovedToNewNode \n";//todo DFD
    logActivity(logMsg);
    vector<BucketMessage> retVal;

    uint64_t bucketId = convertBytesBackToUint64(&msg.messageData[BUCKET_ID_START_INDEX]);
    uint64_t nodeId = convertBytesBackToUint64(&msg.messageData[NODE_ID_START_INDEX]);
    if(nodeId == bucketManager.nodeId){
        bucketsToReceiveFromNodes.erase(bucketId);
        if(bucketsToReceiveFromNodes.empty()){
            string finishMsg = "Node " + std::to_string(bucketManager.nodeId) + " log: " + "FINISHED RECEIVING ALL BUCKETS!. \n" ;//todo DFD
            logActivity(finishMsg);
            uint8_t messageData[MESSAGE_SIZE];
            messageData[MSG_ENUM_IDX] = (uint8_t) NODE_FINISHED_RECEIVING_BUCKETS;
            messageData[MSG_SND_IDX] = (uint8_t) bucketManager.nodeId;
            auto finishedReceivingBucketsMsg = BucketMessage(messageData);
            retVal = collectMessagesToGossip(finishedReceivingBucketsMsg);
            //todo case for the last one - maybe DFD?
            consensusVec[NODE_FINISHED_RECEIVING_BUCKETS].set(bucketManager.nodeId - 1);
            if(consensusVec[NODE_FINISHED_RECEIVING_BUCKETS].all()) {
                moveAtomicallyToNormalState();
            }
        }
    }
    // todo yuval implement this properly- this to be locked!
    bucketManager.bucketCacheMtx.lock();
    bucketManager.bucketIdToNodeCache[bucketId] = nodeId;
    bucketManager.bucketCacheMtx.unlock();
    return retVal;
}

vector<BucketMessage> BucketManagerMessageHandler::handleNodeFinishedReceivingBuckets(BucketMessage msg){
    string logMsg = "Node " + std::to_string(bucketManager.nodeId) + " log: " + "handleNodeFinishedReceivingBuckets. \n" ;//todo DFD
    logActivity(logMsg);
    vector<BucketMessage> retVal;

    bool allNodesUpdated = markBitAndReturnAreAllNodesExcludingSelfTrue(msg);
    if(allNodesUpdated){
        moveAtomicallyToNormalState();
    }
    return retVal;

}




// Node joined / leaving functions


vector<BucketMessage> BucketManagerMessageHandler::gossipNodeLeft(){

    string logMsg = "Node " + std::to_string(bucketManager.nodeId) + " log: " + "gossipNodeLeft. \n" ;//todo DFD
    logActivity(logMsg);
    vector<BucketMessage> retVal;
    leavingNode = bucketManager.nodeId;

    uint8_t messageDataForLeavingNode[MESSAGE_SIZE];
    messageDataForLeavingNode[MSG_ENUM_IDX] = (uint8_t) NODE_LEAVING_THE_CLUSTER_LEAVE;
    messageDataForLeavingNode[MSG_SND_IDX] = (uint8_t) bucketManager.nodeId;
    messageDataForLeavingNode[MSG_DATA_START_IDX] = (uint8_t) bucketManager.nodeId;
    auto bucketAmountFinishedMsg = BucketMessage(messageDataForLeavingNode);
    retVal = collectMessagesToGossip(bucketAmountFinishedMsg);
    bucketManager.nodeLeftOrJoinedCluster(false,bucketManager.nodeId);
    return retVal;

}


// Buckets amounts functions

vector<BucketMessage> BucketManagerMessageHandler::prepareBucketAmountsToNodesMessages(MessagesEnum msgEnum){
    vector<BucketMessage> retVal;

    for (auto itr : nodeIdsForMessages){
        if(itr == bucketManager.nodeId) continue;
        uint8_t messageData[MESSAGE_SIZE];
        messageData[MSG_ENUM_IDX] = (uint8_t) msgEnum;
        messageData[MSG_SND_IDX] = (uint8_t) bucketManager.nodeId;
        messageData[MSG_RCV_IDX] = (uint8_t) itr;
        auto bucketAmountToNodeMsg = BucketMessage(messageData);

        // if this a node that just joined he will have 0 buckets to send to others. it is planned this way.
        uint8_t value = (uint8_t) 0;
        if(bucketManager.bucketAmountsToSendEachNodeAfterMerging.find(itr) !=  bucketManager.bucketAmountsToSendEachNodeAfterMerging.end()) {
            value = (uint8_t) bucketManager.bucketAmountsToSendEachNodeAfterMerging[itr];
        }
        messageData[MSG_DATA_START_IDX] = (uint8_t) value;
        string amountMsg = "Node " + std::to_string(bucketManager.nodeId) + " sends : " + std::to_string((int)value) + " buckets to node: " + std::to_string(itr) + "\n" ;//todo DFD
        logActivity(amountMsg);
        retVal.emplace_back(bucketAmountToNodeMsg);
        sendMessage(bucketAmountToNodeMsg); // todo - dfd
    }
    return retVal;

}


vector<BucketMessage> BucketManagerMessageHandler::gossipBucketAmountFinishedLeave(){
    string logMsg = "Node " + std::to_string(bucketManager.nodeId) + " log: " + "gossipBucketAmountFinishedLeave. \n" ;//todo DFD
    logActivity(logMsg);
    vector<BucketMessage> retVal;

    uint8_t messageData[MESSAGE_SIZE];
    messageData[MSG_ENUM_IDX] = (uint8_t) BUCKETS_AMOUNTS_APPROVED_LEAVE;
    messageData[MSG_SND_IDX] = (uint8_t) bucketManager.nodeId;

    auto bucketAmountFinishedMsg = BucketMessage(messageData);
    retVal = collectMessagesToGossip(bucketAmountFinishedMsg);
    return retVal;

}

vector<BucketMessage> BucketManagerMessageHandler::addIncomingUnionFindData(BucketMessage msg) {

    vector<BucketMessage> retVal;

    uint8_t numberOfBucketDataInMessage = msg.messageData[UNION_FIND_BUCKET_DATA_SENT_IDX];
    string logMsg = "Node " + std::to_string(bucketManager.nodeId) + " log: " + "addIncomingUnionFindData. Buckets data left: " + std::to_string(unionFindTotalAmount) + "number of buckets arriving: " +  std::to_string((int)numberOfBucketDataInMessage) + "\n" ;//todo DFD
    logActivity(logMsg);
    for(int i = 0; i < (int)numberOfBucketDataInMessage; i++){
        uint64_t bucketId = convertBytesBackToBucketId(&msg.messageData[MSG_DATA_START_IDX+ (i * 6)]);
        uint64_t rootId = convertBytesBackToBucketId(&msg.messageData[(MSG_DATA_START_IDX + (i * 6)) + 6 ]);
        bucketManager.putNewUnionFindPairData(bucketId, rootId);
    }
    unionFindTotalAmount -= (int)numberOfBucketDataInMessage;
    if( unionFindTotalAmount == 0){
        retVal = gossipFinishedUnionFind(UNION_FIND_NODE_RECEIVED_ALL_LEAVE);
    }
    return retVal;

}

vector<BucketMessage> BucketManagerMessageHandler::gossipFinishedUnionFind(MessagesEnum msgEnum){
    string logMsg = "Node " + std::to_string(bucketManager.nodeId) + " log: " + "gossipFinishedUnionFind. \n" ;//todo DFD
    logActivity(logMsg);
    vector<BucketMessage> retVal;

    uint8_t messageData[MESSAGE_SIZE];
    messageData[MSG_ENUM_IDX] = (uint8_t) msgEnum;
    messageData[MSG_SND_IDX] = (uint8_t) bucketManager.nodeId;
    auto unionFindFinishedMsg = BucketMessage(messageData);
    retVal = collectMessagesToGossip(unionFindFinishedMsg);
    return retVal;

}


///////// Consensus Vector functions /////////

bool BucketManagerMessageHandler::markBitAndReturnAreAllNodesExcludingSelfTrue(const BucketMessage msg){

    int sendingNode = (int)msg.messageData[MSG_SND_IDX];
    consensusVec[msg.messageEnum].set(sendingNode - 1);
    consensusVec[msg.messageEnum].set(bucketManager.nodeId - 1);
    if(consensusVec[msg.messageEnum].all()){
        consensusVec[msg.messageEnum].reset();
        return true;
    }else{
        return false;
    }
}

///////// buckets sending functions /////////


vector<BucketMessage> BucketManagerMessageHandler::gossipBucketMoved(uint64_t bucketId, uint64_t nodeId){
    vector<BucketMessage> retVal;

    uint8_t messageData[MESSAGE_SIZE];
    breakDownUint64ToBytes(bucketId,&messageData[BUCKET_ID_START_INDEX]);
    messageData[MSG_ENUM_IDX] = (uint8_t) BUCKET_MOVED_TO_NEW_NODE;
    messageData[MSG_SND_IDX] = (uint8_t) bucketManager.nodeId;
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
        auto bucketMsg = BucketMessage(msg.messageData);
        msg.messageData[MSG_RCV_IDX] = (uint8_t) nodeId;
        retVal.emplace_back(bucketMsg);
        sendMessage(msg); // todo -DFD
    }
    return retVal;

}


void BucketManagerMessageHandler::sendMessage(BucketMessage msg) { //todo DFD
    auto receivingNodeId = (uint64_t) msg.messageData[MSG_RCV_IDX];
    string logMsg = "Node " + std::to_string(bucketManager.nodeId) + " log: " + "write msg to node: " +  std::to_string(receivingNodeId) + "\n" ;//todo DFD
    logActivity(logMsg);
}

void BucketManagerMessageHandler::logActivity(string const& str){
    char *cstr = new char[str.length() + 1];
    strcpy(cstr, str.c_str());
    fprintf(bmmhLogFile,"%s",cstr);
    loggerFlushCounter++;
    //if(loggerFlushCounter % 20 == 0){ // todo yuval - if this causes issues ignore it
        fflush(bmmhLogFile);
    //}
}

void BucketManagerMessageHandler::moveAtomicallyToNormalState(){
    string finishedMsg = "Node " + std::to_string(bucketManager.nodeId) + " log: " + "moveAtomicallyToNormalState. \n" ;//todo DFD
    logActivity(finishedMsg);
    bucketsToReceiveFromNodes.clear();
    bucketManager.atomicallyMoveToNormal();
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

LocalBucketsMergeJob BucketManagerMessageHandler::getMergeJob(){
    // todo yuval - is there a less time consuming way of doing this?
    unsigned long queueSize = localMergeJobsCounter.load();
    LocalBucketsMergeJob retVal;
    if(queueSize > 0){
        mtxForLocalJobsQueue.lock();
        if(localBucketsMergeJobQueue.empty() == false){ // this is to protect fro getting the lock and then queue is empty
            retVal = localBucketsMergeJobQueue.front();
            localBucketsMergeJobQueue.pop();
            localMergeJobsCounter.store(localBucketsMergeJobQueue.size());
            retVal.lastMerge = localBucketsMergeJobQueue.empty();
            mtxForLocalJobsQueue.unlock();
        }else{
            mtxForLocalJobsQueue.unlock();
        }
    }
    return retVal;
}

queue<pair<uint64_t,uint64_t>> BucketManagerMessageHandler::prepareUnionFindData(){

    string logMsg = "Node " + std::to_string(bucketManager.nodeId) + " log: " + "prepareUnionFindData. \n" ;//todo DFD
    logActivity(logMsg);
    auto retVal = queue<pair<uint64_t,uint64_t>>();
    for ( const auto &unionFindPair :   bucketManager.getDisjointSets().getMap() ) {
        retVal.push({unionFindPair.first,unionFindPair.second});
    }
    return retVal;
}

BucketMessage BucketManagerMessageHandler::prepareNextUnionFindDataToSendToNode(uint64_t nodeId){
    uint8_t messageData[MESSAGE_SIZE];
    messageData[MSG_ENUM_IDX] = (uint8_t) UNION_FIND_DATA_LEAVE;
    messageData[MSG_SND_IDX] = (uint8_t) bucketManager.nodeId;
    messageData[MSG_RCV_IDX] = (uint8_t)nodeId;
    int i = 0;
    while(i < UNION_FIND_DATA_MAX_AMOUNT && unionFindDataForNodes.find(nodeId)->second.empty()== false){
        breakDownBucketIdToBytes(unionFindDataForNodes.find(nodeId)->second.front().first,&messageData[MSG_DATA_START_IDX + (6 * i)]);
        breakDownBucketIdToBytes(unionFindDataForNodes.find(nodeId)->second.front().second,&messageData[MSG_DATA_START_IDX + (6 * i) + 6]);
        unionFindDataForNodes.find(nodeId)->second.pop();
        i++;
    }
    messageData[UNION_FIND_BUCKET_DATA_SENT_IDX] = (uint8_t) i;
    if(unionFindDataForNodes.find(nodeId)->second.empty()){
        messageData[UNION_FIND_LAST_DATA] = (uint8_t) 1;
    }else{
        messageData[UNION_FIND_LAST_DATA] = (uint8_t) 0;
    }
    BucketMessage retVal = BucketMessage(messageData);
    return retVal;
}
void BucketManagerMessageHandler::prepareIncomingBucketsDataForOtherNodes(){ //pair is <bucket id , node>
    string logMsg = "Node " + std::to_string(bucketManager.nodeId) + " log: " + "prepareOtherNodesForIncomingBuckets. \n" ;//todo DFD
    logActivity(logMsg);
    vector<pair<uint64_t,uint64_t>> bucketsAndNodes =  bucketManager.getBucketsShufflePrioritiesAndNodes();
    for(auto pair : bucketsAndNodes){
        bucketShuffleDataForNodes.find(pair.second)->second.push(pair.first);
    }
}
vector<BucketMessage> BucketManagerMessageHandler::handleIncomingShuffledBucketData(BucketMessage msg){
    string logMsg = "Node " + std::to_string(bucketManager.nodeId) + " log: " + "handleIncomingShuffledBucketData. \n" ;//todo DFD
    logActivity(logMsg);
    vector<BucketMessage> retVal;
    uint64_t newBucketId = convertBytesBackToUint64(&msg.messageData[BUCKET_ID_START_INDEX]);
    bucketManager.createNewBucket(false, newBucketId);
    bucketsToReceiveFromNodes.insert(newBucketId);
    if(msg.messageData[SHUFFLED_BUCKET_LAST_DATA] == 1){
        bool receivedFromAllNodes = markBitAndReturnAreAllNodesExcludingSelfTrue(msg);
        if(receivedFromAllNodes){
            uint8_t messageData[MESSAGE_SIZE];
            messageData[MSG_SND_IDX] = (uint8_t) bucketManager.nodeId;
            messageData[MSG_ENUM_IDX] = (uint8_t) INCOMING_SHUFFLED_BUCKET_DATA_RECEIVED_ALL;
            auto allShuffledBucktsDataArrived = BucketMessage(messageData);
            retVal = collectMessagesToGossip(allShuffledBucktsDataArrived);
            //edge case check for last one the shuffle data
            consensusVec[INCOMING_SHUFFLED_BUCKET_DATA_RECEIVED_ALL].set(bucketManager.nodeId-1);
            bool requiresSecondCheck = consensusVec[INCOMING_SHUFFLED_BUCKET_DATA_RECEIVED_ALL].all();
            if(requiresSecondCheck){
                string finsihedMessage = "o########################ok######################### \n" ;//todo DFD
                logActivity(finsihedMessage);
            }
        }

    }else{
        uint8_t messageData[MESSAGE_SIZE];
        messageData[MSG_ENUM_IDX] = (uint8_t) INCOMING_SHUFFLED_BUCKET_DATA_SEND_MORE;
        messageData[MSG_SND_IDX] = (uint8_t) bucketManager.nodeId;
        messageData[MSG_RCV_IDX] = msg.messageData[MSG_SND_IDX];
        auto askForMoreDataMsg = BucketMessage(messageData);
        sendMessage(askForMoreDataMsg);  //todo dfd
        retVal.emplace_back(askForMoreDataMsg);

    }
    return retVal;
}
vector<BucketMessage> BucketManagerMessageHandler::handleIncomingShuffledBucketDataSendMore(BucketMessage msg){
    string logMsg = "Node " + std::to_string(bucketManager.nodeId) + " log: " + "handleIncomingShuffledBucketDataSendMore. \n" ;//todo DFD
    logActivity(logMsg);
    vector<BucketMessage> retVal;
    uint8_t messageData[MESSAGE_SIZE];
    messageData[MSG_ENUM_IDX] = (uint8_t) INCOMING_SHUFFLED_BUCKET_DATA;
    messageData[MSG_SND_IDX] = (uint8_t) bucketManager.nodeId;
    messageData[MSG_RCV_IDX] = (uint8_t) msg.messageData[MSG_SND_IDX];
    auto dataQueue = bucketShuffleDataForNodes.find(msg.messageData[MSG_SND_IDX]);
    breakDownUint64ToBytes(dataQueue->second.front(),&messageData[BUCKET_ID_START_INDEX]);
    dataQueue->second.pop();
    if(dataQueue->second.empty()){
        messageData[SHUFFLED_BUCKET_LAST_DATA] = 1;
    }else{
        messageData[SHUFFLED_BUCKET_LAST_DATA] = 0;
    }
    auto moreShuffledBucketData = BucketMessage(messageData);
    sendMessage(moreShuffledBucketData);  //todo dfd
    retVal.emplace_back(moreShuffledBucketData);
    return retVal;

}
vector<BucketMessage>BucketManagerMessageHandler::handleIncomingShuffledBucketDataReceivedAll(BucketMessage msg){
    string logMsg = "Node " + std::to_string(bucketManager.nodeId) + " log: " + "handleIncomingShuffledBucketDataReceivedAll. \n" ;//todo DFD
    logActivity(logMsg);
    vector<BucketMessage> retVal;
    consensusVec[INCOMING_SHUFFLED_BUCKET_DATA_RECEIVED_ALL].set(leavingNode-1);
    bool receivedFromAllNodes = markBitAndReturnAreAllNodesExcludingSelfTrue(msg);
    if(receivedFromAllNodes){
        string finsihedMessage = "o########################ok######################### \n" ;//todo DFD
        logActivity(finsihedMessage);
    }
    return retVal;

}