//
// Created by YuvalFreund on 23.01.24.
//

#ifndef SCALESTOREDB_BUCKETSHUFFLER_H
#define SCALESTOREDB_BUCKETSHUFFLER_H
#include "BucketManager.h"
#include "BucketMessage.h"
#include "RemoteBucketShuffleJob.h"
#include "LocalBucketsMergeJob.h"
#include "BucketManagerMessageHandler.h"
#include "scalestore/rdma/MessageHandler.hpp"
#include "scalestore/storage/buffermanager/BufferFrameGuards.hpp"
#include "scalestore/storage/buffermanager/Buffermanager.hpp"

struct BucketShuffler{

    static void sendBucketToNode(RemoteBucketShuffleJob bucketShuffleJob, BucketManager& bucketManager, BucketManagerMessageHandler& bmmh, scalestore::storage::Buffermanager& bufferManager){

        uint64_t bucketId = bucketShuffleJob.bucketId;
        uint64_t nodeId = bucketShuffleJob.nodeId;
        map<uint64_t, uint64_t> mapOfNode = bucketManager.mergableBucketsForEachNode[nodeId];
        Bucket* bigBucket = &(bucketManager.bucketsMap.find(bucketId)->second);
        Bucket* smallBucket;

        // dealing with big bucket firstuint64_t bigBucketSsdSlotsStart = bigBucket-> SSDSlotStart;
        for (auto const& [key, val] : bigBucket->pageIdToSlot){
            //uint64_t oldSsdSlot = bigBucketSsdSlotsStart + val;
            // todo yuval -  actually do something with it here
        }
        bigBucket->destroyBucketData();
        bucketManager.deleteBucket(bucketId);

        if(mapOfNode[bucketId] != BUCKET_ALREADY_MERGED){
            smallBucket = &(bucketManager.bucketsMap.find(mapOfNode[bucketId])->second);
            uint64_t smallBucketSsdSlotsStart = smallBucket-> SSDSlotStart;
            for (auto const& [key, val] : smallBucket->pageIdToSlot){
                //uint64_t oldSsdSlot = smallBucketSsdSlotsStart + val;
                // todo yuval -  actually do something with it here
            }
            smallBucket->destroyBucketData();
            bucketManager.deleteBucket(smallBucket->getBucketId());
        }

        string logMsg = "Node " + std::to_string(bucketManager.nodeId) + " log: " + "finished sending bucket " + std::to_string(bucketId)+ " to node : " + std::to_string(nodeId)+  " \n" ;//todo DFD
        logActivity(logMsg);
        uint8_t messageData[MESSAGE_SIZE];
        breakDownUint64ToBytes(bucketId,&messageData[BUCKET_ID_START_INDEX]);
        messageData[MSG_ENUM_IDX] = (uint8_t) FINISHED_BUCKET;
        messageData[MSG_SND_IDX] = (uint8_t) bucketManager.nodeId;
        messageData[MSG_RCV_IDX] = (uint8_t) nodeId;
        auto finishedBucketMsg = BucketMessage(messageData);
        sendMessage(finishedBucketMsg);
        gossipBucketMoved(bucketId,nodeId);

        retVal = gossipBucketMoved(shuffleJob.bucketId,shuffleJob.nodeId);

    }

    static void merge2bucketsLocally(LocalBucketsMergeJob mergeJob){

    }

    vector<BucketMessage> preparePageIdToBeAddedInBucketOfNewNodeMsg(uint64_t pageId, uint64_t nodeId, uint64_t bucketId){
        vector<BucketMessage> retVal;
        uint8_t messageData[MESSAGE_SIZE];
        BucketManagerMessageHandler::breakDownUint64ToBytes(bucketId,&messageData[BUCKET_ID_START_INDEX]);
        BucketManagerMessageHandler::breakDownUint64ToBytes(pageId,&messageData[PAGE_ID_START_INDEX]);
        messageData[MSG_ENUM_IDX] = (uint8_t) ADD_PAGE_ID_TO_BUCKET;
        messageData[MSG_RCV_IDX] = (uint8_t) nodeId;
        auto addPageIdToBucketMsg = BucketMessage(messageData);
        vector<BucketMessage> msgsToSend;
        retVal.emplace_back(addPageIdToBucketMsg);
        return retVal;
    }
};


#endif //SCALESTOREDB_BUCKETSHUFFLER_H
