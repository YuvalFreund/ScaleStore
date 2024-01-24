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
#include "scalestore/storage/buffermanager/Guard.hpp"

struct BucketShuffler{

    static void sendBucketToNode(RemoteBucketShuffleJob bucketShuffleJob,scalestore::rdma::MessageHandler& mh, BucketManagerMessageHandler& bmmh, scalestore::storage::Buffermanager& bm){

        uint64_t bucketId = bucketShuffleJob.bucketId;
        uint64_t nodeId = bucketShuffleJob.nodeId;
        Bucket* bigBucket = &(bmmh.bucketManager.bucketsMap.find(bucketId)->second);
        Bucket* smallBucket;
        map<uint64_t, uint64_t> mapOfNode = bmmh.bucketManager.mergableBucketsForEachNode[nodeId]; // this data is necessary for getting the bucket that will be merged

        // dealing with big bucket first uint64_t bigBucketSsdSlotsStart = bigBucket-> SSDSlotStart;
        // bucket will be blocked from adding (suggest other bucket in Random loop), removing will be ignored
        // this the bucket becomes basically "read only"= no problem to iterate over the map
        bigBucket->lockBucketBeforeShuffle();
        // for efficiency, deleted pages can be ignored
        // todo yuval - export all this to a function. this needs to happen both for small and big bucket
        for (auto const& [key, val] : bigBucket->pageIdToSlot){
            uint64_t pageId = key;
            // todo yuval - send message to receiving bucket in the new node, with new page id
            vector<BucketMessage> msgToNewNodeToAddBucketId = preparePageIdToBeAddedInBucketOfNewNodeMsg(pageId,nodeId,bucketId);
            mh.writeMsgsForBucketManager(msgToNewNodeToAddBucketId);
            auto guard = bm.findFrame<CONTENTION_METHOD::NON_BLOCKING>(pageId, Invalidation(), ctx.bmId);


            // todo yuval- send message to new node, ask to add this frame and mark as "ON_THE_WAY" in BF_STATE, Possession will be updated later
            // todo yuval - create new message, that upon receiving will need to open new frame (find or insert from buffer manager)

            // todo yuval-get frame from buffer manager (find or insert from buffer manager) with exclusive guard
            // if frame is not found - then regard it as if there no possessors
            // todo yuval- mark frame at the LOCAL NODE! as "MOVED_TO_NEW" in BF_STATE

            //case no possessors:
                // todo yuval - if frame is cached, send from cache to new node
                // todo yuval - if frame is not cached - get from SSD
                // todo yuval - send page from cache / ssd and mark as "NO_BODY" in the new location and BF_STATE hot

            // case shared
                // todo yuval - check if any other node is the posssesor as well

                // case shared only by the leaving node -
                    // todo yuval - send page from cache and mark as "NO_BODY" in the new location and BF_STATE hot

                // case shared also by at least one other nodes
                    // todo yuval - no need to send page - just update the possessors in case leaving node is there
                    // todo yuval - send frame to the new node
            // case exclusive
                //todo yuval

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

    static vector<BucketMessage> preparePageIdToBeAddedInBucketOfNewNodeMsg(uint64_t pageId, uint64_t nodeId, uint64_t bucketId){
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
