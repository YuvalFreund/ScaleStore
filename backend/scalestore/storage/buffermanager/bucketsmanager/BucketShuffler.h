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
using namespace scalestore;

struct BucketShuffler{

    static void sendBucketToNode(RemoteBucketShuffleJob bucketShuffleJob,rdma::MessageHandler& mh, BucketManagerMessageHandler& bmmh, storage::Buffermanager& bm){
        // todo yuval - this function needs to return list of bucket ids that were transferred
        BucketManager& bucketManager = bmmh.bucketManager;
        uint64_t bucketId = bucketShuffleJob.bucketId;
        uint64_t newNodeId = bucketShuffleJob.nodeId;
        uint64_t leavingNodeId = bucketManager.nodeId;
        map<uint64_t, uint64_t> mapOfNode = bucketManager.mergableBucketsForEachNode[newNodeId]; // this data is necessary for getting the bucket that will be merged
        Bucket* bigBucket = &(bucketManager.bucketsMap.find(bucketId)->second);
        //Bucket* smallBucket;

        // dealing with big bucket first uint64_t bigBucketSsdSlotsStart = bigBucket-> SSDSlotStart;

        // bucket will be blocked from adding (suggest other bucket in Random loop), removing will be ignored
        // this the bucket becomes basically "read only"= no problem to iterate over the map
        bigBucket->lockBucketBeforeShuffle();
        // todo yuval - export all this to a function. this needs to happen both for small and big bucket
        for (auto const& [key, val] : bigBucket->pageIdToSlot){
            uint64_t pageId = key;
            // todo yuval - send message to receiving bucket in the new node, with new page id
            vector<BucketMessage> msgToNewNodeToAddBucketId = preparePageIdToBeAddedInBucketOfNewNodeMsg(pageId, newNodeId, bucketId);
            mh.writeMsgsForBucketManager(msgToNewNodeToAddBucketId);

            // send message to new node, ask to add this frame and mark as "ON_THE_WAY" in BF_STATE, Possession will be updated later
            auto onTheWayUpdateRequest = *rdma::MessageFabric::createMessage<rdma::SendShuffledFrameRequest>(mh.cctxs[newNodeId].request, pageId);
            threads::Worker::my().writeMsg<rdma::SendShuffledFrameRequest>(newNodeId, onTheWayUpdateRequest);
            // todo yuval - create new message, that upon receiving will need to open new frame (find or insert from buffer manager)

            // get frame from buffer manager (find or insert from buffer manager) with exclusive guard
            //  mark frame at the LOCAL NODE as "MOVED_TO_NEW" in BF_STATE
            // todo yuval ask tobi - the way it is implemented here, what contention method and functor should be used
            auto guard = bm.findFrame<storage::CONTENTION_METHOD::BLOCKING>(PID(pageId), rdma::MessageHandler::Copy(), newNodeId);
            ensure(guard.state != storage::STATE::UNINITIALIZED);
            ensure(guard.state != storage::STATE::NOT_FOUND);
            ensure(guard.state != storage::STATE::RETRY);
            auto oldBFState = guard.frame->state.load();
            guard.frame->state = storage::BF_STATE::MOVED_TO_NEW;
            // todo yuval - maybe release guard here ?
            auto checkPossession = guard.frame->possession;
            switch(checkPossession){
                case storage::POSSESSION::NOBODY : {
                    // todo yuval ask tobi - is this how to make sure a frame is in memory?
                    if(oldBFState ==  storage::BF_STATE::HOT || oldBFState == storage::BF_STATE::EVICTED){

                        // todo yuval - if frame is cached, send from cache to new node
                    }else{

                        // todo yuval - if frame is not cached - get from SSD
                    }

                    // todo yuval - send page from cache / ssd and mark as "NO_BODY" in the new location and BF_STATE hot

                    break;

                }

                case storage::POSSESSION::SHARED : {

                    bool nodeIdIsSolePossessor = isNodeIdSolePossessor(guard.frame,leavingNodeId); // todo yuval- is this dangerous? frame is now "moved to new" ...
                    if(nodeIdIsSolePossessor){
                        // case shared only by the leaving node -
                        // todo yuval - send page from cache and mark as "NO_BODY" in the new location and BF_STATE hot
                       // auto shuffledFrameArrivedUpdate = *rdma::MessageFabric::createMessage<rdma::UpdateShuffledFrameArrived>(mh.cctxs[newNodeId].request, pageId);
                        threads::Worker::my().writeMsg<rdma::SendShuffledFrameRequest>(newNodeId, onTheWayUpdateRequest);
                    } else {
                        // todo yuval - no need to send page - just update the possessors in case leaving node is there

                    }
                    break;
                }

                case storage::POSSESSION::EXCLUSIVE : {

                    break;
                }


            }
            guard.frame->latch.unlatchExclusive();
        }
        bigBucket->destroyBucketData();
        bmmh.bucketManager.deleteBucket(bucketId);
        /*
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
*/
    }
    // TODO YUVAL -implement this
    /*static void merge2bucketsLocally(LocalBucketsMergeJob mergeJob){

    }*/

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

    static bool isNodeIdSolePossessor(storage::BufferFrame* bf, uint64_t nodeId){
        bool retVal;
        bool leavingNodePossession = bf->possessors.shared.test(nodeId);
        bf->possessors.shared.reset(nodeId);
        retVal = bf->possessors.shared.none();
        if(leavingNodePossession){
            bf->possessors.shared.set(nodeId);
        }

        return retVal;
    }
};



#endif //SCALESTOREDB_BUCKETSHUFFLER_H
