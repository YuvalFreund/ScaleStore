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
    // todo yuval - at one point replace with optimisitc locking

    struct PageShuffleJob{
        uint64_t nodeId;
        uint64_t pageId;
    };

    std::mutex bucketsJobMutex;
    std::map<uint64_t,uint16_t>::iterator bucketPageIterator;
    std::queue<RemoteBucketShuffleJob> remoteShuffleJobs;
    atomic<bool> initiated = false;
    atomic<bool> workDone = false;
    RemoteBucketShuffleJob currentShuffleJob;
    rdma::MessageHandler& mh;
    BucketManagerMessageHandler& bmmh;
    storage::Buffermanager& bm;

    BucketShuffler(rdma::MessageHandler &mh, BucketManagerMessageHandler &bmmh, storage::Buffermanager &bm) : mh(mh),
                                                                                                              bmmh(bmmh),
                                                                                                              bm(bm) {
    }

    bool doOnePageShuffle(){
        if (initiated == false){
            checkAndInitBucketShuffler();
            return true;
        }else{
            if(workDone){
                return true;
            }else{
                PageShuffleJob jobToExecute = getNextPageToShuffle();

            }
        }
    }

    PageShuffleJob getNextPageToShuffle(){
        PageShuffleJob retVal = PageShuffleJob();
        bucketsJobMutex.lock();
        PageIdJobFromBucket check = bmmh.bucketManager.getPageFromBucketToShuffle(currentShuffleJob.bucketId);
        if(check.bucketContainsPages){
            retVal.nodeId = currentShuffleJob.nodeId;
            retVal.pageId = check.pageId;
        }else{
            updateCurrentShuffleJob();
            PageIdJobFromBucket newCheck = bmmh.bucketManager.getPageFromBucketToShuffle(currentShuffleJob.bucketId);
            retVal.nodeId = currentShuffleJob.nodeId;
            retVal.pageId = newCheck.pageId;
        }
        bucketsJobMutex.unlock();
        return retVal;
    }

    void handleOnePageSend(PageShuffleJob pageShuffleJob){
        // todo yuval - this function needs to return list of bucket ids that were transferred
        BucketManager& bucketManager = bmmh.bucketManager;
        uint64_t pageId = pageShuffleJob.pageId;
        uint64_t newNodeId = pageShuffleJob.nodeId;
        auto clientId = mh.bmmh_cctxs[newNodeId];
        auto ctx = mh.cctxs[clientId];

        vector<BucketMessage> msgToNewNodeToAddBucketId = preparePageIdToBeAddedInBucketOfNewNodeMsg(pageId, newNodeId, bucketId);
        mh.writeMsgsForBucketManager(msgToNewNodeToAddBucketId);

        // send message to new node, ask to add this frame and mark as "ON_THE_WAY" in BF_STATE, Possession will be updated later
        auto onTheWayUpdateRequest = *rdma::MessageFabric::createMessage<rdma::CreateShuffledFrameRequest>(mh.cctxs[newNodeId].request, pageId);
        threads::Worker::my().writeMsg<rdma::CreateShuffledFrameRequest>(newNodeId, onTheWayUpdateRequest);
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
                    threads::Worker::my().writeMsg<rdma::CreateShuffledFrameRequest>(newNodeId, onTheWayUpdateRequest);
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

    void checkAndInitBucketShuffler(){
        bool checkIfQueueReady = bmmh.shuffleQueueIsReady;
        if(checkIfQueueReady){
            bucketsJobMutex.lock();
            remoteShuffleJobs = bmmh.remoteBucketShufflingQueue;
            currentShuffleJob = remoteShuffleJobs.front();
            bmmh.bucketManager.lockBucketBeforeShuffle(currentShuffleJob.bucketId);
            remoteShuffleJobs.pop();
            bucketsJobMutex.unlock(); // todo - think about this bucket locking - maybe the pointer is not enough
            initiated.store(true);
        }
    }

    void updateCurrentShuffleJob(){
        bmmh.bucketManager.deleteBucket(currentShuffleJob.bucketId);
        auto gossipMessages = bmmh.gossipBucketMoved(currentShuffleJob.bucketId,currentShuffleJob.nodeId);
        mh.writeMsgsForBucketManager(gossipMessages);
        // todo yuval - also, deal with small bucket

        bucketsJobMutex.lock();
        remoteShuffleJobs = bmmh.remoteBucketShufflingQueue;
        currentShuffleJob = remoteShuffleJobs.front();
        bmmh.bucketManager.lockBucketBeforeShuffle(currentShuffleJob.bucketId);
        remoteShuffleJobs.pop();
        bucketsJobMutex.unlock();
    }
};



#endif //SCALESTOREDB_BUCKETSHUFFLER_H
