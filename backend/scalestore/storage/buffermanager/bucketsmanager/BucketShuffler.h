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
        uint64_t bucketId;
    };

    std::mutex bucketsJobMutex;
    std::map<uint64_t,uint16_t>::iterator bucketPageIterator;
    std::queue<RemoteBucketShuffleJob> remoteShuffleJobs;
    atomic<bool> initiated = false;
    atomic<bool> workDone = false;
    bool bigBucketShuffle = false;
    RemoteBucketShuffleJob currentShuffleJob;
    rdma::MessageHandler& mh;
    BucketManagerMessageHandler& bmmh;
    storage::Buffermanager& bm;

    BucketShuffler(rdma::MessageHandler &mh, BucketManagerMessageHandler &bmmh, storage::Buffermanager &bm) : mh(mh),
                                                                                                              bmmh(bmmh),
                                                                                                              bm(bm) {
    }

    bool doOnePageShuffleAndReturnIsFinished(){
        if (initiated == false){
            checkAndInitBucketShuffler();
            return false;
        }else{
            if(workDone){
                return true;
            }else{
                PageShuffleJob jobToExecute = getNextPageToShuffle();
                handleOnePageSend(jobToExecute);
                return false;
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
            retVal.bucketId = currentShuffleJob.bucketId;
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
       // BucketManager& bucketManager = bmmh.bucketManager;
        uint64_t pageId = pageShuffleJob.pageId;
        uint64_t newNodeId = pageShuffleJob.nodeId;
        auto clientId = mh.bmmh_cctxs[newNodeId];
        auto ctx = mh.cctxs[clientId];

        // send message to new node, ask to add this frame and mark as "ON_THE_WAY" in BF_STATE, Possession will be updated later
        auto guard = bm.findFrame<storage::CONTENTION_METHOD::BLOCKING>(PID(pageId), rdma::MessageHandler::Invalidation(), newNodeId);
        ensure(guard.state != storage::STATE::UNINITIALIZED);
        ensure(guard.state != storage::STATE::NOT_FOUND);
        ensure(guard.state != storage::STATE::RETRY);
        guard.frame->state = storage::BF_STATE::MOVED_TO_NEW;
        auto onTheWayUpdateRequest = *rdma::MessageFabric::createMessage<rdma::CreateOrUpdateShuffledFrameRequest>(mh.cctxs[newNodeId].request, pageId, guard.frame->possessors,guard.frame->possession);
        threads::Worker::my().writeMsg<rdma::CreateOrUpdateShuffledFrameRequest>(newNodeId, onTheWayUpdateRequest);
        guard.frame->latch.unlatchExclusive();


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
        remoteShuffleJobs = bmmh.remoteBucketShufflingQueue;
        if(bigBucketShuffle){
            bigBucketShuffle = false;
            map<uint64_t, uint64_t> mergableBucketsToSendToNode = bmmh.bucketManager.mergableBucketsForEachNode[currentShuffleJob.nodeId];
            if(mergableBucketsToSendToNode[currentShuffleJob.bucketId] != BUCKET_ALREADY_MERGED){
                currentShuffleJob = RemoteBucketShuffleJob(mergableBucketsToSendToNode[currentShuffleJob.bucketId],currentShuffleJob.nodeId);
            }
        }else{
            currentShuffleJob = remoteShuffleJobs.front();
        }
        bmmh.bucketManager.lockBucketBeforeShuffle(currentShuffleJob.bucketId);
        remoteShuffleJobs.pop();
        bucketsJobMutex.unlock();
        }
    };



#endif //SCALESTOREDB_BUCKETSHUFFLER_H
