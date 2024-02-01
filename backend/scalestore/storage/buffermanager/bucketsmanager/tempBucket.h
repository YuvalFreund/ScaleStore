//
// Created by YuvalFreund on 30.01.24.
//

#ifndef SCALESTOREDB_TEMPBUCKET_H
#define SCALESTOREDB_TEMPBUCKET_H


class tempBucket {
    /*
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
};


#endif //SCALESTOREDB_TEMPBUCKET_H
