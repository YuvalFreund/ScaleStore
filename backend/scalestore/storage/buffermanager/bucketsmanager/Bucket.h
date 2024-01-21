//
// Created by YuvalFreund on 28.10.23.
//

#ifndef LOCALTHESIS_BUCKET_H
#define LOCALTHESIS_BUCKET_H

#include <string>
#include <map>
#include <set>
#include <iostream>
#include <stack>
#include <mutex>
#include "BucketManagerDefs.h"
struct Bucket{
public:
    uint64_t BucketId;
    uint64_t SSDSlotStart;
    std::stack<uint16_t> freeSlots;
    std::map<uint64_t,uint16_t> pageIdToSlot; // mapping of page id to SSD slot
    std::mutex bucketLock;
    uint32_t maxPagesByParameter;
    uint64_t bucketIdMaskByParameter;

    Bucket(){}
    Bucket(uint64_t BucketId,uint64_t SSDSlotStart):
            BucketId(BucketId), SSDSlotStart(SSDSlotStart){
        //empty slots stack is fulled of all the available slots
        // todo yuval - this needs to be read from the flags
        initBucketSizeDataByParameter(6);
        for(int64_t i = maxPagesByParameter-1; i >=0 ; i--) {
            freeSlots.push(i);
        }
    }

    uint64_t getSsdSlotStart() const {
        return SSDSlotStart;
    }


    uint64_t getBucketId() const {
        return BucketId;
    }

    /**
     * Find the first empty slots, create a new page id and assigns it.
     * The page id is the bucket id + the slot assigned to it.
     * @return
     */

    uint64_t requestNewPageId(){
        bucketLock.lock();
        uint64_t retVal;
        if(freeSlots.empty()){
            std::cout<<"full bucket"<<std::endl;
            bucketLock.unlock();
            throw std::runtime_error("No empty slots");
        }

        auto freeSlot = freeSlots.top();
        retVal = BucketId & bucketIdMaskByParameter;
        retVal = freeSlot | retVal;
        freeSlots.pop();
        pageIdToSlot.insert(std::pair<uint64_t,uint16_t>(retVal,freeSlot));
        bucketLock.unlock();

        return retVal;
    }

    void mergeBucketIn(Bucket* bucketToMergeIn){
        bucketLock.lock();
        bucketToMergeIn->bucketLock.lock();
        // this should not happen. but- just in case!

        if(canBucketBeMerged(bucketToMergeIn) == false){
            bucketToMergeIn->bucketLock.unlock();
            bucketLock.unlock();
            throw std::runtime_error("Not enough empty slots to merge");
        }
        //for(auto & iter : bucketToMergeIn->pageIdToSlot){
            //uint64_t destSsdSlot = addPageWithPageIdWithNoLock(iter.first);
            //uint64_t srcSsdSlot = bucketToMergeIn->getPageSSDSlotByPageIdNoLock(iter.first);

            // todo - actually copying the data!
        //}
        bucketToMergeIn-> destroyBucketData();
        bucketToMergeIn->bucketLock.unlock();
        bucketLock.unlock();
    }

    bool canBucketBeMerged(Bucket* bucketToMerge){
        bool retVal = true;
        int pagesOfSmallBucket = bucketToMerge->getPagesNumInBucket();
        int freeSlotsInBigBucket = getFreeSlotsNum();
        if(freeSlotsInBigBucket - pagesOfSmallBucket < 0){
            retVal = false;
        }
        return retVal;
    }

    // this function returns the SSD spot for this page
    // no need to lock! this is called only from the merging function
    uint64_t addPageWithPageIdWithNoLock(uint64_t newPageId){
        uint64_t retVal;
        auto freeSlot = freeSlots.top();
        pageIdToSlot.insert(std::pair<uint64_t,uint16_t>(newPageId, freeSlot));
        retVal = SSDSlotStart + freeSlot ;
        freeSlots.pop();
        return retVal;
        // eventually also actually move page
    }

    void removePageId(uint64_t pageId){
        bucketLock.lock();
        auto iter = pageIdToSlot.find(pageId);
        if(iter == pageIdToSlot.end()){
            bucketLock.unlock();
            throw std::runtime_error("Page doesnt exist");
        }
        auto enterValue  = iter->second;
        pageIdToSlot.erase(pageId);
        freeSlots.push(enterValue);
        bucketLock.unlock();
    }

    void removePageIdNoLock(uint64_t pageId){
        auto iter = pageIdToSlot.find(pageId);
        if(iter == pageIdToSlot.end()){
            throw std::runtime_error("Page doesnt exist");
        }
        auto enterValue  = iter->second;
        pageIdToSlot.erase(pageId);
        freeSlots.push(enterValue);
    }

    uint64_t getPageSSDSlotByPageId(uint64_t pageId){
        bucketLock.lock();
        uint64_t retVal;
        uint64_t offset;
        try {
            offset = pageIdToSlot.at(pageId);
        }catch (const std::out_of_range& e) {
            bucketLock.unlock();
            throw std::runtime_error("Page doesnt exist!");
        }
        retVal = SSDSlotStart + offset ;
        bucketLock.unlock();
        return retVal;
    }

    ~Bucket(){

    }
    bool isBucketFull(){
        return freeSlots.empty();
    }
    bool isBucketEmpty(){
        return freeSlots.size() == maxPagesByParameter;
    }
    void printBucketData(){
        std::cout<<"Bucket: " << BucketId << " has "<< getPagesNumInBucket() <<" pages, and "<< freeSlots.size() << " free slots." << std::endl;
    }

    int getFreeSlotsNum(){
        return freeSlots.size();
    }

    int getPagesNumInBucket(){
        return maxPagesByParameter - freeSlots.size();
    }
    uint16_t getSlotIdFromPageId(uint64_t pageId){
        return pageIdToSlot.find(pageId)->second;
    }

    struct bucketsCompare
    {
        inline bool operator() ( Bucket& lhs, Bucket& rhs)
        {
            return (lhs.getPagesNumInBucket() < rhs.getPagesNumInBucket());
        }
    };

    void printBucketDataVerbose(){
        std::cout<<"Printing verbose bucket data. Bucket id: "<<getBucketId() << std::endl;
        printBucketData();
        for(auto itr : pageIdToSlot){
            std::cout <<"page id: "<< itr.first << ", slot id:  " << getPageSSDSlotByPageId(itr.first) << std::endl;
        }
    }

    void destroyBucketData(){
        freeSlots = std::stack<uint16_t>();
        pageIdToSlot.clear();
    }

    uint64_t getPageSSDSlotByPageIdNoLock(uint64_t pageId){
        uint64_t retVal;
        uint64_t offset;
        try {
            offset = pageIdToSlot.at(pageId);
        }catch (const std::out_of_range& e) {
            bucketLock.unlock();
            throw std::runtime_error("Page doesnt exist!");
        }
        retVal = SSDSlotStart + offset ;
        return retVal;
    }

    void initBucketSizeDataByParameter(int bucketIdByteSize){
        switch(bucketIdByteSize){
            case 5:
                maxPagesByParameter = MAX_PAGES_5_BYTES_BUCKET_ID;
                bucketIdMaskByParameter = BUCKET_ID_MASK_5_BYTES_BUCKET_ID;
                break;
            case 6:
                maxPagesByParameter = MAX_PAGES_6_BYTES_BUCKET_ID;
                bucketIdMaskByParameter = BUCKET_ID_MASK_6_BYTES_BUCKET_ID;
                break;
            case 7:
                maxPagesByParameter = MAX_PAGES_7_BYTES_BUCKET_ID;
                bucketIdMaskByParameter = BUCKET_ID_MASK_7_BYTES_BUCKET_ID;
                break;
        }
    }
};
#endif //LOCALTHESIS_BUCKET_H
