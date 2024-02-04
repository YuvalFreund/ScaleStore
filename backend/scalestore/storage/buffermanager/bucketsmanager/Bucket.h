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
#include "scalestore/Config.hpp"
#include "Defs.hpp"

struct Bucket{
public:
    uint64_t BucketId;
    uint64_t SSDSlotStart;
    std::stack<uint16_t> freeSlots;
    std::map<uint64_t,uint16_t> pageIdToSlot; // mapping of page id to SSD slot
    std::mutex bucketLock;
    uint32_t maxPagesByParameter;
    uint64_t bucketIdMaskByParameter;
    std::atomic<bool> bucketIsUnderWay;

    Bucket(){}
    Bucket(uint64_t BucketId,uint64_t SSDSlotStart):
            BucketId(BucketId), SSDSlotStart(SSDSlotStart){
        //empty slots stack is fulled of all the available slots
        initBucketSizeDataByParameter(FLAGS_bucket_id_size_in_bytes);
        for(int64_t i = maxPagesByParameter-1; i >=0 ; i--) {
            freeSlots.push(i);
        }
        bucketIsUnderWay.store(false);
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
        if(freeSlots.empty()|| bucketIsUnderWay.load()){ // when bucket is underway, it becomes read only!
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

    void addNewPageWithPageId(uint64_t pageId){
        bucketLock.lock();
        if(freeSlots.empty() || bucketIsUnderWay.load()){
            bucketLock.unlock();
            throw std::runtime_error("No empty slots");
        }
        auto freeSlot = freeSlots.top();
        freeSlots.pop();
        pageIdToSlot.insert(std::pair<uint64_t,uint16_t>(pageId,freeSlot));
        bucketLock.unlock();
    }

    void lockBucketBeforeShuffle(){
        bucketIsUnderWay.store(true);
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


    uint64_t getPageSSDSlotByPageId(uint64_t pageId){
        bucketLock.lock();
        uint64_t retVal;
        uint64_t offset;
        auto iter = pageIdToSlot.find(pageId);
        if(iter != pageIdToSlot.end()){
            offset = iter->second;
        }
        else{
            bucketLock.unlock();
            throw std::runtime_error("Page doesnt exist!");
        }
        retVal = SSDSlotStart + offset;
        bucketLock.unlock();
        return retVal;
    }

    ~Bucket(){

    }
    bool isBucketFull(){
        return freeSlots.empty();
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


    PageIdJobFromBucket getAnyPageOfBucketToShuffle(){
        PageIdJobFromBucket retVal;
        bucketLock.lock();
        auto check = pageIdToSlot.begin();
        if(check == pageIdToSlot.end()){
            retVal.bucketContainsPages = false;
        }else{
            retVal.bucketContainsPages = true;
            retVal.pageId = check->first;
        }
        bucketLock.unlock();
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
