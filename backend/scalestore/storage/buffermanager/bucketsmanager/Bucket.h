#ifndef RANGESATTEMPT_BUCKET_H
#define RANGESATTEMPT_BUCKET_H
#define MAX_PAGES 65536
#define BUCKET_ID_MASK 0xFFFFFFFFFFFF0000
#define PAGE_ID_MASK 0x00000000000000FF
#define PAGE_SIZE_IN_BYTES 4096
#include <string>
#include <map>
#include <set>
#include <iostream>
#include <stack>
#include <mutex>
#include <shared_mutex>

struct Bucket{
public:
    uint64_t BucketId;
    uint64_t SSDSlotStart;
    std::stack<uint16_t> freeSlots;
    std::map<uint64_t,uint16_t> pageIdToSlot; // mapping of page id to SSD slot
    std::mutex readWriteLock;

    Bucket(){}
    Bucket(uint64_t BucketId,uint64_t SSDSlotStart):
            BucketId(BucketId), SSDSlotStart(SSDSlotStart){
        //empty slots stack is fulled of all the available slots
        for(int64_t i = MAX_PAGES-1; i >=0 ; i--) {
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
        readWriteLock.lock();
        uint64_t retVal;
        if(freeSlots.empty()){
            std::cout<<"full bucket"<<std::endl;
            readWriteLock.unlock();
            throw std::runtime_error("No empty slots");
        }

        auto freeSlot = freeSlots.top();
        retVal = BucketId & BUCKET_ID_MASK;
        retVal = freeSlot | retVal;
        freeSlots.pop();
        pageIdToSlot.insert(std::pair<uint64_t,uint16_t>(retVal,freeSlot));
        if(retVal % 10000 == 0 ){
            //std::cout<<"add page id: "<< retVal << "to bucket id: "<<BucketId <<std::endl;
        }
        readWriteLock.unlock();

        return retVal;
    }

    void mergeBucketIn(Bucket* bucketToMerge){
        if(canBucketBeMerged(bucketToMerge) == false){
            throw std::runtime_error("Not enough empty slots to merge");
        }
        for(auto & iter : bucketToMerge->pageIdToSlot){
            addPageWithPageId(iter.first);
        }

    }

    bool canBucketBeMerged(Bucket* bucketToMerge){
        bool retVal;
        if(bucketToMerge->getPagesNumInBucket() + getFreeSlotsNum() > MAX_PAGES){
            retVal = false;
        }
        return retVal;
    }

    void addPageWithPageId(uint64_t newPageId){
        readWriteLock.lock();
        auto freeSlot = freeSlots.top();
        pageIdToSlot.insert(std::pair<uint64_t,uint16_t>(newPageId, freeSlot));
        freeSlots.pop();
        readWriteLock.unlock();

        // eventually also actually move page
    }

    void removePageId(uint64_t pageId){
        readWriteLock.lock();
        auto iter = pageIdToSlot.find(pageId);
        if(iter == pageIdToSlot.end()){
            std::cout<<"YUVAL CHECK 7"<<std::endl;
            throw std::runtime_error("Page doesnt exist");
        }
        auto enterValue  = iter->second;
        pageIdToSlot.erase(pageId);
        freeSlots.push(enterValue);
        readWriteLock.unlock();
    }

    uint64_t getPageSSDSlotByPageId(uint64_t pageId){
        uint64_t retVal;
        readWriteLock.lock();
        retVal = SSDSlotStart + (pageIdToSlot.at(pageId) );
        readWriteLock.unlock();
        return retVal;
    }

    ~Bucket(){

    }
    bool isBucketFull(){
        return freeSlots.empty();
    }
    bool isBucketEmpty(){
        return freeSlots.size() == MAX_PAGES;
    }
    void printBucketData(){
        std::cout<<"Bucket: " << BucketId << " has "<< freeSlots.size() << " free slots." << std::endl;
    }

    int getFreeSlotsNum(){
        return freeSlots.size();
    }

    int getPagesNumInBucket(){
        return MAX_PAGES - freeSlots.size();
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


};


#endif //RANGESATTEMPT_BUCKET_H
