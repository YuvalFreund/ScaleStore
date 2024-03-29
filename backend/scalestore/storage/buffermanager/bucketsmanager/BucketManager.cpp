//
// Created by YuvalFreund on 22.11.23.
//
#include "BucketManager.h"

uint64_t BucketManager::addNewPage(){
    bool availableBucketFound = false;
    uint64_t retVal;
    int tryNum = 1;
    while(availableBucketFound == false){
        uint64_t randIndex = rand() % bucketsNum;
        if(availableBucketsBitSet.test(randIndex)){
            availableBucketFound = true;
            uint64_t chosenBucketId = bucketIds[randIndex];
            auto bucketIter = bucketsMap.find(chosenBucketId);
            try{
                retVal = bucketIter->second.requestNewPageId();
                // check now if bucket got full due to this insert
                if(bucketIter->second.isBucketFull()){
                    availableBucketsBitSet.reset(randIndex);
                    tryNum++;
                }
            }catch (const runtime_error& error){
                std::cout<<error.what()<<std::endl;
                std::cout<<"runtime error putting new page"<<std::endl;
                availableBucketFound = false;
                tryNum++;
            }
        }
        if(tryNum == MAX_TRY_TO_ADD_TO_BUCKET){
            // todo yuval - this will be replaced with b-tree
            createNewBucket(false,ZERO);
        }
    }
    if(retVal % 200000 == 0){
        //printNodeData();
    }
    return retVal;
}

void BucketManager::removePage(uint64_t pageId){
    uint64_t bucketId = pageId & bucketIdMaskByParameter;
    uint64_t realBucketId = disjointSets.find(bucketId);
    auto wantedBucket = bucketsMap.find(realBucketId);
    wantedBucket->second.removePageId(pageId);
}

uint64_t BucketManager::getPageSSDSlotInSelfNode(uint64_t pageId) {
    uint64_t retVal;
    uint64_t bucketId = pageId & bucketIdMaskByParameter;
    auto checkBucket = bucketsMap.find(bucketId); //this is not only an optimization but important for when merging buckets
    if(checkBucket != bucketsMap.end()){
        try{
            retVal = checkBucket->second.getPageSSDSlotByPageId(pageId); // this is the actual mapping
        }catch (const runtime_error& error){
            uint64_t updatedBucketId = disjointSets.find(bucketId);
            retVal = bucketsMap.find(updatedBucketId)->second.getPageSSDSlotByPageId(pageId);
        }

    }else{
        uint64_t realBucketId = disjointSets.find(bucketId);
        retVal = bucketsMap.find(realBucketId)->second.getPageSSDSlotByPageId(pageId);
    }

    return retVal;
}



uint64_t BucketManager::getNodeIdOfPage(uint64_t pageId, bool searchOldRing){

    uint64_t bucketId = pageId & bucketIdMaskByParameter;
    uint64_t foundNodeId = getNodeIdOfBucket(bucketId, false, searchOldRing);

    return foundNodeId;
}


uint64_t BucketManager::getNodeIdOfBucket(uint64_t bucketId, bool fromInitStage, bool searchOldRing){
    uint64_t res = INVALID_NODE_ID;
    if(searchOldRing){
        //bucketCacheMtx.lock(); // todo yuval - this needs to be locked optimistically, regardless of other places in code using mtx
        auto bucketIdCacheIter = bucketIdToNodeCache.find(bucketId);
        //bucketCacheMtx.unlock();
        if(bucketIdCacheIter != bucketIdToNodeCache.end()){
            return bucketIdCacheIter->second;
        }
    }
    std::map<uint64_t, uint64_t> *mapToSearch = searchOldRing ? (&nodesRingLocationMap ) : (&newNodesRingLocationMap);
    std::vector<uint64_t> * vectorToSearch = searchOldRing ? (&nodeRingLocationsVector) : (&newNodeRingLocationsVector);

    uint64_t l = 0;
    uint64_t r = vectorToSearch->size() - 1;
    // edge case for cyclic operation
    if(bucketId < vectorToSearch->at(l) || bucketId > vectorToSearch->at(r)) {
        auto itr = mapToSearch->find(vectorToSearch->at(r));
        ensure(itr != mapToSearch->end());
        return itr->second;
    }
    // binary search
    while (l <= r) {
        uint64_t m = l + (r - l) / 2;
        if (vectorToSearch->at(m) <= bucketId && vectorToSearch->at(m + 1) > bucketId) {
            auto itr = mapToSearch->find(vectorToSearch->at(r));
            ensure(itr != mapToSearch->end());
            res = itr->second;
            break;
        }
        if (vectorToSearch->at(m) < bucketId) {
            l = m + 1;
        } else{
            r = m - 1;
        }
    }

    // from init stage checks that we don't just add buckets that are not actually chosen in the end
    if(searchOldRing && fromInitStage == false){
        bucketCacheMtx.lock();
        bucketIdToNodeCache[bucketId] = res;
        bucketCacheMtx.unlock();
    }
    return res;
}

//////////////BUCKETS MANAGEMENT///////////////////

// we merge small bucket into the big bucket
void BucketManager::mergeSmallBucketIntoBigBucket(LocalBucketsMergeJob localBucketMergeJob){
    bucketMapMtx.lock();
    auto bigBucket = bucketsMap.find(localBucketMergeJob.bigBucket);
    auto smallBucket = bucketsMap.find(localBucketMergeJob.smallBucket);
    //std::cout << "Merging buckets! Big bucket:  " << bigBucketId << " Small bucket: " << smallBucketId << std::endl;

    bigBucket->second.mergeBucketIn(&smallBucket->second);
    disjointSets.Union(localBucketMergeJob.bigBucket,localBucketMergeJob.smallBucket);
    deleteBucket(localBucketMergeJob.smallBucket);
    bucketMapMtx.unlock();
}

uint64_t BucketManager::createNewBucket(bool isNewBucketIdNeeded, uint64_t givenBucketId){
    uint64_t retVal;
    bucketMapMtx.lock();
    uint64_t newBucketId;
    if(isNewBucketIdNeeded){
        bucketsFreeSSDSlots.pop();
        newBucketId = bucketIds[freeBucketIdIndex];
        newBucketId = newBucketId& bucketIdMaskByParameter;
        freeBucketIdIndex++;
    }else{
        newBucketId = givenBucketId;
    }
    retVal = bucketsFreeSSDSlots.top();
    bucketsMap.try_emplace(newBucketId, newBucketId, retVal);
    bucketsNum++;
    bucketMapMtx.unlock();
    return retVal;
}

 map<uint64_t, uint64_t> BucketManager::findMergableBuckets(vector<pair<uint64_t, uint64_t>> bucketsSizes, int * nonMergedBucketsAmount){
    map<uint64_t, uint64_t> retVal;
    sort(bucketsSizes.rbegin(), bucketsSizes.rend());
    uint64_t mergableBucketsAmount = bucketsSizes.size();
    for(uint64_t i = 0; i<mergableBucketsAmount; i++){
        if(bucketsSizes[i].first == BUCKET_ALREADY_MERGED){
            continue; // this mean this bucket was already matched, so there are no more possible matches
        }
        for(uint64_t j = i+1; j < mergableBucketsAmount; j++){
            if(bucketsSizes[j].first == BUCKET_ALREADY_MERGED){
                continue; // this mean this bucket was already matched, therefore continue
            }
            if(bucketsSizes[i].first + bucketsSizes[j].first < maxPagesByParameter){ // buckets can be merged
                bucketsSizes[i].first = BUCKET_ALREADY_MERGED;
                bucketsSizes[j].first = BUCKET_ALREADY_MERGED;
                retVal.insert(make_pair(bucketsSizes[i].second,bucketsSizes[j].second));
                break;
            }
        }
    }
    int bucketsNotMergedAmounts = 0;
    for(uint64_t i = 0; i<mergableBucketsAmount; i++) {
        if (bucketsSizes[i].first != BUCKET_ALREADY_MERGED) {
            // this means this bucket was not matched there for needed to be added separately
            retVal.insert(make_pair(bucketsSizes[i].second, BUCKET_ALREADY_MERGED));
            bucketsNotMergedAmounts++;
        }
    }
    *nonMergedBucketsAmount = bucketsNotMergedAmounts;
    return retVal;
}

///////COMMUNICATIONS WITH OTHER BUCKETS///////////

void BucketManager::nodeLeftOrJoinedCluster(bool nodeJoined, uint64_t leftOrJoinedNodeId){
    if(leftOrJoinedNodeId == nodeId && nodeJoined == false){
        nodeIsToBeDeleted = true;
    }
    bucketsLeavingNum = 0;
    updateConsistentHashingData(nodeJoined,leftOrJoinedNodeId);
    auto bucketDesignatedForEachNode = getBucketsIdsAndSizeToSendToNodes();
    int nonMergedBuckets = 0;
    for (auto & [key, value] : bucketDesignatedForEachNode) {
        mergableBucketsForEachNode[key] = findMergableBuckets(value,&nonMergedBuckets);
        bucketAmountsToSendEachNodeAfterMerging[key] = value.size() - mergableBucketsForEachNode[key].size() + nonMergedBuckets;
        if(key != nodeId){
            bucketsLeavingNum += bucketAmountsToSendEachNodeAfterMerging[key];
        }else{
            aggregatedBucketNum = bucketAmountsToSendEachNodeAfterMerging[key];
        }
    }
    duplicateDisjointSetsAndMergeNew(); // this include new merges
}


// this function returns a map from node id to a vector of pairs,
// where each pair is a bucket id and his size (page num in bucket)
std::map<uint64_t,std::vector<pair<uint64_t,uint64_t>>> BucketManager::getBucketsIdsAndSizeToSendToNodes(){
    std::map<uint64_t,std::vector<pair<uint64_t,uint64_t>>> retVal;
    uint64_t tempNodeId;
    for ( auto & [key, value] : bucketsMap){
        tempNodeId = getNodeIdOfBucket(value.getBucketId(), false,true);
        retVal[tempNodeId].emplace_back(make_pair(value.getPagesNumInBucket(),value.getBucketId()));
    }
    return retVal;
}

/////////////////INIT FUNCTIONS////////////////////


void BucketManager::fullBucketManagerInit(const std::vector<uint64_t> nodeIdsInput){

    for(const unsigned long long & i : nodeIdsInput){
        nodeIdsInCluster.insert(i);
    }
    nodeIdsInCluster.insert(nodeId);
    initConsistentHashingInfo(true);
    initFreeListOfBucketIds();
    makeStackOfSSDSlotsForBuckets();
    disjointSets.makeInitialSet(bucketIds);
    initAllBuckets();
}

void BucketManager::initFreeListOfBucketIds(){
    uint64_t temp;
    uint64_t bucketIdToEnter;
    uint64_t randSalt;
    for(int i = 0; i< MAX_BUCKETS; i++){
        randSalt = rand();
        temp = tripleHash(randSalt);
        while(getNodeIdOfBucket(temp & bucketIdMaskByParameter, true, true) != nodeId){
            temp = tripleHash(randSalt);
            randSalt = rand();
        }
        bucketIdToEnter = temp & bucketIdMaskByParameter;
        bucketIds[i]= bucketIdToEnter;
    }
}

void BucketManager::initConsistentHashingInfo(bool firstInit){
    std::map<uint64_t, uint64_t> *mapToUpdate = firstInit ?  (&nodesRingLocationMap) : (&newNodesRingLocationMap); // locations to swap YUYU
    std::vector<uint64_t> * vectorToUpdate = firstInit ? (&nodeRingLocationsVector) :(&newNodeRingLocationsVector);
    vector<uint64_t> nodesIdVector(nodeIdsInCluster.begin(), nodeIdsInCluster.end());

    for(uint64_t i = 0; i<nodesIdVector.size(); i++){
        for(uint64_t j = 0; j<CONSISTENT_HASHING_WEIGHT; j++){
            (*mapToUpdate)[tripleHash(nodesIdVector[i] * CONSISTENT_HASHING_WEIGHT + j) ] = nodesIdVector[i];
        }
    }
    for(auto it = mapToUpdate->begin(); it != mapToUpdate->end(); ++it ) {
        vectorToUpdate->push_back(it->first );
    }
    std::sort (vectorToUpdate->begin(), vectorToUpdate->end());
}



void BucketManager::makeStackOfSSDSlotsForBuckets() {
    for(int i = 0; i<BUCKETS_NUM_TO_INIT+1; i++){
        bucketsFreeSSDSlots.push(i*SLOT_SIZE_IN_BYTE);
    }
}

void BucketManager::initAllBuckets(){
    for(int i = 0; i<BUCKETS_NUM_TO_INIT; i++){
        createNewBucket(true,ZERO);
        availableBucketsBitSet.set(i);
    }
}

///////////////////////////////////////////////////
///////////////GENERAL UTILS //////////////////////
///////////////////////////////////////////////////

void BucketManager::updateConsistentHashingData(bool newNodeJoined,uint64_t nodeIdChanged){
    newNodeRingLocationsVector.clear();
    newNodesRingLocationMap.clear();
    if(newNodeJoined){
        nodeIdsInCluster.insert(nodeIdChanged);
    }else{
        nodeIdsInCluster.erase( nodeIdChanged);
    }
    initConsistentHashingInfo(false);
}

std::queue<LocalBucketsMergeJob> BucketManager::getJobsForMergingOwnBuckets() {
    std::queue<LocalBucketsMergeJob> retVal;
    for (auto nodeIdForMergingBuckets : mergableBucketsForEachNode){
        if(nodeIdForMergingBuckets.first == nodeId){
            for(auto bucketsPair : nodeIdForMergingBuckets.second){
                if(bucketsPair.second != BUCKET_ALREADY_MERGED){
                    LocalBucketsMergeJob jobToAdd = LocalBucketsMergeJob(bucketsPair.first,bucketsPair.second);
                    retVal.push(jobToAdd);
                }
            }
            break;
        }
    }
    return retVal;
}

void BucketManager::printNodeData() {
    std::cout << "BucketManager " << nodeId << " has " << bucketsMap.size() << " buckets. " << std::endl;
    std::cout<<endl;
    std::cout << "Printing buckets data: " << std::endl;
    int i = 0;
    int totalNumberOfPages = 0;
    for (auto &itr: bucketsMap) {
        std::cout<<i<<": ";
        i++;
        totalNumberOfPages += itr.second.getPagesNumInBucket();
        itr.second.printBucketData();
    }
    std::cout << "total number of pages in node " <<  nodeId << ": " << totalNumberOfPages << std::endl;
}


inline uint64_t BucketManager::FasterHash(uint64_t input) {
    uint64_t local_rand = input;
    uint64_t local_rand_hash = 8;
    local_rand_hash = 40343 * local_rand_hash + ((local_rand) & 0xFFFF);
    local_rand_hash = 40343 * local_rand_hash + ((local_rand >> 16) & 0xFFFF);
    local_rand_hash = 40343 * local_rand_hash + ((local_rand >> 32) & 0xFFFF);
    local_rand_hash = 40343 * local_rand_hash + (local_rand >> 48);
    local_rand_hash = 40343 * local_rand_hash;
    return local_rand_hash; // if 64 do not rotate
    // else:
    // return Rotr64(local_rand_hash, 56);
}

uint64_t BucketManager::tripleHash(uint64_t input){
    return FasterHash(FasterHash(FasterHash(input)));
}

// this is just for testing - this wil NOT be called
uint64_t BucketManager::getNumberOfPagesInNode(){
    int retVal = 0;
    for (auto &itr: bucketsMap) {
        retVal += itr.second.getPagesNumInBucket();
    }
    return retVal;
}



void BucketManager::putBucketIdToUnionFind(uint64_t newBucketId){
    disjointSets.addToUnionFind(newBucketId);
}

void BucketManager::putNewUnionFindPairData(uint64_t child, uint64_t root){
    newDisjointSets.addPairToUnionFind(child,root);
}

void BucketManager::duplicateDisjointSetsAndMergeNew() {
    auto existingMap = disjointSets.getMap();
    for ( const auto &unionFindPair :   disjointSets.getMap() ) {
        newDisjointSets.addPairToUnionFind(unionFindPair.first,unionFindPair.second);
    }
    for (auto & [key, value] : mergableBucketsForEachNode) {
        for(auto unionFindPair : value){
            newDisjointSets.addPairToUnionFind(unionFindPair.first,unionFindPair.second);
        }
    }
}

bool BucketManager::updateRequestedBucketNumAndIsMergeNeeded(uint64_t newBucketsAmount){
    bool retVal = false;
    aggregatedBucketNum += newBucketsAmount;

    if((aggregatedBucketNum - bucketsLeavingNum) > MAX_BUCKETS ){
       retVal = false;
    }
    return retVal;
}

void BucketManager::deleteBucket(uint64_t bucketId){
    bucketMapMtx.lock();
    auto bucketToRemove = bucketsMap.find(bucketId);
    uint64_t ssdSlotFreed = bucketToRemove->second.SSDSlotStart;
    bucketsMap.erase(bucketToRemove);
    bucketsNum--;
    bucketsFreeSSDSlots.push(ssdSlotFreed);
    bucketMapMtx.unlock();

}


///////////////shuffling functions //////////////////////

//pair is <bucket id , node>
// this function will be able to change according to parameters.
// for example - random send, send to each node all his buckets, big buckets first, etc.
vector<pair<uint64_t,uint64_t>> BucketManager::getBucketsShufflePrioritiesAndNodes(){

    vector<pair<uint64_t,uint64_t>> retVal;
    for(const auto& pairOfNodeIdToSet: mergableBucketsForEachNode){
        if(pairOfNodeIdToSet.first != nodeId){
            for(auto pairOfMergableBuckets : pairOfNodeIdToSet.second){
                retVal.emplace_back(pairOfMergableBuckets.first,pairOfNodeIdToSet.first);
            }
        }
    }

    return retVal;
}
void BucketManager::atomicallyMoveToNormal(){

    //
}

void BucketManager::initBucketSizeDataByParameter(int bucketIdByteSize){
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
PageIdJobFromBucket BucketManager::getPageFromBucketToShuffle(uint64_t bucketId){
    PageIdJobFromBucket retVal;
    bucketMapMtx.lock();
    auto bucketToGetPageFrom = bucketsMap.find(bucketId);
    retVal = bucketToGetPageFrom->second.getAnyPageOfBucketToShuffle();
    bucketMapMtx.unlock();
    return retVal;
}

void BucketManager::lockBucketBeforeShuffle(uint64_t bucketIdToLock){
    bucketMapMtx.lock();
    bucketsMap.find(bucketIdToLock)->second.lockBucketBeforeShuffle();
    bucketMapMtx.unlock();
}

void BucketManager::enterNewShuffledPidToBucketManager(uint64_t shuffledPid){
    uint64_t bucketId = bucketIdMaskByParameter & shuffledPid;
    uint64_t realBucketId = disjointSets.find(bucketId);
    bucketsMap.find(realBucketId)->second.addNewPageWithPageId(shuffledPid);
}



BucketsDisjointSets& BucketManager::getDisjointSets(){
    return disjointSets;
}
