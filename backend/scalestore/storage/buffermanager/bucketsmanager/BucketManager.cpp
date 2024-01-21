//
// Created by YuvalFreund on 22.11.23.
//
#include "BucketManager.h"

uint64_t BucketManager::addNewPage(){
    bool availableBucketFound = false;
    uint64_t retVal;
    int tryNum=1;
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
            // todo - how to lock here the whole system when adding a bucket
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
    // TODO YUVAL - THIS CAN BE REPLACED WITH SOME OPTIMISTIC WAY SKIPPING LOOK UP
    uint64_t realBucketId = disjointSets.find(bucketId);
    retVal = bucketsMap.find(realBucketId)->second.getPageSSDSlotByPageId(pageId); // this is the actual mapping

    return retVal;
}


uint64_t BucketManager::getNodeIdOfPage(uint64_t pageId){

    uint64_t bucketId = pageId & bucketIdMaskByParameter;
    uint64_t foundNodeId = getNodeIdOfBucket(bucketId, false, false);

    return foundNodeId;
}


uint64_t BucketManager::getNodeIdOfBucket(uint64_t bucketId,bool fromInitStage, bool forceNewState){
    uint64_t res = INVALID_NODE_ID;
    bool searchOldOrNewRing = ((managerState.load() == ManagerState::normal) || (managerState.load() == ManagerState::synchronizing));
    if(forceNewState ) searchOldOrNewRing = false;
    if(searchOldOrNewRing){
        auto pageInCacheIter = bucketIdToNodeCache.find(bucketId);
        if(pageInCacheIter != bucketIdToNodeCache.end()){
            return pageInCacheIter->second;
        }
    }
    std::map<uint64_t, uint64_t> *mapToSearch = searchOldOrNewRing ? (&nodesRingLocationMap ) : (&newNodesRingLocationMap) ; // locations to swap YUYU
    std::vector<uint64_t> * vectorToSearch = searchOldOrNewRing ? (&nodeRingLocationsVector) : (&newNodeRingLocationsVector);

    uint64_t l = 0;
    uint64_t r = vectorToSearch->size() - 1;
    // edge case for cyclic operation
    if(bucketId < vectorToSearch->at(l) || bucketId > vectorToSearch->at(r)) return mapToSearch->at(vectorToSearch->at(r));
    // binary search
    while (l <= r) {
        uint64_t m = l + (r - l) / 2;
        if (vectorToSearch->at(m) <= bucketId && vectorToSearch->at(m + 1) > bucketId) {
            res = mapToSearch->at(vectorToSearch->at(r));
            break;
        }
        if (vectorToSearch->at(m) < bucketId) {
            l = m + 1;
        } else{
            r = m - 1;
        }
    }

    // from init stage checks that we don't just add buckets that are not actually chosen in the end
    if(searchOldOrNewRing && fromInitStage == false){
        bucketIdToNodeCache[bucketId] = res;
    }
    return res;
}

//////////////BUCKETS MANAGEMENT///////////////////

// we merge small bucket into the big bucket
void BucketManager::mergeSmallBucketIntoBigBucket(LocalBucketsMergeJob localBucketMergeJob){
    bucketManagerMtx.lock();
    auto bigBucket = bucketsMap.find(localBucketMergeJob.bigBucket);
    auto smallBucket = bucketsMap.find(localBucketMergeJob.smallBucket);
    //std::cout << "Merging buckets! Big bucket:  " << bigBucketId << " Small bucket: " << smallBucketId << std::endl;

    bigBucket->second.mergeBucketIn(&smallBucket->second);
    disjointSets.Union(localBucketMergeJob.bigBucket,localBucketMergeJob.smallBucket);
    deleteBucket(localBucketMergeJob.smallBucket);
    bucketManagerMtx.unlock();
}

uint64_t BucketManager::createNewBucket(bool isNewBucketIdNeeded, uint64_t givenBucketId){
    uint64_t retVal;
    bucketManagerMtx.lock(); //todo yuval - how to get this lock without causing problem
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
    //std::cout<<"bucket id: " <<newBucketId << "ssd slot start: " << SSDSlotStart<<std::endl;
    // create new bucket
    bucketsMap.try_emplace(newBucketId, newBucketId, retVal);
    bucketsNum++;
    bucketManagerMtx.unlock(); //todo yuval - how to get this lock without causing problem
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
    managerState.store(synchronizing);

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
        while(getNodeIdOfBucket(temp & bucketIdMaskByParameter, true, false) != nodeId){
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
void BucketManager::getOldConsistentHashingInfoForNewNode(){
    nodeIdsInCluster.erase(nodeId);
    nodeRingLocationsVector.clear();
    nodesRingLocationMap.clear();
    initConsistentHashingInfo(true);
    nodeIdsInCluster.insert(nodeId);
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
    newNodeRingLocationsVector.clear();// yuyu
    newNodesRingLocationMap.clear();// yuyu
    if(newNodeJoined){
        nodeIdsInCluster.insert(nodeIdChanged);
    }else{
        nodeIdsInCluster.erase( nodeIdChanged);
    }
    initConsistentHashingInfo(false);
}

void BucketManager::mergeOwnBuckets() {
    for (auto nodeIdForMergingBuckets : mergableBucketsForEachNode){
        if(nodeIdForMergingBuckets.first == nodeId){
            for(auto bucketsPair : nodeIdForMergingBuckets.second){
                if(bucketsPair.second != BUCKET_ALREADY_MERGED){

                    //mergeSmallBucketIntoBigBucket(bucketsPair.first,bucketsPair.second);
                }
            }
            break;
        }
    }
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

BucketsDisjointSets BucketManager::getDisjointSets(){
    return disjointSets;
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
    auto bucketToRemove = bucketsMap.find(bucketId);
    uint64_t ssdSlotFreed = bucketToRemove->second.SSDSlotStart;
    bucketsMap.erase(bucketToRemove);
    bucketsNum--;
    bucketsFreeSSDSlots.push(ssdSlotFreed);
}


///////////////shuffling functions //////////////////////

//pair is <bucket id , node>
// this function will be able to change according to parameters.
// for example - random send, send to each node all his buckets, big buckets first, etc.
vector<pair<uint64_t,uint64_t>> BucketManager::getBucketsShufflePrioritiesAndNodes(){

    managerState.store(shuffling);
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
    managerState.store(finished); // TODO DFD CHANGE LATER TO NORMAL
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

