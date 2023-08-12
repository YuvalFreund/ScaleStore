//
// Created by YuvalFreund on 31.03.23.
//
#include "BucketManager.h"
#include "testUtils.h"
#ifndef RANGESATTEMPT_TESTS_H
#define RANGESATTEMPT_TESTS_H

void sanityTest (){

    std::cout<<endl;
    std::cout<<"//////////////////////////"<<std::endl;
    std::cout<<"/// simple sanity test ///"<<std::endl;
    std::cout<<"////////// START /////////"<<std::endl;
    std::cout<<"//////////////////////////"<<std::endl;
    std::cout<<endl;

    vector<uint64_t> nodesInCluster = {1,2};
    int numPages = 100;
    BucketManager node1 = BucketManager(1, nodesInCluster);
    BucketManager node2 = BucketManager(2, nodesInCluster);
    for(int i = 0; i<numPages; i++){
        node1.addNewPage();
        node2.addNewPage();
    }
    for(int i = 0; i<numPages / 2; i++){
        node1.removePage(node1.bucketsMap.begin()->second.getBucketId()+i);
    }
    node1.printNodeData();
    node2.printNodeData();

    std::cout<<endl;
    std::cout<<"//////////////////////////"<<std::endl;
    std::cout<<"/// simple sanity test ///"<<std::endl;
    std::cout<<"////////// FINISH ////////"<<std::endl;
    std::cout<<"//////////////////////////"<<std::endl<<std::endl;
    std::cout<<endl;
}
void oneNode2BucketsTest(){
    std::cout<<std::endl;
    std::cout<<"///////////////////////////////"<<std::endl;
    std::cout<<"/// one BucketManager 2 Buckets Test ///"<<std::endl;
    std::cout<<"///////////// START ///////////"<<std::endl;
    std::cout<<"///////////////////////////////"<<std::endl<<std::endl;
    std::cout<<endl;
    vector<uint64_t> nodesInCluster = {1};

    int numPages = MAX_PAGES + 100;
    BucketManager node1 = BucketManager(1, nodesInCluster);
    for(int i = 0; i<numPages; i++){
        node1.addNewPage();
    }
    node1.printNodeData();

    std::cout<<endl;
    std::cout<<"///////////////////////////////"<<std::endl;
    std::cout<<"/// one BucketManager 2 Buckets Test ///"<<std::endl;
    std::cout<<"///////////// FINISH //////////"<<std::endl;
    std::cout<<"///////////////////////////////"<<std::endl<<std::endl;
    std::cout<<endl;

}
void simpleMergeBucketsTest(){
    std::cout<<std::endl;
    std::cout<<"/////////////////////////////////"<<std::endl;
    std::cout<<"/// simple Merge Buckets Test ///"<<std::endl;
    std::cout<<"///////////// START /////////////"<<std::endl;
    std::cout<<"/////////////////////////////////"<<std::endl<<std::endl;
    int numPages = MAX_PAGES + 100;
    vector<uint64_t> nodesInCluster = {1};

    BucketManager node1 = BucketManager(1, nodesInCluster);
    for(int i = 0; i<numPages; i++){
        node1.addNewPage();
    }
    node1.printNodeData();

    std::cout<<"/// removing 300 pages ///"<<std::endl;

    for(long i = 0; i<300; i++){
        node1.removePage(598429493952839680+i);
    }

    node1.printNodeData();

    std::cout<<"/// merging buckets ///"<<std::endl;

    node1.mergeSmallBucketIntoBigBucket(598429493952839680, 11194205097226469376);
    node1.printNodeData();

    std::cout<<"/////////////////////////////////"<<std::endl;
    std::cout<<"/// simple Merge Buckets Test ///"<<std::endl;
    std::cout<<"//////////// FINISH /////////////"<<std::endl;
    std::cout<<"/////////////////////////////////"<<std::endl<<std::endl;
}
void consistentHashingSearchTest(){
    std::cout<<std::endl;
    std::cout<<"//////////////////////////////////////"<<std::endl;
    std::cout<<"/// consistent Hashing Search Test ///"<<std::endl;
    std::cout<<"/////////////// START ////////////////"<<std::endl;
    std::cout<<"//////////////////////////////////////"<<std::endl<<std::endl;

    std::vector<uint64_t> nodeLocations;
    for(uint64_t i = 0; i<100000; i++){
        nodeLocations.emplace_back(i * 10 + 5);
    }
    for(uint64_t i = 0; i<1000000; i++){
        std::cout<<"i: " << i << " index: "<<getIndexOfRingPosition(i,nodeLocations)<<std::endl;
    }


    std::cout<<"//////////////////////////////////////"<<std::endl;
    std::cout<<"/// consistent Hashing Search Test ///"<<std::endl;
    std::cout<<"/////////////// FINISH ///////////////"<<std::endl;
    std::cout<<"//////////////////////////////////////"<<std::endl<<std::endl;
};
void multipleThreadsSimpleTest(){
    std::cout<<std::endl;
    std::cout<<"////////////////////////////////////"<<std::endl;
    std::cout<<"/// multiple Threads Simple Test ///"<<std::endl;
    std::cout<<"/////////////// START //////////////"<<std::endl;
    std::cout<<"////////////////////////////////////"<<std::endl<<std::endl;
    int numThreads = 100;
    int numPages = 100000;
    vector<uint64_t> nodesInCluster = {1};

    BucketManager node(1, nodesInCluster);
    std::vector<std::thread> threads;
    for(int i=0; i<numThreads; i++){
        threads.emplace_back(insertPagesToNode,&node,&numPages);
    }
    for(auto& thread : threads){
        thread.join();
    }
    node.printNodeData();
    std::cout<<std::endl;
    std::cout<<"/////////////////////////////////////"<<std::endl;
    std::cout<<"/// multiple Threads Simple Test ////"<<std::endl;
    std::cout<<"/////////////// FINISH //////////////"<<std::endl;
    std::cout<<"/////////////////////////////////////"<<std::endl<<std::endl;
}

void singleNodeTest (){

    std::cout<<endl;
    std::cout<<"//////////////////////////"<<std::endl;
    std::cout<<"///  Single node test  ///"<<std::endl;
    std::cout<<"////////// START /////////"<<std::endl;
    std::cout<<"//////////////////////////"<<std::endl;
    std::cout<<endl;

    vector<uint64_t> nodesInCluster = {1};
    vector<uint64_t> addedPagesIds;
    int numPages = 100;
    BucketManager node1 = BucketManager(1, nodesInCluster);
    for(int i = 0; i<numPages; i++){
        addedPagesIds.emplace_back(node1.addNewPage());
    }
    for(int i = 0; i<numPages / 2; i++){
        node1.removePage(addedPagesIds[i]);
    }
    node1.printNodeData();

    std::cout<<endl;
    std::cout<<"//////////////////////////"<<std::endl;
    std::cout<<"/// simple sanity test ///"<<std::endl;
    std::cout<<"////////// FINISH ////////"<<std::endl;
    std::cout<<"//////////////////////////"<<std::endl<<std::endl;
    std::cout<<endl;
}



#endif //RANGESATTEMPT_TESTS_H
