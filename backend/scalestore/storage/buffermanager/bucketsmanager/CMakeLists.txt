cmake_minimum_required(VERSION 3.24)
project(localThesis)

set(CMAKE_CXX_STANDARD 17 -pthread)

add_executable(localThesis main.cpp Bucket.h BucketManager.h GenericDisjointSets.h Tests.h testUtils.h  MessagesEnum.h BucketManager.cpp Message.h BucketManagerMessageHandler.h BucketManagerMessageHandler.cpp RemoteBucketShuffleJob.h LocalBucketsMergeJob.h BucketManagerDefs.h)
