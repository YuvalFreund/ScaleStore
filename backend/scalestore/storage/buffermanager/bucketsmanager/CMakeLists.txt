cmake_minimum_required(VERSION 3.24)
project(localThesis)

set(CMAKE_CXX_STANDARD 17 -pthread)

add_executable(localThesis BucketsDisjointSets.h Tests.h testUtils.h  MessagesEnum.h RemoteBucketShuffleJob.h LocalBucketsMergeJob.h)
