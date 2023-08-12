cmake_minimum_required(VERSION 3.24)
project(buckets)

set(CMAKE_CXX_STANDARD 17)

add_executable(buckets Bucket.h Page.h BucketManager.h tools.h cluster.h GenericDisjointSets.cpp GenericDisjointSets.h tests.h testUtils.h)
