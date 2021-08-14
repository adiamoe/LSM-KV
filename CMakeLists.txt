cmake_minimum_required(VERSION 3.19)
project(LSM-KV)

set(CMAKE_CXX_STANDARD 17)

add_executable(LSM-KV correctness.cc kvstore.cc kvstore.h kvstore_api.h MurmurHash3.h SkipList.cpp SkipList.h
        TableCache.cpp TableCache.h utils.h test.h threadPool.h)