//
// Created by ZJW on 2021/4/19.
//
#include "SkipList.h"
#include"MurmurHash3.h"
#include<assert.h>
#include <map>
#ifndef LSM_KV_TABLE_H
#define LSM_KV_TABLE_H


class Table {
private:
    string sstable;
    uint64_t metadata[4];
    bitset<81920> BloomFilter;
    map<int64_t, uint32_t> offset;
    fstream *file;
public:
    Table(string &fileName);
    bool valid() {return file != nullptr;}

    uint64_t getTimestamp() {return metadata[0];}
    uint64_t getNumPair() {return metadata[1];}
    uint64_t getMaxKey() {return metadata[2];}
    uint64_t getminKey() {return metadata[3];}

    string getValue(uint64_t key);
    void open();
    void traverse(map<int64_t, string> &pair);
    void reset();
};


#endif //LSM_KV_TABLE_H
