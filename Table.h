//
// Created by ZJW on 2021/4/19.
//
#include "SkipList.h"
#include "MurmurHash3.h"
#include<assert.h>
#include <map>
#ifndef LSM_KV_TABLE_H
#define LSM_KV_TABLE_H


class Table {
private:
    string sstable;                     //文件路径及文件名
    uint64_t metadata[4];               //时间戳，键值对数量，最小键和最大键
    bitset<81920> BloomFilter;          //过滤器
    map<int64_t, uint32_t> offset;      //储存对应的偏移量
    ifstream *file;
public:
    Table(string &fileName);

    uint64_t getTimestamp() {return metadata[0];}
    uint64_t getNumPair() {return metadata[1];}
    uint64_t getMaxKey() {return metadata[2];}
    uint64_t getminKey() {return metadata[3];}

    string getValue(uint64_t key) const;
    void open();
    void traverse(map<int64_t, string> &pair);
    void reset();
};


#endif //LSM_KV_TABLE_H
