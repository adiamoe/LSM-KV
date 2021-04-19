//
// Created by ZJW on 2021/4/19.
//

#include "Table.h"

Table::Table(string &fileName, int lev) {
    level = lev;
    sstable = fileName;
    file = nullptr;
}

string Table::getValue(uint64_t key) {
    assert(valid());
    //通过布隆过滤器判断key是否存在，如果有其中一个bit为0，则证明不存在
    unsigned int hash[4] = {0};
    MurmurHash3_x64_128(&key, sizeof(key), 1, hash);
    for(int i=0; i<4; ++i)
        if(!BloomFilter[hash[i]%81920])
            return "";

    //再在键值对中进行查找
    if(offset.count(key)==0)
        return "";

    uint32_t pos = offset[key];
    uint64_t len = length[key];
    file->seekg(pos);
    string ans;
    file->read((char *)(&ans), len);
    return ans;
}

void Table::traverse(map<int64_t, string> &pair) {
    assert(valid());
    auto iter = offset.begin();
    string ans;
    while(iter!=offset.end())
    {
        uint64_t len = length[iter->first];
        file->seekg(iter->second);
        file->read((char *)(&ans), len);
        pair[iter->first] = ans;
        iter++;
    }
}

void Table::reset() {
    file->open(sstable, ios::in|ios::binary);
}