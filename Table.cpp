//
// Created by ZJW on 2021/4/19.
//

#include "Table.h"

Table::Table(string &fileName) {
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

    auto iter = offset.find(key);
    uint32_t pos1 = iter->second;
    uint32_t pos2 = (++iter)->second;
    uint64_t len = pos2 - pos1;
    file->seekg(pos1);
    string ans;
    file->read((char *)(&ans), len);
    return ans;
}

void Table::traverse(map<int64_t, string> &pair) {
    assert(valid());
    auto iter1 = offset.begin();
    auto iter2 = offset.begin();
    iter2++;

    string ans;
    uint64_t len;
    while(iter1!=offset.end())
    {
        if(iter2!=offset.end())
        {
            len = iter2->second - iter1->second;
            iter2++;
        }
        else
        {
            file->seekg(0, ios::end);
            len = file->tellg();
        }
        file->seekg(iter1->second);
        file->read((char *)(&ans), len);
        pair[iter1->first] = ans;
        iter1++;
    }
}

void Table::open()
{
    file->open(sstable, ios::in|ios::binary);
    file->read((char *)(&metadata), 4* sizeof(uint64_t));
    file->read((char *)(&BloomFilter), BloomFilter.size());
    int num = metadata[2];
    uint64_t tempKey;
    int32_t  tempOffset;
    while(num--)
    {
        file->read((char*)(&tempKey), sizeof(uint64_t));
        file->read((char*)(&tempOffset), sizeof(int32_t));
        offset[tempKey] = tempOffset;
    }
}

void Table::reset() {
    file->open(sstable, ios::in|ios::binary);
}