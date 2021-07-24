//
// Created by ZJW on 2021/4/19.
//

#include "TableCache.h"

TableCache::TableCache(string &fileName) {
    file = new ifstream;
    sstable = fileName;
    open();
}

/**
 * 根据输入的key，寻找文件中有无对应的value
 */
string TableCache::getValue(const uint64_t key) const{

    if(key<MinMaxKey[0] || key>MinMaxKey[1])
        return "";
    //通过布隆过滤器判断key是否存在，如果有其中一个bit为0，则证明不存在
    unsigned int hash[4] = {0};
    MurmurHash3_x64_128(&key, sizeof(key), 1, hash);
    for(unsigned int i : hash)
        if(!BloomFilter[i%81920])
            return "";

    //再在键值对中进行查找
    if(offset.count(key)==0)
        return "";
    auto iter1 = offset.find(key);
    auto iter2 = offset.find(key);
    iter2++;

    file->open(sstable, ios::in|ios::binary);
    file->seekg(0, ios::end);
    uint64_t len = iter2!=offset.end()? iter2->second-iter1->second:(int)file->tellg() - iter1->second;

    char* ans = new char[len];


    reset();                                        //遇到了部分数据有概率读不到的情况，关闭文件再打开可以规避这个问题
    file->seekg(iter1->second);                    //但是原因实在百思不得其解
    file->read(ans, sizeof(char) * len);

    file->close();                  //一定要关闭文件，没有关闭文件会读出乱码，血泪教训
    return ans;
}

//遍历文件，将键值对全部读进内存
void TableCache::traverse(map<int64_t, string> &pair) const{
    file->open(sstable, ios::in|ios::binary);
    reset();
    auto iter1 = offset.begin();
    auto iter2 = offset.begin();
    iter2++;

    char *ans;
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
            len = (int)file->tellg() - iter1->second;
        }
        file->seekg(iter1->second);
        ans = new char[len];
        file->read(ans, sizeof(char) * len);
        pair[iter1->first] = ans;
        iter1++;
    }
    file->close();
}

//打开文件，将缓存在内存中的各项数据更新
void TableCache::open()
{
    file->open(sstable, ios::in|ios::binary);
    reset();
    file->read((char *)(&TimeAndNum), 2* sizeof(uint64_t));
    file->read((char*)(&MinMaxKey), 2* sizeof(int64_t));
    file->read((char *)(&BloomFilter), sizeof(BloomFilter));

    int num = TimeAndNum[1];
    int64_t tempKey;
    uint32_t  tempOffset;
    while(num--)
    {
        file->read((char*)(&tempKey), sizeof(int64_t));
        file->read((char*)(&tempOffset), sizeof(uint32_t));
        offset[tempKey] = tempOffset;
    }

    file->close();
}

void TableCache::reset() const{
    file->close();
    file->open(sstable, ios::in|ios::binary);
}