//
// Created by ZJW on 2021/4/16.
//
#ifndef LSM_KV_SKIPLIST_H
#define LSM_KV_SKIPLIST_H

#include "kvstore_api.h"
#include <vector>
using namespace std;

struct Node{
    Node *right, *down;
    int64_t key;
    string val;
    Node(Node *right, Node *down, int64_t key, const string &s): right(right), down(down), key(key), val(s){}
    Node(): right(nullptr), down(nullptr) ,key(0), val(" "){}
};

class SkipList {
private:
    //储存的键值对个数
    int Size;
    //转换成SSTable占用的空间大小
    size_t memory;
    //头结点
    Node *head;
public:
    //todo：如何计算转换后SSTable占用的空间大小
    SkipList():Size(0), head(), memory(0){}

    //get the number of KV
    int getPairNum() {return Size;}

    size_t getSize() {return memory;}

    //get the value of key
    string get(int64_t key);

    //insert the KV into Skiplist
    void put(int64_t key, const string &value);

    //remove the KV pair
    bool remove(int64_t key);

};


#endif //LSM_KV_SKIPLIST_H
