//
// Created by ZJW on 2021/4/16.
//

#include "SkipList.h"

string SkipList::get(int64_t key)
{
    Node *p = head;
    while(p)
    {
        while(p->right && p->right->key<key)
        {
            p = p->right;
        }
        if(p->right && p->right->key == key)
            return (p->right->val);
        p = p->down;
    }
    return "";
}

void SkipList::put(int64_t key, const string &value)
{
    vector<Node*> pathList;    //从上至下记录搜索路径
    Node *p = head;
    while(p){
        while(p->right && p->right->key < key){
            p = p->right;
        }
        pathList.push_back(p);
        p = p->down;
    }

    bool insertUp = true;
    Node* downNode= nullptr;
    Size++;
    while(insertUp && !pathList.empty()){   //从下至上搜索路径回溯，50%概率
        Node *insert = pathList.back();
        pathList.pop_back();
        insert->right = new Node(insert->right, downNode, key, value); //add新结点
        downNode = insert->right;    //把新结点赋值为downNode
        insertUp = (rand()&1);   //50%概率
    }
    if(insertUp){  //插入新的头结点，加层
        Node* oldHead = head;
        head = new Node();
        head->right = new Node(nullptr, downNode, key, value);
        head->down = oldHead;
    }
}

bool SkipList::remove(int64_t key)
{
    Node *p = head, *q, *pos;
    while(p)
    {
        while(p->right && p->right->key<key)
        {
            p = p->right;
        }
        if(p->right && p->right->key == key)
        {
            while(p)
            {
                q = p->right;
                p->right = q->right;
                p = p->down;
                pos = q->down;
                delete q;
                while(p && p->right && p->right != pos)
                    p = p->right;
            }
            Size--;
            return true;
        }
        p = p->down;
    }
    return false;
}