//
// Created by ZJW on 2021/4/16.
//

#include "SkipList.h"
#include "MurmurHash3.h"

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
    //更新最大最小值键
    if(key<minKey)
        minKey = key;
    if(key>maxKey)
        maxKey = key;

    vector<Node*> pathList;    //从上至下记录搜索路径
    Node *p = head;
    while(p){
        while(p->right && p->right->key <= key){
            p = p->right;
        }
        pathList.push_back(p);
        p = p->down;
    }

    //对于相同的key，MemTable中进行替换
    if(!pathList.empty() && pathList.back()->key == key)
    {
        while(!pathList.empty() && pathList.back()->key == key)
        {
            Node *node = pathList.back();
            pathList.pop_back();
            node->val = value;
        }
        return;
    }

    //如果不存在相同的key，则进行插入
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

/*
 * 删除时无论有没有这个元素，都需要插入“~DELETE~”，因此put可以取代删除的功能
 */
/*
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
}*/

Node* SkipList::GetFirstNode()
{
    Node *p = head, *q = nullptr;
    while(p){
        q = p;
        p = p->down;
    }
    return q;
}

void SkipList::clear()
{
    if(!head)
        return;
    //释放掉所有数据，防止内存泄漏
    Node *p = head, *q = nullptr, *down = nullptr;
    do
    {
        down = p->down;
        do{
            q = p->right;
            delete p;
            p = q;
        }while(p);
        p = down;
    }while(p);

    //head指针置空，重置除时间戳外的其它数据项
    head = nullptr;
    Size = 0;
    memory = 10272;
    minKey = INT64_MAX;
    maxKey = INT64_MIN;
}

void SkipList::store(int num, const std::string &dir)
{
    string FileName = dir + "/SSTable" + to_string(num) + ".sst";
    ofstream outFile(FileName, std::ios::app | std::ios::binary);
    Node *node = GetFirstNode();
    timeStamp++;

    //写入时间戳、键值对个数和最小最大键
    outFile.write((char *)(&timeStamp), sizeof(uint64_t));
    outFile.write((char *)(&Size), sizeof(uint64_t));
    outFile.write((char *)(&minKey), sizeof(uint64_t));
    outFile.write((char *)(&maxKey), sizeof(uint64_t));

    //写入生成对应的布隆过滤器
    bitset<81920> filter;
    uint64_t tempKey;
    string tempValue;
    unsigned int hash[4] = {0};
    while(node)
    {
        tempKey = node->key;
        MurmurHash3_x64_128(&tempKey, sizeof(tempKey), 1, hash);
        for(int i=0; i<4; ++i)
            filter.set(hash[i]%81920);
        node = node->right;
    }
    outFile.write((char *)(&filter), sizeof(filter));

    //索引区，计算key对应的索引值
    const int dataArea = 10272 + Size * 12;   //数据区开始的位置
    uint32_t index = 0;
    node = GetFirstNode();
    int length = 0;
    while(node)
    {
        tempKey = node->key;
        index = dataArea + length;
        outFile.write((char *)(&tempKey), sizeof(uint64_t));
        outFile.write((char *)(&index), sizeof(uint32_t));
        length += node->val.size();
        node = node->right;
    }

    //数据区，存放value
    node = GetFirstNode();
    while(node)
    {
        tempValue = node->val;
        outFile.write((char *)(&tempValue), tempValue.size());
        node = node->right;
    }
    outFile.close();
    clear();
}