#include "kvstore.h"
#include "utils.h"

//todo:注意reset()
KVStore::KVStore(const std::string &dir): KVStoreAPI(dir)
{
    memTable = new SkipList;
    this->dir = dir;
    Level.push_back(0);

    reset();

    vector<string> ret;
    int num = utils::scanDir(dir, ret);
    for(int i=0; i<num; ++i)
    {
        SSTable.emplace_back();
        string path = dir + "/" + ret[i];
        vector<string> file;
        int numSST = utils::scanDir(path, file);
        Level.push_back(numSST);
        for(int j=0; j<numSST; ++j)
        {
            string FileName = path + "/" + file[j];
            Table sstable(FileName);
            SSTable[i][FileName] = sstable;
        }
    }
    if(SSTable.empty())
        SSTable.emplace_back();
}

//将 MemTable 中的所有数据以 SSTable 形式写进磁盘
KVStore::~KVStore()
{
    string path = dir + "/level0";
    memTable->store(++Level[0], path);
    if(Level[0]==3)
        compactionForLevel(1);
    memTable->clear();
    delete memTable;
}

/**
 * Insert/Update the key-value pair.
 * No return values for simplicity.
 */
void KVStore::put(uint64_t key, const string &s)
{
    string val = memTable->get(key);
    if(!val.empty())
        memTable->memory += s.size() - val.size();
    else
        memTable->memory += 4 + 8 + s.size() + 1;  //索引值 + key + value所占的内存大小 + "\0"

    if(memTable->memory > MEMTABLE)
    {
        string path = dir + "/level0";
        if(!utils::dirExists(path))
            utils::mkdir(path.c_str());

        Level[0]++;
        memTable->store(Level[0], path);
        string newFile = path + "/SSTable" + to_string(Level[0]) + ".sst";
        Table newTable(newFile);
        SSTable[0][newFile] = newTable;
        if(Level[0]>UpperNum(0))
        {
            compactionForLevel(1);
            Level[0] = 0;
        }
    }
    memTable->put(key, s);
}
/**
 * Returns the (string) value of the given key.
 * An empty string indicates not found.
 */

std::string KVStore::get(uint64_t key)
{
	string ans = memTable->get(key);

	if(!ans.empty())
    {
	    if(ans=="~DELETED~")
	        return "";
	    else
            return ans;
    }

    string out;
    for(const auto &tableList:SSTable)
    {
	    for(auto table = tableList.rbegin(); table!=tableList.rend(); ++table)
        {
	        out = table->second.getValue(key);
	        if(!out.empty())
            {
	            if(out=="~DELETED~")
	                return "";
	            else
                    return out;
            }
        }
    }
	//cout<<key<<endl;
	return "";
}
/**
 * Delete the given key-value pair if it exists.
 * Returns false iff the key is not found.
 */
bool KVStore::del(uint64_t key)
{
    bool flag = !get(key).empty();
    if(flag)
        put(key, "~DELETED~");
    return flag;
}

/**
 * This resets the kvstore. All key-value pairs should be removed,
 * including memtable and all sstables files.
 */
void KVStore::reset()
{
    memTable->clear();
    vector<string> file;
    int numDir = utils::scanDir(dir, file);
    for(int i=0; i<numDir; ++i)
    {
        string path = dir + "/" + file[i];
        vector<string> ret;
        int numSST = utils::scanDir(path, ret);
        for(int j=0; j<numSST; ++j)
            utils::rmfile((path + "/" +ret[j]).c_str());
        utils::rmdir(path.c_str());
    }
}

/**
 * 递归合并各层SSTABLE，对0层将所有文件向下合并
 * 其它层则只将超出最大个数的文件向下合并
 * @param level 被合并的层
 */

void KVStore::compactionForLevel(int level)
{
    //递归中止条件，如果上层的文件数小于最大上限，则不再合并
    int SSTnum = SSTable[level-1].size();
    if(SSTnum<=UpperNum(level-1))
        return;

    string path = dir + "/level" + to_string(level);
    //若没有Level文件夹，先创建
    if(!utils::dirExists(path))
    {
        utils::mkdir(path.c_str());
        Level.push_back(0);
        SSTable.emplace_back();
    }

    vector<string> FileToRemoveLevelminus1;                    //记录需要被删除的文件
    vector<string> FileToRemoveLevel;

    //Level层，将SSTable按时间戳排序
    priority_queue<Table> sortTableLastLevel;
    //需要被合并的SSTable
    priority_queue<Table> sortTable;

    for(auto &table:SSTable[level-1])
    {
        sortTableLastLevel.push(table.second);
    }

    uint64_t timestamp = 0;
    int64_t tempMin = INT64_MAX, tempMax = INT64_MIN;

    //需要合并的文件数
    int compactNum = (level-1==0)?UpperNum(0)+1:SSTnum - UpperNum(level-1);

    //遍历Level-1中被合并的SSTable，获得时间戳和最大最小键
    for(int i=0; i<compactNum; ++i)
    {
        Table table = sortTableLastLevel.top();
        sortTable.push(table);
        FileToRemoveLevelminus1.push_back(table.getFileName());
        sortTableLastLevel.pop();

        timestamp = table.getTimestamp();
        if(table.getMaxKey()>tempMax)
            tempMax = table.getMaxKey();
        if(table.getMinKey()<tempMin)
            tempMin = table.getMinKey();
    }

    //找到Level中和Level-1中键有交集的文件
    for(auto &iter:SSTable[level])
    {
        if(iter.second.getMaxKey()>=tempMin && iter.second.getMinKey()<=tempMax)
        {
            sortTable.push(iter.second);
            FileToRemoveLevel.push_back(iter.second.getFileName());
        }
    }

    vector<map<int64_t, string>> KVToCompact;                   //被合并的键值对，下标越大时间戳越大
    vector<map<int64_t, string>::iterator> KVToCompactIter;     //键值对迭代器
    int size = InitialSize;
    map<int64_t, int> minKey;                      //minKey中只存放num个数据，分别为各个SSTable中最小键和对应的SSTable
    uint64_t tempKey;
    int index;                                      //对应的SSTable序号
    string tempValue;
    map<int64_t, string> newTable;                 //暂存合并后的键值对

    while(!sortTable.empty())
    {
        map<int64_t, string> KVPair;
        sortTable.top().traverse(KVPair);
        sortTable.pop();
        KVToCompact.push_back(KVPair);
    }

    //获取键值对迭代器
    for(int i=0; i<KVToCompact.size(); ++i)
    {
        auto iter = KVToCompact[i].begin();

        if(minKey.count(iter->first)==0 || i>minKey[iter->first])  //如果键相同，保留时间戳较大的
        {
            minKey[iter->first] = i;
        }
        KVToCompactIter.push_back(iter);
    }

    //只要minKey不为空，minKey的第一个元素一定为所有SSTable中的最小键
    //每个循环将minKey中的最小键和对应的值加入newTable
    while(!minKey.empty())
    {
        auto iter = minKey.begin();
        tempKey = iter->first;
        index = iter->second;
        tempValue = KVToCompactIter[index]->second;
        size += tempValue.size() + 1 + 12;
        if(size > MEMTABLE)
        {
            writeToFile(level, timestamp, newTable.size(), newTable);
            size = InitialSize + tempValue.size() + 1 + 12;
        }
        newTable[tempKey] = tempValue;
        minKey.erase(tempKey);
        KVToCompactIter[index]++;
        if(KVToCompactIter[index]!=KVToCompact[index].end())
        {
            int64_t select = KVToCompactIter[index]->first;
            if(minKey.count(select)==0 || index>minKey[select])
                minKey[select] = index;
        }
    }

    //不足2MB的文件也要写入
    if(!newTable.empty())
        writeToFile(level, timestamp, newTable.size(), newTable);

    //删除level和level-1中的被合并文件
    for(auto &file:FileToRemoveLevelminus1)
    {
        utils::rmfile(file.data());
        SSTable[level-1].erase(file);
    }

    for(auto &file:FileToRemoveLevel)
    {
        utils::rmfile(file.data());
        SSTable[level].erase(file);
    }

    compactionForLevel(level+1);
}

/**
 * 对Level0层的合并
 * 统计所有Level0层的SSTable键区间，在Level1中寻找有交集的文件
 * 将这些SSTable按照键值对大小重新排序，生成新的SSTable
 *
 * 因为各层之间的合并是相同的逻辑，将代码重构为各层适用
 */

/*void KVStore::compactionForLevel0()
{
    //若没有Level1文件夹，先创建
    string path1 = dir + "/level-1";
    if(!utils::dirExists(path1))
    {
        utils::mkdir(path1.c_str());
        Level.push_back(0);
        SSTable.emplace_back();
    }

    //遍历Level0中的SSTable，找到最大最小键
    uint64_t timestamp = 0;
    int64_t tempMin = INT64_MAX, tempMax = INT64_MIN;
    for(auto &table:SSTable[0])
    {
        if(table.getTimestamp()>timestamp)
            timestamp = table.getTimestamp();
        if(table.getMaxKey()>tempMax)
            tempMax = table.getMaxKey();
        if(table.getMinKey()<tempMin)
            tempMin = table.getMinKey();
    }

    //
    vector<int> SStableInLevel1;
    for(int i=0; i<SSTable[1].size(); ++i)
    {
        if(SSTable[1][i].getMaxKey()>=tempMin && SSTable[1][i].getMinKey()<=tempMax)
        {
            SStableInLevel1.push_back(i);
        }
    }

    vector<map<int64_t, string>> KVToCompact;                   //被合并的键值对，下标越大时间戳越大
    vector<map<int64_t, string>::iterator> KVToCompactIter;     //键值对迭代器
    int size = InitialSize;
    map<int64_t, int> minKey;                      //minKey中只存放num个数据，分别为各个SSTable中最小键和对应的SSTable
    uint64_t tempKey;
    int index;                                      //对应的SSTable序号
    string tempValue;
    map<int64_t, string> newTable;                 //暂存合并后的键值对

    //考虑Level1层，将table按键值对排序
    priority_queue<Table> sortTable;
    for(int i:SStableInLevel1)
    {
        sortTable.push(SSTable[1][i]);
    }

    while(!sortTable.empty())
    {
        map<int64_t, string> KVPair;
        sortTable.top().traverse(KVPair);
        sortTable.pop();
        KVToCompact.push_back(KVPair);
    }

    //Level0层，后面生成的文件时间戳一定大于之前的文件
    for(int i=0; i<SSTable[0].size(); ++i)
    {
        map<int64_t, string> KVPair;
        SSTable[0][i].traverse(KVPair);
        KVToCompact.push_back(KVPair);
    }

    for(int i=0; i<KVToCompact.size(); ++i)
    {
        auto iter = KVToCompact[i].begin();

        if(minKey.count(iter->first)==0 || i>minKey[iter->first])  //如果键相同，保留时间戳较大的
        {
            minKey[iter->first] = i;
        }
        KVToCompactIter.push_back(iter);
    }

    //只要minKey不为空，minKey的第一个元素一定为所有SSTable中的最小键
    //每个循环将minKey中的最小键和对应的值加入newTable
    while(!minKey.empty())
    {
        auto iter = minKey.begin();
        tempKey = iter->first;
        index = iter->second;
        tempValue = KVToCompactIter[index]->second;
        size += tempValue.size() + 1 + 12;
        if(size > MEMTABLE)
        {
            writeToFile(timestamp, newTable.size(), newTable);
            size = InitialSize + tempValue.size() + 1 + 12;
        }
        newTable[tempKey] = tempValue;
        minKey.erase(tempKey);
        KVToCompactIter[index]++;
        if(KVToCompactIter[index]!=KVToCompact[index].end())
        {
            int64_t select = KVToCompactIter[index]->first;
            if(minKey.count(select)==0 || index>minKey[select])
                minKey[select] = index;
        }
    }

    //不足2MB的文件也要写入
    if(!newTable.empty())
        writeToFile(timestamp, newTable.size(), newTable);

    //合并后将被合并的文件删除
    for(auto ss:SSTable[0])
    {
        utils::rmfile(ss.getFileName().data());
    }
    SSTable[0].clear();
    for(int i=SStableInLevel1.size()-1; i>=0; --i)
    {
        utils::rmfile(SSTable[1][SStableInLevel1[i]].getFileName().data());
        SSTable[1].erase(SSTable[1].begin()+SStableInLevel1[i], SSTable[1].begin()+SStableInLevel1[i]+1);
    }
}*/

void KVStore::writeToFile(int level, uint64_t timeStamp, uint64_t numPair, map<int64_t, string> &newTable)
{
    string path = dir + "/level" + to_string(level);
    Level[level]++;
    string FileName = path + "/SSTable" + to_string(Level[level]) + ".sst";
    fstream outFile(FileName, std::ios::app | std::ios::binary);

    auto iter1 = newTable.begin();
    int64_t minKey = iter1->first;
    auto iter2 = newTable.rbegin();
    int64_t maxKey = iter2->first;

    //写入时间戳、键值对个数和最小最大键
    outFile.write((char *)(&timeStamp), sizeof(uint64_t));
    outFile.write((char *)(&numPair), sizeof(uint64_t));
    outFile.write((char *)(&minKey), sizeof(int64_t));
    outFile.write((char *)(&maxKey), sizeof(int64_t));

    //写入生成对应的布隆过滤器
    bitset<81920> filter;
    int64_t tempKey;
    const char* tempValue;
    unsigned int hash[4] = {0};
    while(iter1!=newTable.end())
    {
        tempKey = iter1->first;
        MurmurHash3_x64_128(&tempKey, sizeof(tempKey), 1, hash);
        for(auto i:hash)
            filter.set(i%81920);
        iter1++;
    }
    outFile.write((char *)(&filter), sizeof(filter));

    //索引区，计算key对应的索引值
    const int dataArea = 10272 + numPair * 12;   //数据区开始的位置
    uint32_t index = 0;
    int length = 0;
    iter1 = newTable.begin();
    while(iter1!=newTable.end())
    {
        tempKey = iter1->first;
        index = dataArea + length;
        outFile.write((char *)(&tempKey), sizeof(int64_t));
        outFile.write((char *)(&index), sizeof(uint32_t));
        length += (iter1->second).size()+1;
        iter1++;
    }

    //数据区，存放value
    iter1 = newTable.begin();
    while(iter1!=newTable.end())
    {
        tempValue = (iter1->second).c_str();
        outFile.write(tempValue, sizeof(char)* ((iter1->second).size()));
        tempValue = "\0";
        outFile.write(tempValue, sizeof(char)* 1);
        iter1++;
    }
    outFile.close();

    Table newSSTable(FileName);
    SSTable[level][FileName] = newSSTable;

    newTable.clear();
}
