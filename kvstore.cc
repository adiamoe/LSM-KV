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
            SSTable[i].push_back(sstable);
        }
    }
    if(SSTable.empty())
        SSTable.emplace_back();
}

//将 MemTable 中的所有数据以 SSTable 形式写进磁盘
KVStore::~KVStore()
{
    string path = dir + "/level-0";
    memTable->store(++Level[0], path);
    if(Level[0]==3)
        compactionForLevel0();
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
        string path = dir + "/level-0";
        if(!utils::dirExists(path))
            utils::mkdir(path.c_str());

        Level[0]++;
        memTable->store(Level[0], path);
        string newFile = path + "/SSTable" + to_string(Level[0]) + ".sst";
        Table newTable(newFile);
        SSTable[0].push_back(newTable);
        if(Level[0]==3)
        {
            compactionForLevel0();
            Level[0] = 0;
        }
    }
    memTable->put(key, s);
}
/**
 * Returns the (string) value of the given key.
 * An empty string indicates not found.
 */
 //todo:考虑时间戳
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

	for(const auto &tableList:SSTable)
    {
	    for(auto table = tableList.rbegin(); table!=tableList.rend(); ++table)
        {
	        string out = table->getValue(key);
	        if(!out.empty())
            {
	            if(out=="~DELETED~")
	                return "";
	            else
                    return out;
            }
        }
    }
	return "";
}
/**
 * Delete the given key-value pair if it exists.
 * Returns false iff the key is not found.
 */
bool KVStore::del(uint64_t key)
{
    bool flag = !get(key).empty();
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
    for(int i=0; i<Level.size(); ++i)
    {
        string path = dir + "/level-" + to_string(i);
        vector<string> ret;
        int num = utils::scanDir(path, ret);
        for(int i=0; i<num; ++i)
            utils::rmfile((path + "/" +ret[i]).c_str());
        utils::rmdir(path.c_str());
    }
}

/**
 * 对Level0层的合并
 * 统计所有Level0层的SSTable键区间，在Level1中寻找有交集的文件
 * 将这些SSTable按照键值对大小重新排序，生成新的SSTable
 */

void KVStore::compactionForLevel0()
{
    //若没有Level1文件夹，先创建
    string path1 = dir + "/level-1";
    if(!utils::dirExists(path1))
    {
        utils::mkdir(path1.c_str());
        Level.push_back(0);
    }

    //遍历Level0中的SSTable，找到最大最小键
    uint64_t timestamp = INT64_MIN;
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

    vector<map<int64_t, string>> KVToCompact;                   //被合并的键值对
    vector<map<int64_t, string>::iterator> KVToCompactIter;     //键值对迭代器
    int num=3;                                      //合并的SSTable个数
    int size = InitialSize;
    map<int64_t, int> minKey;                      //minKey中只存放num个数据，分别为各个SSTable中最小键和对应的SSTable
    uint64_t tempKey;
    int index;                                      //对应的SSTable序号
    string tempValue;
    map<int64_t, string> newTable;                 //暂存合并后的键值对
    uint64_t numPair=0;

    for(int i=0; i<num; ++i)
    {
        map<int64_t, string> KVPair;
        SSTable[0][i].traverse(KVPair);             //读取SSTable中的全部键值对
        KVToCompact.push_back(KVPair);
        auto iter = KVToCompact[i].begin();

        if(minKey.count(iter->first)==0 || i>minKey[iter->first])      //时间戳更大的才能覆盖原有的值
            minKey[iter->first] = i;

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
            writeToFile(timestamp, numPair, newTable);
            size = InitialSize + tempValue.size() + 1 + 12;
            numPair = 0;
        }
        newTable[tempKey] = tempValue;
        numPair++;
        minKey.erase(tempKey);
        if(KVToCompactIter[index]!=KVToCompact[index].end())
        {
            KVToCompactIter[index]++;
            int64_t select = KVToCompactIter[index]->first;
            if(minKey.count(select)==0 || index>minKey[select])
                minKey[select] = index;
        }
    }

}

void KVStore::writeToFile(uint64_t timeStamp, uint64_t numPair, map<int64_t, string> &newTable)
{
    string path = dir + "/level-1";
    vector<string> file;
    int numSST = utils::scanDir(path, file);
    string FileName = path + "/SSTable" + to_string(numSST+1) + ".sst";
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
        outFile.write((char *)(&tempKey), sizeof(uint64_t));
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
    newTable.clear();
}
