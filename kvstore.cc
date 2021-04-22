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
        cout<<numSST<<endl;
        for(int j=0; j<numSST; ++j)
        {
            string FileName = path + "/" + file[j];
            Table sstable(FileName);
            SSTable[i].push_back(sstable);
        }
    }
    cout<<"!"<<endl;
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
        /*if(Level[0]==2)
        {
            memTable->store(3, path);
            compactionForLevel0();
            Level[0] = 0;
        }
        else
        {
            Level[0]++;
            memTable->store(Level[0], path);
        }*/
        Level[0]++;
        memTable->store(Level[0], path);
        string newFile = path + "/SSTable" + to_string(Level[0]) + ".sst";
        Table newTable(newFile);
        if(SSTable.empty())
            SSTable.emplace_back();
        SSTable[0].push_back(newTable);
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
	//cout<<"here!"<<endl;
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

void KVStore::compactionForLevel0()
{
    string path1 = dir + "/level-1";
    if(!utils::dirExists(path1))
    {
        utils::mkdir(path1.c_str());
        Level.push_back(0);
    }
    string path0 = dir + "/level-0";
    vector<string> ret;
    utils::scanDir(path0, ret);
    uint64_t minKey = INT64_MAX, maxKey = INT64_MIN;
    uint64_t metadata[4];
    //cout<<ret.size()<<endl;
    for(int i=0; i<ret.size(); ++i)
    {
        string file = path0 + "/" +ret[i];
        fstream fin(file, ios::in|ios::binary);
        fin.read((char*)(&metadata), 4*sizeof(uint64_t));
        if(minKey > metadata[2])
            minKey = metadata[2];
        if(maxKey < metadata[3])
            maxKey = metadata[3];
    }

    exit(0);
}
