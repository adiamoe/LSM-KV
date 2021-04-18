#include "kvstore.h"
#include "utils.h"

KVStore::KVStore(const std::string &dir): KVStoreAPI(dir)
{
    memTable = new SkipList;
    this->dir = dir;
    Level.push_back(0);
    reset();
}

//todo:将 MemTable 中的所有数据以 SSTable 形式写回
KVStore::~KVStore()
{
    /*if(memTable->getPairNum()!=0)
        memTable->store(dir);*/
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
    if(val!="")
        memTable->memory += s.size() - val.size();
    else
        memTable->memory += 4 + 8 + s.size();  //索引值 + key + value所占的内存大小

    if(memTable->memory > MEMTABLE)
    {
        string path = dir + "/level-0";
        if(!utils::dirExists(path))
            utils::mkdir(path.c_str());
        vector<string> ret;
        int num = utils::scanDir(path, ret);
        if(Level[0]==2)
        {
            memTable->store(3, path);
            compactionForLevel0();
            Level[0] = 0;
        }
        else
        {
            Level[0]++;
            memTable->store(Level[0], path);
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

	//当读到删除标记时，返回空
	if(ans == "~DELETED~")
	    return "";
	return ans;
}
/**
 * Delete the given key-value pair if it exists.
 * Returns false iff the key is not found.
 */
bool KVStore::del(uint64_t key)
{
    if(get(key)!="")
    {
        put(key, "~DELETED~");
        return true;
    }
    else{
        put(key, "~DELETED~");
        return false;
    }
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
        if(minKey>metadata[2])
            minKey = metadata[2];
        if(maxKey<metadata[3])
            maxKey = metadata[3];
    }

    exit(0);
}
