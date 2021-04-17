#include "kvstore.h"
#include "utils.h"

KVStore::KVStore(const std::string &dir): KVStoreAPI(dir)
{
    memTable = new SkipList;
    this->dir = dir;
}

//todo:将 MemTable 中的所有数据以 SSTable 形式写回
KVStore::~KVStore()
{
    /*if(memTable->getPairNum()!=0)
        memTable->store(dir);*/
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
        //if(utils::scanDir(path, ret)<2)
        memTable->store(num, path);
    }
    memTable->put(key, s);
}
/**
 * Returns the (string) value of the given key.
 * An empty string indicates not found.
 */
std::string KVStore::get(uint64_t key)
{
	return memTable->get(key);
}
/**
 * Delete the given key-value pair if it exists.
 * Returns false iff the key is not found.
 */
bool KVStore::del(uint64_t key)
{
	return memTable->remove(key);
}

/**
 * This resets the kvstore. All key-value pairs should be removed,
 * including memtable and all sstables files.
 */
void KVStore::reset()
{
    delete memTable;
}
