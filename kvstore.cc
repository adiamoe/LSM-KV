#include "kvstore.h"
#include <string>
#include <iostream>

KVStore::KVStore(const std::string &dir): KVStoreAPI(dir)
{
    memTable = new SkipList;
}

//todo:将 MemTable 中的所有数据以 SSTable 形式写回
KVStore::~KVStore()
{

}

/**
 * Insert/Update the key-value pair.
 * No return values for simplicity.
 */
void KVStore::put(uint64_t key, const string &s)
{
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
