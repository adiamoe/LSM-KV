#pragma once

#include "kvstore_api.h"
#include "SkipList.h"
using namespace std;


class KVStore : public KVStoreAPI {
	// You can add your implementation here
private:
    SkipList *memTable;
    string dir;
    vector<int> level;
    //todo:内存池，用于储存SSTable中的索引
public:
	KVStore(const std::string &dir);

	~KVStore();

	void put(uint64_t key, const std::string &s) override;

	std::string get(uint64_t key) override;

	bool del(uint64_t key) override;

	void reset() override;

};
