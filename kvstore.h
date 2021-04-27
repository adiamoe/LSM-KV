#pragma once

#include "kvstore_api.h"
#include "Table.h"
#include "SkipList.h"
#include <map>
#include <queue>

using namespace std;

static inline int UpperNum(int i) {return pow(2, i+1);}

class KVStore : public KVStoreAPI {
	// You can add your implementation here
private:
    SkipList *memTable;
    string dir;
    vector<int> Level; //记录对应层的文件数目
    vector<map<string, Table>> SSTable;
public:
	KVStore(const std::string &dir);

	~KVStore();

	void put(uint64_t key, const std::string &s) override;

	std::string get(uint64_t key) override;

	bool del(uint64_t key) override;

	void reset() override;

	void compactionForLevel(int level);

	void writeToFile(int level, uint64_t timeStamp, uint64_t numPair, map<int64_t, string> &newTable);
};
