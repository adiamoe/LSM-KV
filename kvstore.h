#pragma once

#include "kvstore_api.h"
#include "Table.h"
#include "SkipList.h"
#include <map>
using namespace std;


class KVStore : public KVStoreAPI {
	// You can add your implementation here
private:
    SkipList *memTable;
    string dir;
    vector<int> Level; //记录对应层的文件数目
    vector<vector<Table>> SSTable;
public:
	KVStore(const std::string &dir);

	~KVStore();

	void put(uint64_t key, const std::string &s) override;

	std::string get(uint64_t key) override;

	bool del(uint64_t key) override;

	void reset() override;

	void compactionForLevel0();

	void compactionForLeveln(int level);

	void writeToFile(uint64_t timeStamp, uint64_t numPair, map<int64_t, string> &newTable);
};
