#pragma once

#include "kvstore_api.h"
#include "TableCache.h"
#include "SkipList.h"
#include "threadPool.h"
#include <map>
#include <set>
#include <queue>
#include <mutex>
#include <shared_mutex>

using namespace std;

static inline int UpperNum(int i) {return pow(2, i+1);}
const string DEL = "~DELETED~";

class KVStore : public KVStoreAPI {
	// You can add your implementation here
private:
    enum mode{
        normal,
        compact,
        exits
    };

    shared_ptr<SkipList> memTable;
    shared_ptr<SkipList> immutableTable;
    string dir;
    vector<int> Level; //记录对应层的文件数目
    vector<set<TableCache>> SSTable;
    mode kvStoreMode;
    condition_variable cv;
    mutex m;
    shared_mutex read_write;
    ThreadPool pool{4};

public:
	KVStore(const std::string &dir);

	~KVStore();

	void put(uint64_t key, const std::string &s) override;

	std::string get(uint64_t key) override;

	bool del(uint64_t key) override;

	future<string> getTask(uint64_t key);

    void delTask(uint64_t key);

    void putTask(uint64_t key, const std::string &s);

	void reset() override;

	void compaction();

	void compactionForLevel(int level);

	void writeToFile(int level, uint64_t timeStamp, uint64_t numPair, map<int64_t, string> &newTable);
};
