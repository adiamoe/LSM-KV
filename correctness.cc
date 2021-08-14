#include <iostream>
#include <cstdint>
#include <string>

#include "test.h"

class CorrectnessTest : public Test {
private:
	const uint64_t SIMPLE_TEST_MAX = 512;
	const uint64_t LARGE_TEST_MAX = 1024 * 20;

	void regular_test(uint64_t max)
	{
		uint64_t i;

		// Test a single key
		/*EXPECT(not_found, store.get(1));
		store.put(1, "SE");
		EXPECT("SE", store.get(1));
		EXPECT(true, store.del(1));
		EXPECT(not_found, store.get(1));
		EXPECT(false, store.del(1));

		phase();*/

        // Test multiple key-value pairs
        for (i = 0; i < max; ++i) {
            store.put(i, std::string(i+1, 's'));
            EXPECT(std::string(i+1, 's'), store.get(i));
        }
        phase();

        vector<future<string>> tasks;
        for(i=5; i<=200; i+=3) {
            tasks.emplace_back(store.getTask(i*50));
            tasks.emplace_back(store.getTask((i+1)*50));
            tasks.emplace_back(store.getTask((i+2)*50));
            store.putTask(i+5*i, std::string(i+5*i+1, 's'));
        }


        for(i=5; i<=100; ++i) {
            EXPECT(std::string((i*50)+1, 's'), tasks[i-5].get());
        }
        phase();

        // Test after all insertions
        /*for (i = 0; i < max; ++i)
            EXPECT(std::string(i+1, 's'), store.get(i));
        phase();*/

        for (i = 1; i < max; i+=4) {
            EXPECT(std::string(i, 's'), store.get(i-1));
        }
        phase();

        /*for (i = 0; i < max; i+=7)
            EXPECT(std::string(i+2, 's'), store.get(i));
        phase();*/

		//Test deletions
		for (i = 0; i < max; i+=2)
			store.del(i);

		for (i = 0; i < max; ++i)
			EXPECT((i & 1) ? std::string(i+1, 's') : not_found,
			       store.get(i));
		phase();

		report();
	}

public:
	CorrectnessTest(const std::string &dir, bool v=true) : Test(dir, v)
	{
	}

	void start_test(void *args = NULL) override
	{
		std::cout << "KVStore Correctness Test" << std::endl;

		//std::cout << "[Simple Test]" << std::endl;
		//regular_test(SIMPLE_TEST_MAX);

		std::cout << "[Large Test]" << std::endl;
		regular_test(LARGE_TEST_MAX);
	}
};

int main(int argc, char *argv[])
{
	bool verbose = true;

//	//std::cout << "Usage: " << argv[0] << " [-v]" << std::endl;
//	std::cout << "  -v: print extra info for failed tests [currently ";
//	std::cout << (verbose ? "ON" : "OFF")<< "]" << std::endl;
//	std::cout << std::endl;
//	std::cout.flush();

	CorrectnessTest test("./data", verbose);

	test.start_test();

	return 0;
}
