//
// Created by 靖晏 on 2021/8/4.
//

#ifndef THREADPOOL_THREADPOOL_H
#define THREADPOOL_THREADPOOL_H

#include <vector>
#include <queue>
#include <memory>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <future>
#include <functional>
#include <stdexcept>
#include <atomic>

class ThreadPool {
public:
    explicit ThreadPool(size_t);
    template<class F, class... Args>
    auto enqueue(F&& f, Args&&... args)
    -> std::future<typename std::result_of<F(Args...)>::type>;
    ~ThreadPool();
private:
    // need to keep track of threads so we can join them
    std::vector< std::thread > workers;
    // the task queue
    std::queue< std::function<void()> > tasks;

    // synchronization
    std::mutex queue_mutex;
    std::condition_variable condition;
    atomic<bool> stop;
};

// the constructor just launches some amount of workers
inline ThreadPool::ThreadPool(size_t threads)
        :   stop(false)
{
    for(size_t i = 0;i<threads;++i)
        workers.emplace_back(
                [this]
                {
                    while(true)
                    {
                        std::function<void()> task;

                        {
                            std::unique_lock<std::mutex> lock(this->queue_mutex);
                            this->condition.wait(lock,
                                                 [this]{ return this->stop.load() || !this->tasks.empty(); });
                            if(this->stop.load() && this->tasks.empty())
                                return;
                            task = std::move(this->tasks.front());
                            this->tasks.pop();
                        }

                        task();
                    }
                }
        );
}

// add new work item to the pool
template<class F, class... Args>
auto ThreadPool::enqueue(F&& f, Args&&... args)
-> std::future<typename std::result_of<F(Args...)>::type>
{
    using return_type = typename std::result_of<F(Args...)>::type;

    auto task = std::make_shared< std::packaged_task<return_type()> >(
            std::bind(std::forward<F>(f), std::forward<Args>(args)...)
    );

    std::future<return_type> res = task->get_future();
    {
        std::unique_lock<std::mutex> lock(queue_mutex);

        // don't allow enqueueing after stopping the pool
        if(stop.load())
            throw std::runtime_error("KVStore has been closed");

        tasks.emplace([task](){ (*task)(); });
    }
    condition.notify_one();
    return res;
}

// the destructor joins all threads
inline ThreadPool::~ThreadPool()
{
    stop.store(true);
    condition.notify_all();
    for(std::thread &worker: workers)
        worker.join();
}

#endif //THREADPOOL_THREADPOOL_H
