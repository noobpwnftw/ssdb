
#pragma once

#include <mutex>
#include <condition_variable>
#include <deque>
template <typename T>
class BlockQueue
{
 private:
    std::mutex              d_mutex;
    std::condition_variable d_condition;
    std::deque<T>           d_queue;
 public:
    int push(T const& value) {
	int ret;
	{
	    std::unique_lock<std::mutex> lock(this->d_mutex);
	    this->d_queue.push_front(value);
	    ret = this->d_queue.size();
	}
	this->d_condition.notify_one();
	return ret;
    }
    T pop() {
	std::unique_lock<std::mutex> lock(this->d_mutex);
	while (this->d_queue.empty()) {
	    this->d_condition.wait(lock, [=]{ return !this->d_queue.empty(); });
	}
	T rc(std::move(this->d_queue.back()));
	this->d_queue.pop_back();
	return rc;
    }
    T timed_pop(int seconds) {
	std::unique_lock<std::mutex> lock(this->d_mutex);
	if (this->d_condition.wait_for(lock, std::chrono::seconds(seconds),
				       [=]{ return !this->d_queue.empty(); })) {
	    T rc(std::move(this->d_queue.back()));
	    this->d_queue.pop_back();
	    return rc;
	} else {
	    return T();
	}
    }
    T top() {
	std::unique_lock<std::mutex> lock(this->d_mutex);
	return this->d_queue.back();
    }
};
