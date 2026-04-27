//
// Created by 27044 on 26-3-19.
//

#ifndef ASIOIOCONTEXTPOOL_H
#define ASIOIOCONTEXTPOOL_H

#include "const.h"

using Work = boost::asio::executor_work_guard<boost::asio::io_context::executor_type>;
using Ctx = boost::asio::io_context;

using WorkPtr = std::unique_ptr<Work>;
using CtxPtr = std::unique_ptr<Ctx>;

class AsioIoContextPool: public Singleton<AsioIoContextPool>
{
	friend class Singleton<AsioIoContextPool>;

public:
	~AsioIoContextPool();

	Ctx& getNextIoContext();

private:
	AsioIoContextPool(int poolSize = 4); // 这里可以采用std::thread::hardware_concurrency()

	void stop();

	bool _stop;

	int _poolSize;

	int _nextCtxIndex;

	// 牢记：asio::io_context不能拷贝（理所当然），也不能移动（注意），详情省略（其在内存的地址十分重要，不能随便移动）
	// 用智能指针来指向io_context，是因为vector的扩容机制会拷贝/移动内部元素，所以只能用智能指针保存堆ioContext
	std::vector<CtxPtr> _contextsPtr;

	std::vector<WorkPtr> _worksPtr;

	std::vector<std::jthread> _jthreads;
};


#endif //ASIOIOCONTEXTPOOL_H
