#include "../headers/AsioIOContextPool.h"

AsioIoContextPool::AsioIoContextPool(int poolSize) :
	_poolSize(poolSize),
	_stop(false),
	_nextCtxIndex(0)
{
	for (int i = 0; i < _poolSize; ++i)
	{
		_contextsPtr.emplace_back(std::make_unique<boost::asio::io_context>(1));
		_worksPtr.emplace_back(std::make_unique<Work>(_contextsPtr[i]->get_executor()));
		_jthreads.emplace_back([this, i]
		{
			_contextsPtr[i]->run();
		});
	}
}

AsioIoContextPool::~AsioIoContextPool()
{
	if (!_stop)
	{
		stop();
	}
}

Ctx& AsioIoContextPool::getNextIoContext()
{
	auto& ctx = _contextsPtr[_nextCtxIndex++];
	_nextCtxIndex %= _poolSize;
	return *ctx;
}

void AsioIoContextPool::stop()
{
	if (!_stop)
	{
		_stop = true;

		for (int i = 0; i < _poolSize; ++i)
		{
			_worksPtr[i]->reset();
			_contextsPtr[i]->stop();
		}
	}
}