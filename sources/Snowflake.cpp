
#include "../headers/Snowflake.h"

Snowflake::Snowflake(int64_t dataCenterId, int64_t workerId):
	_sequenceId(0LL),
	_lastTimestamp(-1LL)
{
	if (dataCenterId < 0LL || dataCenterId > MAX_DATACENTER_ID)
		throw std::invalid_argument("Datacenter ID exceeded max limit or is less than 0");

	if (workerId < 0LL || workerId > MAX_WORKER_ID)
		throw std::invalid_argument("Worker ID exceeded max limit or is less than 0");

	_dataCenterId = dataCenterId;
	_workerId = workerId;
}

int64_t Snowflake::getCurrentTimestampMillis() const
{
	auto now = std::chrono::system_clock::now();
	auto duration = now.time_since_epoch();
	return std::chrono::duration_cast<std::chrono::milliseconds>(duration).count();
}

int64_t Snowflake::waitNextMillis(int64_t lastTimestamp) const
{
	int64_t timestamp = getCurrentTimestampMillis();
	// 循环等待，直到时间推移到下一毫秒
	while (timestamp <= lastTimestamp)
	{
		timestamp = getCurrentTimestampMillis();
	}
	return timestamp;
}

int64_t Snowflake::nextId()
{
	std::scoped_lock lock(_mutex);
	int64_t timestamp = getCurrentTimestampMillis();

	// 发生时钟回拨：当前时间小于上一次生成 ID 的时间
	if (timestamp < _lastTimestamp)
		throw std::runtime_error("Clock moved backwards. Refusing to generate id");

	// 同一毫秒最多生成2^12个序列化
	// 如果在同一毫秒内生成
	if (timestamp == _lastTimestamp)
	{
		_sequenceId = (_sequenceId + 1) & SEQUENCE_MASK;
		// 序列号溢出（毫秒内超过 4096 个）
		if (_sequenceId == 0)
			// 阻塞等待下一个毫秒
			timestamp = waitNextMillis(_lastTimestamp);
	}
	// 不同时间戳，序列化重置
	else
	{
		_sequenceId = 0LL;
	}

	// 更新上次生成 ID 的时间戳
	_lastTimestamp = timestamp;

	return ((timestamp - EPOCH) << TIMESTAMP_LEFT_SHIFT) |
		(_dataCenterId << DATACENTER_ID_SHIFT) |
		(_workerId << WORKER_ID_SHIFT) |
		_sequenceId;
}
