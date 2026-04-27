#ifndef SNOWFLAKE_H
#define SNOWFLAKE_H

#include "const.h"

class Snowflake
{
public:
	Snowflake(int64_t dataCenterId, int64_t workerId);

	int64_t nextId();

private:
	// 强烈建议设置一个接近系统上线时间的纪元，这样可用时间更长
	static constexpr int64_t EPOCH = 1704067200000LL;

	// 各部分占用的位数
	static constexpr int64_t WORKER_ID_BITS = 5LL;
	static constexpr int64_t DATACENTER_ID_BITS = 5LL;
	static constexpr int64_t SEQUENCE_BITS = 12LL;

	// 各部分的最大值
	static constexpr int64_t MAX_WORKER_ID = (1LL << WORKER_ID_BITS) - 1;
	static constexpr int64_t MAX_DATACENTER_ID = (1LL << DATACENTER_ID_BITS) - 1;
	static constexpr int64_t MAX_SEQUENCE_ID = (1LL << SEQUENCE_BITS) - 1;

	// 各部分向左的位移量
	static constexpr int64_t WORKER_ID_SHIFT = SEQUENCE_BITS;
	static constexpr int64_t DATACENTER_ID_SHIFT = SEQUENCE_BITS + WORKER_ID_BITS;
	static constexpr int64_t TIMESTAMP_LEFT_SHIFT = SEQUENCE_BITS + WORKER_ID_BITS + DATACENTER_ID_BITS;

	// 序列号的掩码 (用于一毫秒内序列号溢出时的归零操作，结果为 4095)
	static constexpr int64_t SEQUENCE_MASK = (1LL << SEQUENCE_BITS) - 1;

	int64_t _dataCenterId;
	int64_t _workerId;
	int64_t _sequenceId;
	int64_t _lastTimestamp;

	// 保证线程安全的互斥锁
	std::mutex _mutex;

	// 内部辅助函数:
	// 获取当前时间的毫秒数
	int64_t getCurrentTimestampMillis() const;

	int64_t waitNextMillis(int64_t lastTimestamp) const;
};

#endif //SNOWFLAKE_H
