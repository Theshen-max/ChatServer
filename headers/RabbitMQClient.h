#ifndef RABBITMQCLIENT_H
#define RABBITMQCLIENT_H

#include "const.h"
#include "Singleton.h"
#include <amqpcpp.h>
#include "AmqpAsioHandler.h"

using Work = boost::asio::executor_work_guard<boost::asio::io_context::executor_type>;

struct MqPendingMsg {
	std::string routingKey;
	std::string payload;
};

class RabbitMQClient: public Singleton<RabbitMQClient>
{
	friend class Singleton<RabbitMQClient>;
public:
	bool init(const std::string& host, const std::string& port, const std::string& user, const std::string& passwd, const std::string& exchangeName);

	void stop();

	void publish(const std::string& routingKey, const std::string& message);

	~RabbitMQClient();
private:
	RabbitMQClient() = default;

	void doPublish(const std::string& routingKey, const std::string& message);

	std::string _exchangeName;

	// MQ 专属的事件循环和线程
	boost::asio::io_context _ioc;
	std::unique_ptr<Work> _worker;
	std::jthread _thread;

	// AMQP-CPP组件
	std::shared_ptr<AmqpAsioHandler> _handler; // 注意handler与connection的声明定义顺序，connection含有handler的指针
	std::unique_ptr<AMQP::Connection> _connection;	// handler生命周期必须比connection长
	std::unique_ptr<AMQP::Channel> _channel;

	// 可靠性投递核心机制
	uint64_t _deliveryTagCount = 1;
	std::map<uint64_t, MqPendingMsg> _inflightMsgs;
};

#endif //RABBITMQCLIENT_H
