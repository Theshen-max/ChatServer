#include "../headers/RabbitMQClient.h"

bool RabbitMQClient::init(const std::string& host, const std::string& port, const std::string& user, const std::string& passwd, const std::string& exchangeName)
{
	_exchangeName = exchangeName;
	_worker = std::make_unique<Work>(_ioc.get_executor());

	// 启动专属IO线程来启动MQ事件循环
	_thread = std::jthread([this]
	{
		std::cout << "[RabbitMQ] I/O Thread started." << std::endl;
		_ioc.run();
		std::cout << "[RabbitMQ] I/O Thread stopped." << std::endl;
	});

	auto initPromise = std::make_shared<std::promise<bool>>();
	std::future<bool> initFuture = initPromise->get_future();
	// 使用 std::shared_ptr<bool> 防止多次 set_value 导致崩溃
	auto promiseResolved = std::make_shared<std::atomic<bool>>(false);

	try
	{
		// 初始化Asio句柄
		_handler = std::make_shared<AmqpAsioHandler>(_ioc);
		// 建立TCP连接
		_connection = std::make_unique<AMQP::Connection>(_handler.get(), AMQP::Login(user, passwd), "/");
		_handler->setConnection(_connection.get());

		_handler->setReadyCallback([this, initPromise, promiseResolved]()
		{
			std::cout << "[RabbitMQ Producer] AMQP Connection is Ready! Creating Channel..." << std::endl;

			// 此时创建 Channel 绝对安全
			_channel = std::make_unique<AMQP::Channel>(_connection.get());

			// 监听 Channel 的错误
			_channel->onError([initPromise, promiseResolved](const char* message) {
				std::cerr << "[RabbitMQ Producer] Channel error: " << message << std::endl;
				if (!promiseResolved->exchange(true)) {
					initPromise->set_value(false);
				}
			});

			_channel->onReady([this, initPromise, promiseResolved]() {
				std::cout << "[RabbitMQ Producer] Channel is Ready! Declaring Exchange..." << std::endl;
				// 声明直连交换机（持久化）
				/**
				 * 1. 交换机名字
				 * 2. 交换机类型
				 * 3. 交换机标志
				*/
				_channel->declareExchange(_exchangeName, AMQP::direct, AMQP::durable)
				.onSuccess([initPromise, promiseResolved]() {
					std::cout << "[RabbitMQ Producer] Exchange declared successfully!" << std::endl;
					if (!promiseResolved->exchange(true)) {
						initPromise->set_value(true);
					}
				})
				.onError([initPromise, promiseResolved](const char* message) {
					std::cerr << "[RabbitMQ Producer] Exchange declare failed: " << message << std::endl;
					if (!promiseResolved->exchange(true)) {
						initPromise->set_value(false);
					}
				});
				// 声明选择确认
				_channel->confirmSelect()
				.onAck([this](uint64_t deliveryTag, bool multiple)
				{
					if (multiple)
					{
						// multiple=true 代表 <= deliveryTag 的所有消息都成功了
						_inflightMsgs.erase(_inflightMsgs.begin(), _inflightMsgs.upper_bound(deliveryTag));
					}
					else
						_inflightMsgs.erase(deliveryTag);
				})
				.onNack([this](uint64_t deliveryTag, bool multiple, bool requeue)
				{
					std::cout << "[RabbitMQ] Received NACK for tag: " << deliveryTag << ", triggering retransmit!" << std::endl;
					auto resendTask = [this](uint64_t tag)
					{
						auto it = _inflightMsgs.find(tag);
						if (it != _inflightMsgs.end())
						{
							std::string rk = it->second.routingKey;
							std::string msg = it->second.payload;
							_inflightMsgs.erase(it);
							doPublish(rk, msg);
						}
					};

					if (multiple)
					{
						std::vector<uint64_t> tagsToResend;
						for (auto it = _inflightMsgs.begin(); it != _inflightMsgs.upper_bound(deliveryTag); ++it)
						{
							tagsToResend.push_back(it->first);
						}
						for (auto t: tagsToResend)
						{
							resendTask(t);
						}
					}
					else
						resendTask(deliveryTag);
				});
			});
		});
		_handler->setErrorCallback([initPromise, promiseResolved](const char* message) {
			std::cerr << "[RabbitMQ Producer] Connection level error: " << message << std::endl;
			if (!promiseResolved->exchange(true)) {
				initPromise->set_value(false);
			}
		});
		boost::asio::co_spawn(_ioc, [this, host, port, initPromise, promiseResolved]()->boost::asio::awaitable<void>
		{
			try
			{
				// 建立TCP连接
				co_await _handler->async_connect(host, port);
				std::cout << "[RabbitMQ Producer] TCP Socket connected. Waiting for AMQP Handshake..." << std::endl;
			}
			catch (const std::exception& e)
			{
				std::cerr << "[RabbitMQ] Async connect exception: " << e.what() << std::endl;
				if (!promiseResolved->exchange(true)) {
					initPromise->set_value(false);
				}
			}
		}, boost::asio::detached);

		// 阻塞等待完整流程结束 (TCP连接 -> AMQP握手 -> Channel建立 -> Exchange声明)
		if (initFuture.wait_for(std::chrono::seconds(5)) == std::future_status::ready)
		{
			return initFuture.get();
		}
		std::cerr << "[RabbitMQ] Init timeout! Handshake failed." << std::endl;
		return false;
	}
	catch (const std::exception& e)
	{
		std::cerr << "[RabbitMQ] Init exception: " << e.what() << std::endl;
		return false;
	}
}

void RabbitMQClient::publish(const std::string& routingKey, const std::string& message)
{
	boost::asio::post(_ioc, [this, routingKey, message]
	{
		doPublish(routingKey, message);
	});
}

void RabbitMQClient::stop()
{
	if (_worker)
	{
		_worker->reset();
	}
	_ioc.stop();
}

RabbitMQClient::~RabbitMQClient()
{
	stop();
}

void RabbitMQClient::doPublish(const std::string& routingKey, const std::string& message)
{
	// std::cout << "[RabbitMQ] Publishing message: " << message << std::endl;
	if (!_channel || !_channel->usable())
	{
		std::cerr << "[RabbitMQ] Channel is not usable, drop message!" << std::endl;
		return;
	}

	// 新增：极限背压防线！
	// 如果内存中等待 RabbitMQ 确认的消息超过 50,000 条，说明 MQ 或 DB 已经瘫痪
	// 坚决拒绝继续写入内存，保护 ChatServer 主进程不崩！
	if (_inflightMsgs.size() > 50000) {
		std::cerr << "[Backpressure] System overloaded! Dropping message." << std::endl;
		// TODO: 可选 -> 抛出一个信号，让 LogicSystem 给客户端回复一个 {"error": "Server Busy"} 的包
		return;
	}

	uint64_t tag = _deliveryTagCount++;
	_inflightMsgs[tag] = {routingKey, message};

	AMQP::Envelope env(message);
	env.setDeliveryMode(2);
	_channel->publish(_exchangeName, routingKey, env);
}
