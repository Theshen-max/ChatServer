#ifndef AMQPASIOHANDLER_H
#define AMQPASIOHANDLER_H

#include "const.h"
#include <amqpcpp.h>
class AmqpAsioHandler: public AMQP::ConnectionHandler, public std::enable_shared_from_this<AmqpAsioHandler>
{
public:
	using ReadyCallback = std::function<void()>;
	using ErrorCallback = std::function<void(const char*)>;

	AmqpAsioHandler(boost::asio::io_context& ioc);

	~AmqpAsioHandler() override;

	boost::asio::awaitable<void> async_connect(const std::string& host, const std::string& port);

	// 绑定 AMQP-CPP 的核心 Connection 对象
	void setConnection(AMQP::Connection* connection);

	boost::asio::ip::tcp::socket& socket();

	void setReadyCallback(ReadyCallback cb);

	void setErrorCallback(ErrorCallback cb);

private:
	void onData(AMQP::Connection* connection, const char* buffer, size_t size) override;

	void onReady(AMQP::Connection* connection) override;

	void onClosed(AMQP::Connection* connection) override;

	void onError(AMQP::Connection* connection, const char* message) override;

	boost::asio::awaitable<void> readLoop();

	void doWrite();

private:
	boost::asio::ip::tcp::socket _socket;

	AMQP::Connection* _connection{};

	std::queue<std::vector<char>> _writeQueue;

	bool _isWriting{false};

	bool _connected{false};

	ReadyCallback _readyCallback;

	ErrorCallback _errorCallback;

};

#endif //AMQPASIOHANDLER_H
