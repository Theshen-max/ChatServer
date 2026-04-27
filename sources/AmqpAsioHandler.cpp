#include "../headers/AmqpAsioHandler.h"

AmqpAsioHandler::AmqpAsioHandler(boost::asio::io_context& ioc):
	_socket(ioc)
{

}

AmqpAsioHandler::~AmqpAsioHandler()
{
	if (_socket.is_open())
	{
		boost::system::error_code ec;
		_socket.shutdown(boost::asio::socket_base::shutdown_both, ec);
		_socket.close(ec);
		if (ec)
			std::cerr << "Socket close error: " << ec.message() << std::endl;
	}
}

boost::asio::awaitable<void> AmqpAsioHandler::async_connect(const std::string& host, const std::string& port)
{
	try
	{
		auto executor = co_await boost::asio::this_coro::executor;
		boost::asio::ip::tcp::resolver resolver(executor);

		// 异步解析域名/IP
		auto endpoints = co_await resolver.async_resolve(
			host, port,use_awaitable);

		// 异步连接
		co_await boost::asio::async_connect(_socket, endpoints, use_awaitable);

		_connected = true;
		std::cout << "[AmqpAsioHandler] Successfully connected to RabbitMQ TCP socket." << std::endl;

		// 例如_connection = std::make_unique<AMQP::Connection>的创建就会调用onData发送请求头，但此时_connected = false就会暂存到queue里面
		if (!_writeQueue.empty() && !_isWriting) {
			doWrite();
		}

		boost::asio::co_spawn(executor, readLoop(), boost::asio::detached);
	}
	catch (const std::exception& e)
	{
		std::cerr << "[AmqpAsioHandler] Connection Failed: " << e.what() << std::endl;
		if (_connection)
			onError(_connection, e.what());
	}
}

void AmqpAsioHandler::setConnection(AMQP::Connection* connection)
{
	_connection = connection;
}

boost::asio::ip::tcp::socket& AmqpAsioHandler::socket()
{
	return _socket;
}

boost::asio::awaitable<void> AmqpAsioHandler::readLoop()
{
	try
	{
		char buffer[4096];
		while (_connected)
		{
			auto[ec, bytes_transferred] = co_await _socket.async_read_some(boost::asio::buffer(buffer), use_nothrow_awaitable);
			if (ec) {
				std::cerr << "[AmqpAsioHandler] Socket read error / EOF: " << ec.message() << std::endl;
				_connected = false;
				if (_connection)
				{
					if (ec == boost::asio::error::eof ||
						ec == boost::asio::error::connection_reset)
					{
						onClosed(_connection);
					}
					else
					{
						onError(_connection, ec.message().c_str());
					}
				}

				co_return; // 跳出死循环
				//TODO:实现断线重连状态机，网络恢复后_inflightMsgs 里积压的所有消息，重新循环发送一遍（重赋新的 Tag）
			}
			if (_connection && bytes_transferred > 0)
			{
				_connection->parse(buffer, bytes_transferred);
			}
		}
		co_return;
	}
	catch (const std::exception& e) {
		std::cerr << "[AmqpAsioHandler] Read Loop Error/Disconnected: " << e.what() << std::endl;
		_connected = false;
		if (_connection)
			onClosed(_connection);
	}
}

void AmqpAsioHandler::onData(AMQP::Connection* connection, const char* buffer, size_t size)
{
	_writeQueue.emplace(buffer, buffer + size);
	if (_connected && !_isWriting)
	{
		doWrite();
	}
}

void AmqpAsioHandler::doWrite()
{
	if (_writeQueue.empty())
	{
		_isWriting = false;
		return;
	}
	_isWriting = true;
	auto self = shared_from_this();

	boost::asio::async_write(_socket, boost::asio::buffer(_writeQueue.front()),
	[this, self](const boost::system::error_code& ec, size_t bytes_transferred)
	{
		if (!ec)
		{
			_writeQueue.pop();
			doWrite();
		}
		else
		{
			std::cerr << "[AmqpAsioHandler] Write Error: " << ec.message() << std::endl;
			_connected = false;
			if (_connection)
				onError(_connection, ec.message().c_str());
		}
	});
}


void AmqpAsioHandler::onReady(AMQP::Connection* connection)
{
	std::cout << "[AMQP Protocol] AMQP Connection is READY!" << std::endl;
}

void AmqpAsioHandler::onClosed(AMQP::Connection* connection)
{
	std::cout << "[AMQP Protocol] Connection CLOSED." << std::endl;
}

void AmqpAsioHandler::onError(AMQP::Connection* connection, const char* message)
{
	std::cerr << "[AMQP Protocol] ERROR: " << message << std::endl;
}
