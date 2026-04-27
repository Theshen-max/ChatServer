//
// Created by 27044 on 26-3-19.
//

#include "../headers/CSession.h"
#include "../headers/CServer.h"
#include "../headers/LogicSystem.h"
#include "../headers/RedisMgr.h"

#include "boost/endian/conversion.hpp"

CSession::CSession(net::io_context& ioc, ssl::context& ssl_ctx, std::shared_ptr<CServer> server) :
	_socket(ioc, ssl_ctx),
	_server(std::move(server)),
	_lastActiveTime(std::chrono::duration_cast<std::chrono::seconds>
		(std::chrono::system_clock::now().time_since_epoch()).count()),
	_lastRedisUpdate(std::chrono::steady_clock::now())
{
	boost::uuids::uuid uuidSession = _generator();
	_uuid = boost::uuids::to_string(uuidSession);
	updateActiveTime();
}

CSession::~CSession()
{
	// 这里千万不能调用close(),因为close里面会self = shared_from_this，而此时已经进入析构了，不能再shared_from_this
	std::cout << "CSession is destroyed" << std::endl;
}

ssl::stream<tcp::socket>& CSession::getSocket()
{
	return _socket;
}

boost::asio::streambuf& CSession::getStreambuf()
{
	return _streambuf;
}

std::shared_ptr<CServer>& CSession::getServer()
{
	return _server;
}

const std::string& CSession::getUUid()
{
	return _uuid;
}

const std::string& CSession::getUserInfoUid()
{
	// 后期调用getUserInfoUid时，都是Login阶段已经完成插入的时期
	// assert(!_userInfoUid.empty());
	return _userInfoUid;
}

void CSession::setUserInfoUid(const std::string& uid)
{
	if (_userInfoUid != uid)
		_userInfoUid = uid;
}

void CSession::start()
{
	auto self = shared_from_this();
	_socket.async_handshake(ssl::stream_base::server, [self](boost::system::error_code ec)
	{
		if (ec)
		{
			// 过滤掉对端主动关闭引发的“良性”错误
			if (ec == boost::asio::error::eof ||
				ec == boost::asio::error::connection_reset ||
				ec == boost::asio::ssl::error::stream_truncated)
			{
				// 优雅处理：可以选择只打印一条普通的 Info 日志，或者干脆什么都不打印
				std::cout << "Client dropped connection during SSL handshake (Ignored)." << std::endl;
			}
			else
			{
				// 真正的 SSL 协议报错（如证书不受信任、加密套件不匹配等）
				std::cerr << "SSL handshake failed: " << ec.message() << " (" << ec.value() << ")" << std::endl;
			}
			self->close();
			self->getServer()->cleanSession(self->getUUid());
			return;
		}
		// ssl,tls握手成功，开始解包
		self->asyncReadHead();
	});
}

void CSession::asyncReadHead()
{
	auto self = shared_from_this();
	boost::asio::async_read(_socket, _streambuf, boost::asio::transfer_exactly(HEAD_TOTAL_LEN),
		[self](boost::system::error_code ec, std::size_t bytes_transferred)
		{
			try
			{
				if (ec)
				{
					if (ec != boost::asio::error::operation_aborted)
						std::cerr << "asyncReadHead failed: " << ec.message() << std::endl;
					self->close();
					self->getServer()->cleanSession(self->getUUid());
					return;
				}
				// 刷新活跃时间（心跳）
				self->updateActiveTime();
				// 原地解析
				auto* dataPtr = static_cast<const unsigned char*>(self->getStreambuf().data().data());
				std::shared_ptr<MsgNode> msg = std::make_shared<MsgNode>();
				msg->_id = boost::endian::load_little_u16(dataPtr);
				msg->_len = boost::endian::load_little_u16(dataPtr + sizeof(uint16_t));
				msg->_type = MsgType::RecvMsg;
				// 消费掉streambuf头
				self->getStreambuf().consume(HEAD_TOTAL_LEN);
				// 开始读取内容体
				self->asyncReadBody(msg->_len, msg);
			}
			catch (const std::exception& e )
			{
				std::cerr << e.what() << std::endl;
				return;
			}
		});
}

void CSession::asyncReadBody(int len, std::shared_ptr<MsgNode> msg)
{
	auto self = shared_from_this();
	boost::asio::async_read(_socket, _streambuf, boost::asio::transfer_exactly(len),
		[self, msg](boost::system::error_code ec, std::size_t bytes_transferred)
		{
			try
			{
				if (ec)
				{
					std::cerr << "asyncReadBody failed: " << ec.message() << std::endl;
					self->close();
					self->getServer()->cleanSession(self->getUUid());
					return;
				}
				// 原地解析
				auto* data = static_cast<const unsigned char*>(self->getStreambuf().data().data());
				msg->_data = std::string(data, data + bytes_transferred);
				// 消费掉streambuf头
				self->getStreambuf().consume(bytes_transferred);
				// std::cout << "RecvID: " << msg->_id << " receive data is " << msg->_data << std::endl;
				// 此处读取完一个MsgNode, 将其投递到逻辑队列处理
				LogicSystem::getInstance()->postMsgToQueue(std::make_shared<LogicNode>(self, msg));
				// 继续监听头部接收事件
				self->asyncReadHead();
			}
			catch (const std::exception& e)
			{
				std::cerr << e.what() << std::endl;
				return;
			}
		});
}

void CSession::postSend(std::shared_ptr<MsgNode> msg)
{
	auto self = shared_from_this();
	boost::asio::post(_socket.get_executor(), [self, msg]
	{
		// 以下代码运行在专属的 I/O 线程中, 对应ioc的事件循环里(因为我们的ioc都是一个线程单独跑，此时已经是单线程环境)
		self->_sendQueue.emplace(msg);

		// 下面是tsung测试（队列积压）
		if (self->_sendQueue.size() > 50)
			std::cout << "警告：发送队列积压!" << std::endl;

		// 如果当前没有正在进行的 async_write，则触发发送；
		// 如果正在发送，新消息待在队列里，等上一个发完了自动会发它。(因为async_write是异步操作，只有执行回调时才算真正完成发送)
		if (!self->_isWriting)
		{
			self->asyncWrite();
		}
	});
}

void CSession::asyncWrite()
{
	auto self = shared_from_this();

	// 队列为空，直接返回
	if (_sendQueue.empty())
	{
		_isWriting = false;
		return;
	}

	assert(!_sendQueue.empty());
	_isWriting = true;

	auto msg = _sendQueue.front();
	auto sendNode = std::make_shared<SendNode>(msg);
	boost::asio::async_write(_socket, sendNode->getBuffer(), [self, sendNode](const boost::system::error_code& ec,
	  std::size_t)
	{
		if (ec)
		{
			std::cerr << "asyncWrite failed: " << ec.message() << std::endl;
			self->close();
			self->getServer()->cleanSession(self->getUUid());
			return;
		}

		self->_sendQueue.pop(); // 移除头部已被移动的msg
		self->asyncWrite(); // 继续异步调用写操作，直到队列为空自动返回false
	});
}

void CSession::close()
{
	bool expect = false;
	if (!_stop.compare_exchange_strong(expect, true))
		return;

	auto self = shared_from_this();

	boost::asio::dispatch(_socket.get_executor(), [this, self]
	{
		if (!_userInfoUid.empty()) // 说明缓存中有该CSession
		{
			LogicSystem::getInstance()->onSessionDisconnected(_userInfoUid, _socket.get_executor(), shared_from_this());
		}

		_socket.async_shutdown([self](boost::system::error_code ec)
		{
			// 只要这个回调触发了，说明SSL告别已经做完了， 无论成功与否都要清理数据
			boost::system::error_code error;
			// 此时再关闭底层的 TCP 读写通道
			self->_socket.lowest_layer().shutdown(boost::asio::socket_base::shutdown_both, error);
			// 彻底释放文件描述符
			self->_socket.lowest_layer().close(error);

			if (error) {
				std::cerr << "Socket close error: " << error.message() << std::endl;
			}
		});
	});
}

int64_t CSession::getLastActiveTime() const
{
	return _lastActiveTime.load(std::memory_order_relaxed);
}

void CSession::updateActiveTime()
{
	// steady_clock
	auto now = std::chrono::steady_clock::now();

	_lastActiveTime.store(std::chrono::duration_cast<std::chrono::seconds>(
		std::chrono::system_clock::now().time_since_epoch()).count(), std::memory_order_relaxed);

	auto duration_since_redis_update = std::chrono::duration_cast<std::chrono::seconds>(now - _lastRedisUpdate).count();

	if (!_userInfoUid.empty() && duration_since_redis_update > 60) {
		// 续期 300 秒
		RedisMgr::getServerConfigRedis().expire(_userInfoUid, 300);
		_lastRedisUpdate = now;
	}
}

