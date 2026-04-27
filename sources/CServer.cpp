#include "../headers/AsioIOContextPool.h"
#include "../headers/CServer.h"
#include "../headers/ConfigMgr.h"
#include "../headers/RedisMgr.h"

CServer::CServer(net::io_context& ctx, unsigned short port) :
	_ioc(ctx),
	_acceptor(ctx, tcp::endpoint(tcp::v4(), port)),  // tcp::endpoint(终端，端点)
	_sslContext(ssl::context::tlsv12_server),
	_heartbeatTimer(_ioc),
	_reportTimer(_ioc)
{
	initSSL();
	startHeartbeatCheck();
	startServerReport();
	std::cout << "\nServer starting accept ..." << std::endl;
	// 这里不能直接startAccept();， 因为其内部有shared_from_this,而这个函数不能在构造期间使用
}

void CServer::initSSL()
{
	_sslContext.use_certificate_chain_file("server.crt");	// 证书
	_sslContext.use_private_key_file("server.key", ssl::context::pem);	// 私钥
}

// 客户端心跳检测，避免半开连接幽灵
void CServer::startHeartbeatCheck()
{
	_heartbeatTimer.expires_after(boost::asio::chrono::seconds(15));
	_heartbeatTimer.async_wait([this](const boost::system::error_code& ec)
	{
		if (ec) return;
		int64_t now = std::chrono::duration_cast<std::chrono::seconds>(
			std::chrono::system_clock::now().time_since_epoch()).count();

		std::scoped_lock<std::mutex> lock(_mutex);
		for (auto it = _sessions.begin(); it != _sessions.end();)
		{
			auto session = it->second;
			// 服务器端15s一次检测，允许连丢2个包，45秒判定为物理断线幽灵
			if (now - session->getLastActiveTime() > 45)
			{
				std::cout << "[Heartbeat] Timeout! Kicking out UID: " << session->getUserInfoUid() << std::endl;
				session->close();
				it = _sessions.erase(it);
			}
			else
				++it;
		}
		startHeartbeatCheck();
	});
}

void CServer::startServerReport()
{
	_reportTimer.expires_after(boost::asio::chrono::seconds(3));
	_reportTimer.async_wait([this](const boost::system::error_code& ec)
	{
		if (ec) return;

		// 获取当前最真实的物理连接数
		int sessionCount = 0;
		{
			std::scoped_lock<std::mutex> lock(_mutex);
			sessionCount = _sessions.size();
		}

		std::cout << "\r[Monitor] Current Active Sessions: " << sessionCount << "    " << std::flush;

		auto& cfg = ConfigMgr::getInstance();
		std::string serverName = cfg["SelfServer"]["Name"];
		std::string serverHost = "127.0.0.1";
		std::string serverPort = cfg["SelfServer"]["Port"];

		// 开启一个轻量级的分离线程去执行 Redis 网络 I/O，不阻塞 CServer 的 Acceptor 循环
		std::thread([serverName, serverHost, serverPort, sessionCount]
		{
			try
			{
				auto& redis = RedisMgr::getServerConfigRedis();
				auto pipe = redis.pipeline();
				pipe.zadd("chat_servers", serverName, sessionCount);
				pipe.set("ServerAlive:" + serverName, "1", std::chrono::seconds(7));
				pipe.hset("ServerData:" + serverName, "serverIp", serverHost);
				pipe.hset("ServerData:" + serverName, "serverPort", serverPort);
				pipe.exec();
			}
			catch (const std::exception& e)
			{
				std::cerr << "[CServer] 向 Redis 上报心跳失败: " << e.what() << std::endl;
			}
		}).detach();

		// 循环调用，形成心跳永动机
		startServerReport();
	});
}

void CServer::startAccept()
{
	std::cout << "Waiting New CSession ..." << std::endl;
	// 这里必须拷贝捕获self, 通过std::shared_ptr的引用计数来维持生命周期
	auto self = shared_from_this();
	// 获取IO上下文，并创建会话，供服务器与客户端就行TCP连接
	boost::asio::io_context& ioc = AsioIoContextPool::getInstance()->getNextIoContext();
	std::shared_ptr<CSession> newSession = std::make_shared<CSession>(ioc, _sslContext, self);
	_acceptor.async_accept(newSession->getSocket().lowest_layer(), [self, newSession](boost::system::error_code ec)
	{
		try
		{
			// 出错退出，不再监听
			if (ec)
			{
				if (ec == boost::asio::error::operation_aborted)
				{
					std::cout << "Acceptor closed, stopping accept loop." << std::endl;
					return;
				}
				std::cerr << "Accept error: " << ec.message() << std::endl;
				self->startAccept();
			}
			// 未出错
			self->handleAccept(newSession);
			// CServer继续监听新连接
			self->startAccept();
		}
		catch (const std::exception& e)
		{
			std::cerr << "Accept Error: " << e.what() << std::endl;
		}
	});
}

void CServer::handleAccept(std::shared_ptr<CSession> session)
{
	session->start();
	{
		// 虽然handleAccept是监听线程一直在运行，但是_sessions的修改可能同时被监听线程（emplace），与工作线程（erase）同时修改
		// 必须假设保证不与session的线程竞争数据
		std::scoped_lock<std::mutex> lock(_mutex);
		_sessions.emplace(session->getUUid(), session);
	}
}

void CServer::cleanSession(const std::string& uuid)
{
	std::scoped_lock lock(_mutex);
	auto it = _sessions.find(uuid);
	if (it != _sessions.end())
		_sessions.erase(it);
}
