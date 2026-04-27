#include "../headers/const.h"
#include "../headers/ConfigMgr.h"
#include "../headers/CServer.h"
#include "../headers/RouteGrpcClient.h"
#include "../headers/RabbitMQClient.h"
#include "../headers/MysqlMgr.h"
#include "../headers/RedisMgr.h"
#include "../headers/LogicSystem.h"

std::string serverName;

int main()
{
	try
	{
		// 强制 gRPC 忽略本地回环的代理
		// _putenv("no_proxy=127.0.0.1,localhost");

		auto& cfg = ConfigMgr::getInstance();
		// 配置ChatServer
		boost::asio::io_context ioc{1};
		auto work = boost::asio::make_work_guard(ioc);
		boost::asio::signal_set signals(ioc, SIGINT, SIGTERM);
		signals.async_wait([&ioc, &work](boost::system::error_code ec, int number)
		{
			if (ec) return;
			std::cout << "shutting down server" << std::endl;
			work.reset();	// 取消绑定，恢复io_context停止后可以退出run
			ioc.stop();	// 停止注册新的ioc, 并退出run
		});

		// ini配置文件获取端口
		unsigned short port;
		std::string nameStr = cfg["SelfServer"]["Name"];
		std::string portStr = cfg["SelfServer"]["Port"];
		std::from_chars(portStr.data(), portStr.data() + portStr.size(), port);
		serverName = nameStr;

		// 启动RouteGrpcClient
		RouteGrpcClient::getInstance()->init();

		// 启动RabbitMQClient
		std::cout << "Initializing RabbitMQ..." << std::endl;
		const std::string& MQHost = cfg["RabbitMQ"]["Host"];
		const std::string& MQPort = cfg["RabbitMQ"]["Port"];
		const std::string& MQUser = cfg["RabbitMQ"]["User"];
		const std::string& MQPassword = cfg["RabbitMQ"]["Passwd"];
		if (!RabbitMQClient::getInstance()->init(MQHost, MQPort, MQUser, MQPassword, "chat_exchange"))
		{
			std::cerr << "RabbitMQ init failed! Server cannot start." << std::endl;
			return EXIT_FAILURE;
		}

		// 启动逻辑子系统 (加载线程池)
		LogicSystem::getInstance();

		// 预热 Redis 和 MySQL 连接池
		std::cout << "\nInitializing Redis & MySQL..." << std::endl;
		RedisMgr::getUserInfoRedis();
		RedisMgr::getMsgInfoRedis();
		RedisMgr::getConvInfoRedis();
		RedisMgr::getUserListRedis();
		MysqlMgr::getInstance();

		// 通过智能指针创建CServer实例
		std::make_shared<CServer>(ioc, port)->startAccept();
		std::cout << nameStr << " listen on port: " << port << std::endl;
		ioc.run();
	}
	catch (const std::exception& e)
	{
		std::cerr << "Exception: " << e.what() << std::endl;
		return EXIT_FAILURE;
	}
	return EXIT_SUCCESS;
}
