#ifndef CSESSION_H
#define CSESSION_H

#include "const.h"
#include "MsgNode.h"
class CServer;
class MsgNode;
class CSession: public std::enable_shared_from_this<CSession>
{
public:
	CSession(boost::asio::io_context& ioc, ssl::context& ctx, std::shared_ptr<CServer> server);

	~CSession();

	ssl::stream<tcp::socket>& getSocket();

	const std::string& getUUid();

	const std::string& getUserInfoUid();

	std::shared_ptr<CServer>& getServer();

	void setUserInfoUid(const std::string& uid);

	void start();

	int64_t getLastActiveTime() const;

	// 提供给 LogicSystem 调用的线程安全投递接口
	// LogicSystem 会在别的线程调用这个函数！
	void postSend(std::shared_ptr<MsgNode> msg); // 这个函数是别的线程调用，在多线程环境下

	void close();

private:
	boost::asio::streambuf& getStreambuf();

	void asyncReadHead();

	void asyncReadBody(int len, std::shared_ptr<MsgNode> msg);

	// 真正的异步发送逻辑（只在 I/O 线程内执行）
	void asyncWrite();

	void updateActiveTime();
private:
	ssl::stream<tcp::socket> _socket;

	std::string _uuid;

	std::string _userInfoUid;

	std::atomic<bool> _stop{false};

	std::shared_ptr<CServer> _server;

	boost::asio::streambuf _streambuf{8 * 1024 * 1024};

	std::queue<std::shared_ptr<MsgNode>> _sendQueue;	// Session 专属发送队列(消息队列)

	bool _isWriting = false;                         // 是否正在发送的标志

	inline static thread_local boost::uuids::random_generator _generator{};

	std::atomic<int64_t> _lastActiveTime;

	std::chrono::steady_clock::time_point _lastRedisUpdate;
};

#endif //CSESSION_H
