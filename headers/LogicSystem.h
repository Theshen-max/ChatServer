#ifndef LOGINSYSTEM_H
#define LOGINSYSTEM_H
#include "const.h"
#include "CServer.h"
#include "RapidJsonMgr.h"
#include "Snowflake.h"
// 一次传来的Tcp包头部（本质是数据流）
#define HEAD_TOTAL_LEN 4 // 总长度 （ReqId + ContentLength）
#define HEAD_ID_LEN 2 // ReqID: unsigned short
#define HEAD_DATA_LEN 2	 // ContentLength: unsigned short

class LogicNode;
class MsgNode;
class CSession;

class LogicSystem: public Singleton<LogicSystem>
{
	friend class Singleton<LogicSystem>;

public:
	void postMsgToQueue(std::shared_ptr<LogicNode> logicNode);

	void stop();

	~LogicSystem();

	void onSessionDisconnected(const std::string& uid, const boost::asio::any_io_executor& ex, std::weak_ptr<CSession> oldSession);

	void executeRealOffline(const std::string& uid);

	void chatToTargetUser(const std::string& targetUid, const std::string& msg, int msgId);

	void deliverMsgToLocalUser(const std::string& targetUid, const std::string& msgData, int msgId);

private:
	LogicSystem();

	void registerHandlers();

	std::shared_ptr<UserInfo> getUserProfile(const std::string& uid);

	// 共享锁供于后面多线程共同读写_users场景的线程安全
	std::shared_mutex _shared_mutex;

	boost::asio::thread_pool _threadPool; // 逻辑线程池

	boost::asio::thread_pool _dbPool; // 数据库线程池

	std::vector<std::shared_ptr<boost::asio::strand<boost::asio::thread_pool::executor_type>>> _strands;

	std::atomic<bool> _stop;

	std::unordered_map<uint16_t, std::function<void(std::shared_ptr<LogicNode>)>> _handlers;

	// 本地存储的是基本资料数据，不包括token，缓存是避免龟速MySql查询
	// key是uid
	std::unordered_map<std::string, std::shared_ptr<UserConnectionCtx>> _users;

	std::shared_ptr<Snowflake> _snowflake;

	// 计数器与时间戳，用于高频请求检测
	int _repairCount = 0;
	std::chrono::steady_clock::time_point _lastRepairCheckTime;
	std::mutex _repairMonitorMutex;
};

class LogicNode
{
	friend class LogicSystem;
public:
	LogicNode(std::weak_ptr<CSession> curSession, std::shared_ptr<MsgNode> curNode);

private:
	std::weak_ptr<CSession> _curSession;

	std::shared_ptr<MsgNode> _curMsgNode;
};

#endif //LOGINSYSTEM_H
