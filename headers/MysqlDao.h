#ifndef MYSQLDAO_H
#define MYSQLDAO_H

#include "const.h"
#include <thread>
#include <jdbc/mysql_driver.h>
#include <jdbc/mysql_connection.h>
#include <jdbc/cppconn/prepared_statement.h>
#include <jdbc/cppconn/statement.h>
#include <jdbc/cppconn/exception.h>
#include <jdbc/cppconn/resultset.h>

// 封装的Mysql连接
class SqlConnection
{
public:
	explicit SqlConnection(sql::Connection* con, int64_t lastTime);
	int64_t _lastTime;
	std::unique_ptr<sql::Connection> _con;	// 用std::unique_ptr让其自动回收

	~SqlConnection();
};

class MysqlPool
{
public:
	explicit MysqlPool(
		std::string url,
		std::string user,
		std::string password,
		std::string schema,
		int poolSize,
		bool stop = false
	);

	void checkConnection();

	std::unique_ptr<SqlConnection> getConnection();

	void returnConnection(std::unique_ptr<SqlConnection> connection);

	void close();

	~MysqlPool();

private:
	// 数据
	std::string _url;
	std::string _user;
	std::string _password;
	std::string _schema;
	int _poolSize;
	bool _stop;
	// 实现有锁队列
	std::queue<std::unique_ptr<SqlConnection>> _queue;
	std::mutex _mutex;
	std::condition_variable _cond;
	std::jthread _checkThread;	// 检测线程，判断连接有没有超过规定时间未通信，有则通知Mysql服务器，不要删除，心跳机制
};

/// 当前生成的uid是uuid_short()函数生成的，只支持只有一个服务器ID时的状况，推荐使用snowflake算法（待设计）
class MysqlDao
{
public:
	MysqlDao();

	~MysqlDao();

	std::optional<uint64_t> registerUser(const std::string& user, const std::string& password, const std::string& email);

	bool checkEmail(const std::string& user, const std::string& email);

	bool checkPwd(const std::string& user, const std::string& password, UserInfo& userInfo);

	bool updatePassword(const std::string& email, const std::string& password);

	std::shared_ptr<UserInfo> getUserInfo(const std::string& uid);

	std::unordered_map<std::string, std::shared_ptr<UserInfo>> searchUserInfoList(const std::string& username, const std::string& email);

	bool upsertFriendApply(const std::string& fromUid, const std::string& toUid, int status,
		const std::string& greeting, const std::string& applyTime, const std::string& handleTime, uint64_t version);

	bool insertChatMessage(const std::string& senderUid, const std::string& targetUid, const std::string& msgData, const std::string& seq);

	std::vector<FriendInfo> getFullFriends(const std::string& myUid);

	std::vector<ConversationInfo> getAllConversations(const std::string& myUid);

	std::vector<MsgInfo> getSyncMessages(const std::string& senderUid, const std::string& targetUid, int sessionType,
		const std::string& startVersion, const std::string& endVersion, int limit);

	std::shared_ptr<FriendApplyInfo> getFriendApply(const std::string& fromUid, const std::string& toUid);

	bool updateAllMsgReadStatus(const std::string& senderUid, const std::string& targetUid, int sessionType);

	bool clearConvUnreadCount(const std::string& ownerUid, const std::string& peerUid, int sessionType);

	bool callUpsertFriendApplyProc(const std::string& fromUid, const std::string& toUid, int status,
	const std::string& greeting, const std::string& applyTime, const std::string& handleTime, uint64_t& version);

	bool callAuthFriendApplyProc(const std::string& fromUid, const std::string& toUid, int newStatus,
		const std::string& handleTime, uint64_t& outVersion);

	bool callAddFriendProc(const std::string& selfUid, const std::string& friendUid, int status,
	const std::string& createTime, uint64_t& newFriendVersion);

	std::vector<FriendApplyInfo> getSyncFriendApplies(const std::string& uid, uint64_t version);

	std::vector<FriendInfo> getSyncFriends(const std::string& uid, uint64_t version);

	std::vector<ConversationInfo> getSyncConversations(const std::string& uid, uint64_t version);

	std::vector<MsgInfo> getRepairMessages(const std::string& peerUid, const std::string& myUid, int sessionType,
		uint64_t fromSeq, uint64_t toSeq);

private:
	std::unique_ptr<MysqlPool> _pool;
};

#endif //MYSQLDAO_H
