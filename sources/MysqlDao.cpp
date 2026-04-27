
#include "../headers/MysqlDao.h"
#include "../headers/ConfigMgr.h"
#include "../headers/HashPwd.h"
// SqlConnection的实现定义
SqlConnection::SqlConnection(sql::Connection* con, int64_t lastTime) : _con(con), _lastTime(lastTime)
{

}

SqlConnection::~SqlConnection()
{
	if (_con)
		_con->close(); // 这里必须确保close()不会抛出异常
}

// MysqlPool的实现定义
MysqlPool::MysqlPool(std::string url, std::string user, std::string password,
	std::string schema, int poolSize, bool stop) :
	_url(std::move(url)),
	_user(std::move(user)),
	_password(std::move(password)),
	_schema(std::move(schema)),
	_poolSize(poolSize),
	_stop(stop)
{
	try
	{
		// 创建池内连接
		sql::mysql::MySQL_Driver* driver = sql::mysql::get_mysql_driver_instance();
		for (int i = 0; i < _poolSize; i++)
		{
			auto* connection = driver->connect(_url, _user, _password);
			connection->setSchema(_schema);
			// 获取当前时间戳
			auto currentTime = std::chrono::steady_clock::now().time_since_epoch();
			// 将时间戳转换为秒
			int64_t timestamp = std::chrono::duration_cast<std::chrono::seconds>(currentTime).count();
			_queue.emplace(std::make_unique<SqlConnection>(connection, timestamp));
		}

		// 创建检查线程
		_checkThread = std::jthread([this]()
		{
			while (!_stop)
			{
				// 每60s检查一次
				checkConnection();
				std::this_thread::sleep_for(std::chrono::seconds(60));
			}
		});
	}
	catch (sql::SQLException& e)
	{
		std::cerr << "Fatal Error: MySQL pool init failed! Error: " << e.what() << std::endl;
		throw std::runtime_error("Failed to initialize MySQL Connection Pool.");
	}
	catch (const std::exception& e)
	{
		std::cerr << "MysqlDao Error, " << e.what() << std::endl;
		throw std::runtime_error("Failed to initialize MySQL Connection Pool.");
	}
}

void MysqlPool::checkConnection()
{
	// 获取当前时间戳
	auto currentTime = std::chrono::steady_clock::now().time_since_epoch();
	// 时间戳转化为秒
	int64_t timestamp = std::chrono::duration_cast<std::chrono::seconds>(currentTime).count();
	{
		// 加锁处理
		std::scoped_lock<std::mutex> lock(_mutex);
		for (int i = 0; i < _queue.size(); ++i)	// 注意这里必须是当前空闲队列的实时大小，因为可能当前队列大小不是固定的
		{
			// 取出连接
			auto connection = std::move(_queue.front());
			_queue.pop();

			// 设置延期函数
			// connection是std::unique_ptr，不可拷贝，而且后期还要使用，只能引用
			Defer defer([this, &connection]
			{
				_queue.push(std::move(connection));
			});

			if (timestamp - connection->_lastTime < 5) continue;

			try
			{
				std::unique_ptr<sql::Statement> stmt(connection->_con->createStatement());
				stmt->executeQuery("select 1");
				connection->_lastTime = timestamp;
				// std::cout << "execute timer alive query, cur is " << timestamp << std::endl;
			}
			catch (sql::SQLException& e)
			{
				std::cout << "Error keeping connection alive: " << e.what() << std::endl;
				// 创建新连接替换旧连接
				sql::mysql::MySQL_Driver* driver = sql::mysql::get_mysql_driver_instance();
				auto* newConnection = driver->connect(_url, _user, _password);
				newConnection->setSchema(_schema);
				connection->_con.reset(newConnection);
				connection->_lastTime = timestamp;
			}
		}
	}
}

std::unique_ptr<SqlConnection> MysqlPool::getConnection()
{
	std::unique_lock<std::mutex> lock(_mutex);
	_cond.wait(lock, [this]
	{
		if (_stop) return true;
		return !_queue.empty();
	});
	if (_stop) return nullptr;
	std::unique_ptr<SqlConnection> connection(std::move(_queue.front()));
	_queue.pop();
	return connection;
}

void MysqlPool::returnConnection(std::unique_ptr<SqlConnection> connection)
{
	{
		std::scoped_lock<std::mutex> lock(_mutex);
		if (_stop) return;
		_queue.push(std::move(connection));
	}
	_cond.notify_one();
}

void MysqlPool::close()	// close()只代表关闭池，不代表把池的资源释放，析构函数才释放池资源
{
	{
		std::scoped_lock<std::mutex> lock(_mutex);
		_stop = true;
	}
	_cond.notify_all();
}

MysqlPool::~MysqlPool()
{
	std::scoped_lock<std::mutex> lock(_mutex);
	while (!_queue.empty()) _queue.pop();
}

// MysqlDao的实现定义
MysqlDao::MysqlDao()
{
	const std::string& host = ConfigMgr::getInstance()["Mysql"]["Host"];
	const std::string& port = ConfigMgr::getInstance()["Mysql"]["Port"];
	const std::string& user = ConfigMgr::getInstance()["Mysql"]["User"];
	const std::string& password = ConfigMgr::getInstance()["Mysql"]["Passwd"];
	const std::string& schema = ConfigMgr::getInstance()["Mysql"]["Schema"];

	_pool = std::make_unique<MysqlPool>(host + ":" + port, user, password, schema, 4);
}

MysqlDao::~MysqlDao()
{
	_pool->close(); // 必须先关闭池，让其它线程不再拿或放连接，然后成员unique_ptr析构自动调用MysqlPool析构，释放资源
}

std::optional<uint64_t> MysqlDao::registerUser(const std::string& user, const std::string& password, const std::string& email)
{
	auto connection = _pool->getConnection();
	if (!connection) return std::nullopt;
	try
	{
		Defer defer([this, &connection]
		{
			_pool->returnConnection(std::move(connection));
		});

		// 因为generateSalt生成的纯二进制数据，MySQL的VARCHAR(255)默认是UTF-8编码,所以将其转成16进制可打印字符串再传给MySQL
		std::string bndSalt = PasswordCrypto::generateSalt(password.length());
		std::string salt = PasswordCrypto::toHex(reinterpret_cast<unsigned char*>(&bndSalt[0]), bndSalt.length());
		std::string hashPwd = PasswordCrypto::hashPassword(password, salt);
		std::cout << "Username : " << user << std::endl;

		// 调用数据库的存储过程
		std::unique_ptr<sql::PreparedStatement> stmt(connection->_con->prepareStatement("call regUser(?, ?, ?, ?, @result)"));
		// 设置输入参数
		stmt->setString(1, user);
		stmt->setString(2, hashPwd);
		stmt->setString(3, email);
		stmt->setString(4, salt);
		stmt->execute();
		// 查询@result，获取结果集
		std::unique_ptr<sql::Statement> stmtResult(connection->_con->createStatement());
		std::unique_ptr<sql::ResultSet> res(stmtResult->executeQuery("select @result as result"));
		// 获取结果
		if (res->next())
		{
			uint64_t result = res->getUInt64("result");
			std::cout << "Result: " << result << std::endl;	 // result保存的是UID
			return {result};
		}
		// 结果集为空
		return std::nullopt;
	}
	catch (sql::SQLException& e)
	{
		std::cerr << "SQLException: " << e.what() << std::endl;
		std::cerr << "(MYSQL error code: " << e.getErrorCode();
		std::cerr << ", SQLState: " << e.getSQLState() << " )" << std::endl;
		return std::nullopt;
	}
}

bool MysqlDao::checkEmail(const std::string& user, const std::string& email)
{
	auto connection = _pool->getConnection();
	if (!connection) return false;
	try
	{
		Defer defer([this, &connection]
		{
			_pool->returnConnection(std::move(connection));
		});

		// 准备查询语句
		std::unique_ptr<sql::PreparedStatement> pstmt(connection->_con->prepareStatement("select email from user where name = ?"));
		// 绑定参数
		pstmt->setString(1, email);
		// 执行查询
		std::unique_ptr<sql::ResultSet> result(pstmt->executeQuery());
		// 遍历结果集
		if (result->next())
		{
			const std::string checkEmail = result->getString("email");
			std::cout << "Check Email: " << checkEmail << std::endl;
			if (email != checkEmail)
			{
				std::cerr << "Email does not match check email" << std::endl;
				return false;
			}
			return true;
		}
		// 没有结果
		std::cout << "The User is not exists" << std::endl;
		return false;
	}
	catch (sql::SQLException& e)
	{
		std::cerr << "SQLException: " << e.what() << std::endl;
		std::cerr << "(MYSQL error code: " << e.getErrorCode();
		std::cerr << ", SQLState: " << e.getSQLState() << " )" << std::endl;
		return false;
	}
}

bool MysqlDao::checkPwd(const std::string& user, const std::string& password, UserInfo& userInfo)
{
	auto connection = _pool->getConnection();
	if (!connection) return false;
	try
	{
		Defer defer([this, &connection]
		{
			_pool->returnConnection(std::move(connection));
		});

		std::unique_ptr<sql::PreparedStatement> pstmt(connection->_con->prepareStatement("select * from user where name = ?"));
		pstmt->setString(1, user);
		std::unique_ptr<sql::ResultSet> result(pstmt->executeQuery());
		if (result->next())
		{
			uint64_t uid = result->getUInt64("uid");
			std::string pwd = result->getString("password");
			std::string salt = result->getString("salt");
			std::string email = result->getString("email");
			if (PasswordCrypto::hashPassword(password, salt) != pwd)
				return false;

			std::cout << "登录阶段成功获取到数据库用户信息" << std::endl;
			userInfo.uid = uid;
			userInfo.username = user;
			userInfo.email = email;
			return true;
		}
		// 结果集为空
		return false;
	}
	catch (sql::SQLException& e)
	{
		std::cerr << "SQLException: " << e.what() << std::endl;
		std::cerr << "(MYSQL error code: " << e.getErrorCode();
		std::cerr << ", SQLState: " << e.getSQLState() << " )" << std::endl;
		return false;
	}
}

bool MysqlDao::updatePassword(const std::string& email, const std::string& password)
{
	auto connection = _pool->getConnection();
	if (!connection) return false;
	try
	{
		Defer defer([this, &connection]
		{
			_pool->returnConnection(std::move(connection));
		});

		std::string newSalt = PasswordCrypto::generateSalt(password.length());
		std::string hashPwd = PasswordCrypto::hashPassword(password, newSalt);

		// 准备查询语句
		std::unique_ptr<sql::PreparedStatement> pstmt(connection->_con->prepareStatement("update user set password = ?, salt = ? where email = ?"));
		// 绑定数据
		pstmt->setString(1, hashPwd);
		pstmt->setString(2, newSalt);
		pstmt->setString(3, email);
		// 执行并获取结果
		int res = pstmt->executeUpdate();
		std::cout << "Updated rows: " << res << std::endl;
		return true;
	}
	catch (sql::SQLException& e)
	{
		std::cerr << "SQLException: " << e.what() << std::endl;
		std::cerr << "(MYSQL error code: " << e.getErrorCode();
		std::cerr << ", SQLState: " << e.getSQLState() << " )" << std::endl;
		return false;
	}
}

std::shared_ptr<UserInfo> MysqlDao::getUserInfo(const std::string& uid)
{
	auto connection = _pool->getConnection();
	if (!connection) return nullptr;
	try
	{
		Defer defer([this, &connection]
		{
			_pool->returnConnection(std::move(connection));
		});

		// 准备查询语句
		std::unique_ptr<sql::PreparedStatement> pstmt(connection->_con->prepareStatement("select * from user where uid = ?"));
		// 绑定数据
		pstmt->setString(1, uid);
		// 执行并获取结果
		std::unique_ptr<sql::ResultSet> result(pstmt->executeQuery());
		if (result->next())
		{
			std::string sqlUid = std::to_string(result->getUInt64("uid"));
			std::string email = result->getString("email");
			std::string name = result->getString("name");
			std::string version = std::to_string(result->getUInt64("version"));
			std::shared_ptr<UserInfo> userInfo = std::make_shared<UserInfo>();
			userInfo->uid = sqlUid;
			userInfo->username = name;
			userInfo->email = email;
			userInfo->version = version;
			return userInfo;
		}
		return nullptr;
	}
	catch (sql::SQLException& e)
	{
		std::cerr << "SQLException: " << e.what() << std::endl;
		std::cerr << "(MYSQL error code: " << e.getErrorCode();
		std::cerr << ", SQLState: " << e.getSQLState() << " )" << std::endl;
		return nullptr;
	}
}

std::unordered_map<std::string, std::shared_ptr<UserInfo>> MysqlDao::searchUserInfoList(const std::string& username, const std::string& email)
{
	auto connection = _pool->getConnection();
	if (!connection) return {};
	try
	{
		Defer defer([this, &connection]
		{
			_pool->returnConnection(std::move(connection));
		});

		// 准备查询语句
		std::unique_ptr<sql::PreparedStatement> pstmt(connection->_con->prepareStatement(R"(
			(select * from user where name like ?) union (select * from user where email like ?)
		)"));
		// 绑定数据
		pstmt->setString(1, username + "%");
		pstmt->setString(2, email + "%");

		// 获取结果
		std::unordered_map<std::string, std::shared_ptr<UserInfo>> userInfos;
		// 执行
		std::unique_ptr<sql::ResultSet> result(pstmt->executeQuery());
		while (result->next())
		{
			std::shared_ptr<UserInfo> userInfo = std::make_shared<UserInfo>();
			userInfo->uid = std::to_string(result->getUInt64("uid"));
			userInfo->username = result->getString("name");
			userInfo->email = result->getString("email");
			userInfo->version = std::to_string(result->getUInt64("version"));
			userInfos.emplace(userInfo->uid, userInfo);
		}
		return userInfos;
	}
	catch (sql::SQLException& e)
	{
		std::cerr << "SQLException: " << e.what() << std::endl;
		std::cerr << "(MYSQL error code: " << e.getErrorCode();
		std::cerr << ", SQLState: " << e.getSQLState() << " )" << std::endl;
		return {};
	}
}

bool MysqlDao::upsertFriendApply(const std::string& fromUid, const std::string& toUid, int status,
	const std::string& greeting, const std::string& applyTime, const std::string& handleTime, uint64_t version)
{
	auto connection = _pool->getConnection();
	if (!connection) return false;
	try
	{
		Defer defer([this, &connection]
		{
			_pool->returnConnection(std::move(connection));
		});

		std::string sql;
		if (status == 0)
			sql = R"(
				INSERT INTO
					friend_apply(from_uid, to_uid, status, greeting, apply_time, handle_time, version)
				VALUES
					(?, ?, ?, ?, ?, ?, ?) AS new
				ON DUPLICATE KEY UPDATE
					status = new.status,
					apply_time = new.apply_time,
					handle_time = new.handle_time,
					greeting = new.greeting,
					version = new.version
			)";
		else
			sql = R"(
				INSERT INTO
					friend_apply(from_uid, to_uid, status, greeting, apply_time, handle_time, version)
				VALUES
					(?, ?, ?, ?, ?, ?, ?) AS new
				ON DUPLICATE KEY UPDATE
					status = new.status,
					handle_time = new.handle_time,
					version = new.version
			)";

		std::unique_ptr<sql::PreparedStatement> pstmt(connection->_con->prepareStatement(sql));

		pstmt->setString(1, fromUid);
		pstmt->setString(2, toUid);
		pstmt->setInt(3, status);
		pstmt->setString(4, greeting);
		pstmt->setUInt64(5, std::stoull(applyTime));
		pstmt->setUInt64(6, std::stoull(handleTime));
		pstmt->setUInt64(7, version);

		return pstmt->executeUpdate() > 0;
	}
	catch (sql::SQLException& e)
	{
		std::cerr << "SQLException: " << e.what() << std::endl;
		std::cerr << "(MYSQL error code: " << e.getErrorCode();
		std::cerr << ", SQLState: " << e.getSQLState() << " )" << std::endl;
		return false;
	}
}

bool MysqlDao::insertChatMessage(const std::string& senderUid, const std::string& targetUid, const std::string& msgData,
                                 const std::string& seq)
{
	auto connection = _pool->getConnection();
	if (!connection) return false;
	try
	{
		Defer defer([this, &connection]
		{
			_pool->returnConnection(std::move(connection));
		});
		// TODO：根据msgData的类型来动态判断msgType的类型，后期添加Status字段判断在线或离线以及Pending
		std::unique_ptr<sql::PreparedStatement> pstmt(connection->_con->prepareStatement(R"(
			insert into chat_history(sender_uid, target_uid, msg_data, msg_type, seq_id)
			values(?, ?, ?, 0, ?);
		)"));

		pstmt->setString(1, senderUid);
		pstmt->setString(2, targetUid);
		pstmt->setString(3, msgData);
		pstmt->setString(4, seq);

		int result(pstmt->executeUpdate());
		return true;
	}
	catch (sql::SQLException& e)
	{
		std::cerr << "SQLException: " << e.what() << std::endl;
		std::cerr << "(MYSQL error code: " << e.getErrorCode();
		std::cerr << ", SQLState: " << e.getSQLState() << " )" << std::endl;
		return false;
	}
}

std::vector<FriendInfo> MysqlDao::getFullFriends(const std::string& myUid)
{
	auto connection = _pool->getConnection();
	if (!connection) return {};
	try
	{
		Defer defer([this, &connection]
		{
			_pool->returnConnection(std::move(connection));
		});

		std::unique_ptr<sql::PreparedStatement> pstmt(connection->_con->prepareStatement(R"(
			select uid, name, email, fr.status, fr.create_time, fr.version from friend_relation as fr
			inner join user on fr.friend_uid = user.uid
			where fr.self_uid = ?
		)"));
		pstmt->setString(1, myUid);
		std::unique_ptr<sql::ResultSet> result(pstmt->executeQuery());
		std::vector<FriendInfo> friendList;
		while (result->next())
		{
			FriendInfo friendInfo;
			friendInfo._uid = std::to_string(result->getUInt64("uid"));
			friendInfo._username = result->getString("name");
			friendInfo._email = result->getString("email");
			friendInfo._status = result->getInt("status");
			friendInfo._avatarUrl = "";
			friendInfo._createTime = std::to_string(result->getUInt64("create_time"));
			friendInfo._version = std::to_string(result->getUInt64("version"));
			friendList.emplace_back(friendInfo);
		}
		return friendList;
	}
	catch (sql::SQLException& e)
	{
		std::cerr << "GetFriends,SQLException: " << e.what() << std::endl;
		std::cerr << "(MYSQL error code: " << e.getErrorCode();
		std::cerr << ", SQLState: " << e.getSQLState() << " )" << std::endl;
		return {};
	}
}

std::vector<ConversationInfo> MysqlDao::getAllConversations(const std::string& myUid)
{
	auto connection = _pool->getConnection();
	if (!connection) return {};
	try
	{
		Defer defer([this, &connection]
		{
			_pool->returnConnection(std::move(connection));
		});

		std::unique_ptr<sql::PreparedStatement> pstmt(connection->_con->prepareStatement(R"(
			select * from conversation where owner_uid = ?
		)"));

		pstmt->setString(1, myUid);
		std::unique_ptr<sql::ResultSet> result(pstmt->executeQuery());
		std::vector<ConversationInfo> conversationList;
		while (result->next())
		{
			ConversationInfo conversationInfo;
			conversationInfo.peerUid = result->getString("peer_uid");
			conversationInfo.sessionType = result->getInt("session_type");
			conversationInfo.lastMsgSenderUid = result->getString("last_msg_sender_uid");
			conversationInfo.lastMsgContent = result->getString("last_msg_content");
			conversationInfo.lastMsgType = result->getInt("last_msg_type");
			conversationInfo.lastSeqId = std::to_string(result->getUInt64("last_seq_id"));
			conversationInfo.unreadCount = result->getUInt("unread_count");
			conversationInfo.updateTime = std::to_string(result->getUInt64("update_time"));
			conversationInfo.lastMsgId = std::to_string(result->getUInt64("last_msg_id"));
			conversationInfo.version = std::to_string(result->getUInt64("version"));
			conversationList.emplace_back(conversationInfo);
		}
		return conversationList;
	}
	catch (sql::SQLException& e)
	{
		std::cerr << "SQLException: " << e.what() << std::endl;
		std::cerr << "(MYSQL error code: " << e.getErrorCode();
		std::cerr << ", SQLState: " << e.getSQLState() << " )" << std::endl;
		return {};
	}
}

std::vector<MsgInfo> MysqlDao::getSyncMessages(const std::string& senderUid, const std::string& targetUid, int sessionType,
	const std::string& startVersion, const std::string& endVersion, int limit)
{
	auto connection = _pool->getConnection();
	if (!connection) return {};
	try
	{
		Defer defer([this, &connection]
		{
			_pool->returnConnection(std::move(connection));
		});

		std::string sql;
		std::unique_ptr<sql::PreparedStatement> pstmt;

		if (sessionType == 1)
		{
			sql = R"(
	            SELECT
					*
				FROM
					chat_history
	            WHERE
					((sender_uid = ? AND target_uid = ?) OR (sender_uid = ? AND target_uid = ?))
					AND session_type = 1 AND msg_id > ? AND msg_id < ?
	            ORDER BY
					msg_id DESC
				LIMIT ?
			)";
			pstmt.reset(connection->_con->prepareStatement(sql));
			pstmt->setString(1, senderUid);
			pstmt->setString(2, targetUid);
			pstmt->setString(3, targetUid);
			pstmt->setString(4, senderUid);
			pstmt->setUInt64(5, std::stoull(startVersion));
			pstmt->setUInt64(6, std::stoull(endVersion));
			pstmt->setInt(7, limit);
		}
		else
		{
			// 群聊同理
			sql = R"(
	            SELECT
					*
				FROM
					chat_history
	            WHERE
					target_uid = ? AND session_type = 2
					AND msg_id > ? AND msg_id < ?
	            ORDER BY
					msg_id DESC
				LIMIT ?
        )";
			pstmt.reset(connection->_con->prepareStatement(sql));
			pstmt->setString(1, targetUid);
			pstmt->setUInt64(5, std::stoull(startVersion));
			pstmt->setUInt64(6, std::stoull(endVersion));
			pstmt->setInt(4, limit);
		}

		std::vector<MsgInfo> msgList;
		std::unique_ptr<sql::ResultSet> result(pstmt->executeQuery());
		while (result->next())
		{
			MsgInfo msg;
			msg.msgId = std::to_string(result->getUInt64("msg_id"));
			msg.senderUid = result->getString("sender_uid");
			msg.peerUid = result->getString("target_uid");
			msg.sessionType = result->getInt("session_type");
			msg.clientMsgId = result->getString("client_msg_id");
			msg.msgData = result->getString("msg_content");
			msg.msgType = result->getInt("msg_type");
			msg.seqId = std::to_string(result->getUInt64("seq_id"));
			msg.isRead = result->getInt("is_read");
			msg.createTime = std::to_string(result->getUInt64("create_time"));
			msgList.emplace_back(msg);
		}

		if (!msgList.empty())
			std::reverse(msgList.begin(), msgList.end());
		return msgList;
	}
	catch (sql::SQLException& e)
	{
		std::cerr << "SQLException: " << e.what() << std::endl;
		std::cerr << "(MYSQL error code: " << e.getErrorCode();
		std::cerr << ", SQLState: " << e.getSQLState() << " )" << std::endl;
		return {};
	}
}

std::shared_ptr<FriendApplyInfo> MysqlDao::getFriendApply(const std::string& fromUid, const std::string& toUid)
{
	auto connection = _pool->getConnection();
	if (!connection) return nullptr;
	try
	{
		Defer defer([this, &connection]
		{
			_pool->returnConnection(std::move(connection));
		});
		std::unique_ptr<sql::PreparedStatement> pstmt(connection->_con->prepareStatement(R"(
			SELECT * FROM friend_apply WHERE from_uid = ? AND to_uid = ?;
		)"));

		pstmt->setString(1, fromUid);
		pstmt->setString(2, toUid);

		std::unique_ptr<sql::ResultSet> result(pstmt->executeQuery());
		if (result->next())
		{
			std::shared_ptr<FriendApplyInfo> friendApplyInfo = std::make_shared<FriendApplyInfo>();
			friendApplyInfo->fromUid = result->getString("from_uid");
			friendApplyInfo->toUid = result->getString("to_uid");
			friendApplyInfo->status = result->getInt("status");
			friendApplyInfo->greeting = result->getString("greeting");
			friendApplyInfo->applyTime = std::to_string(result->getUInt64("apply_time"));
			friendApplyInfo->handleTime = std::to_string(result->getUInt64("handle_time"));
			friendApplyInfo->version = std::to_string(result->getUInt64("version"));
			return friendApplyInfo;
		}
		return nullptr;
	}
	catch (sql::SQLException& e)
	{
		std::cerr << "SQLException: " << e.what() << std::endl;
		std::cerr << "(MYSQL error code: " << e.getErrorCode();
		std::cerr << ", SQLState: " << e.getSQLState() << " )" << std::endl;
		return {};
	}
}

bool MysqlDao::updateAllMsgReadStatus(const std::string& senderUid, const std::string& targetUid, int sessionType)
{
	auto connection = _pool->getConnection();
	if (!connection) return false;
	try
	{
		Defer defer([this, &connection]
		{
			_pool->returnConnection(std::move(connection));
		});
		std::unique_ptr<sql::PreparedStatement> pstmt(connection->_con->prepareStatement(R"(
			UPDATE
				chat_history
			SET
				is_read = 1
			WHERE
				sender_uid = ? AND target_uid = ? AND session_type = ?
		)"));

		pstmt->setString(1, senderUid);
		pstmt->setString(2, targetUid);
		pstmt->setInt(3, sessionType);
		pstmt->executeUpdate();
		return true;
	}
	catch (sql::SQLException& e)
	{
		std::cerr << "SQLException: " << e.what() << std::endl;
		std::cerr << "(MYSQL error code: " << e.getErrorCode();
		std::cerr << ", SQLState: " << e.getSQLState() << " )" << std::endl;
		return false;
	}
}

bool MysqlDao::clearConvUnreadCount(const std::string& ownerUid, const std::string& peerUid, int sessionType)
{
	auto connection = _pool->getConnection();
	if (!connection) return false;
	try
	{
		Defer defer([this, &connection]
		{
			_pool->returnConnection(std::move(connection));
		});
		std::unique_ptr<sql::PreparedStatement> pstmt(connection->_con->prepareStatement(R"(
			UPDATE
				conversation
			SET
				unread_count = 0
			WHERE
				owner_uid = ? AND peer_uid = ? AND session_type = ?
		)"));

		pstmt->setString(1, ownerUid);
		pstmt->setString(2, peerUid);
		pstmt->setInt(3, sessionType);
		pstmt->executeUpdate();
		return true;
	}
	catch (sql::SQLException& e)
	{
		std::cerr << "SQLException: " << e.what() << std::endl;
		std::cerr << "(MYSQL error code: " << e.getErrorCode();
		std::cerr << ", SQLState: " << e.getSQLState() << " )" << std::endl;
		return false;
	}
}

bool MysqlDao::callUpsertFriendApplyProc(const std::string& fromUid, const std::string& toUid, int status,
                                         const std::string& greeting, const std::string& applyTime, const std::string& handleTime, uint64_t& version)
{
	auto connection = _pool->getConnection();
	if (!connection) return false;
	try
	{
		Defer defer([this, &connection]
		{
			_pool->returnConnection(std::move(connection));
		});

		std::unique_ptr<sql::PreparedStatement> pstmt(connection->_con->prepareStatement(R"(
			CALL upsertFriendApply(?, ?, ?, ?, ?, ?, @friendApplyVersion);
		)"));

		pstmt->setString(1, fromUid);
		pstmt->setString(2, toUid);
		pstmt->setInt(3, status);
		pstmt->setString(4, greeting);
		pstmt->setUInt64(5, std::stoull(applyTime));
		pstmt->setUInt64(6, std::stoull(handleTime));
		pstmt->execute();

		std::unique_ptr<sql::Statement> stmt(connection->_con->createStatement());
		std::unique_ptr<sql::ResultSet> result(stmt->executeQuery("SELECT @friendApplyVersion as version"));
		if (result->next())
		{
			version = result->getUInt64("version");
			return true;
		}
		return false;
	}
	catch (sql::SQLException& e)
	{
		std::cerr << "SQLException: " << e.what() << std::endl;
		std::cerr << "(MYSQL error code: " << e.getErrorCode();
		std::cerr << ", SQLState: " << e.getSQLState() << " )" << std::endl;
		return false;
	}
}

bool MysqlDao::callAuthFriendApplyProc(const std::string& fromUid, const std::string& toUid, int newStatus,
	const std::string& handleTime, uint64_t& outVersion)
{
	auto connection = _pool->getConnection();
	if (!connection) return false;
	try
	{
		Defer defer([this, &connection] {
		   _pool->returnConnection(std::move(connection));
		});

		std::unique_ptr<sql::PreparedStatement> pstmt(connection->_con->prepareStatement(R"(
			CALL updateAuthFriend(?, ?, ?, ?, @authVersion);
		)"));

		pstmt->setString(1, fromUid);
		pstmt->setString(2, toUid);
		pstmt->setInt(3, newStatus);
		pstmt->setUInt64(4, handleTime.empty() ? 0 : std::stoull(handleTime));
		pstmt->execute();

		// 提取生成的版本号
		std::unique_ptr<sql::Statement> stmt(connection->_con->createStatement());
		std::unique_ptr<sql::ResultSet> result(stmt->executeQuery("SELECT @authVersion as version"));

		if (result->next())
		{
			outVersion = result->getUInt64("version");
			return true;
		}
		return false;
	}
	catch (sql::SQLException& e)
	{
		std::cerr << "SQLException: " << e.what() << std::endl;
		std::cerr << "(MYSQL error code: " << e.getErrorCode();
		std::cerr << ", SQLState: " << e.getSQLState() << " )" << std::endl;
		return false;
	}
}

std::vector<FriendApplyInfo> MysqlDao::getSyncFriendApplies(const std::string& uid, uint64_t version)
{
	auto connection = _pool->getConnection();
	if (!connection) return {};
	try
	{
		Defer defer([this, &connection] {
			_pool->returnConnection(std::move(connection));
		});

		std::unique_ptr<sql::PreparedStatement> pstmt(connection->_con->prepareStatement(R"(
			SELECT * FROM friend_apply
			WHERE (from_uid = ? OR to_uid = ?) AND version > ?
		)"));

		pstmt->setString(1, uid);
		pstmt->setString(2, uid);
		pstmt->setUInt64(3, version);

		std::unique_ptr<sql::ResultSet> result(pstmt->executeQuery());
		std::vector<FriendApplyInfo> applyList;

		while (result->next())
		{
			FriendApplyInfo info;
			info.fromUid = result->getString("from_uid");
			info.toUid = result->getString("to_uid");
			info.status = result->getInt("status");
			info.greeting = result->getString("greeting");
			info.applyTime = std::to_string(result->getUInt64("apply_time"));
			info.handleTime = std::to_string(result->getUInt64("handle_time"));
			info.version = std::to_string(result->getUInt64("version"));
			applyList.emplace_back(info);
		}
		return applyList;
	}
	catch (sql::SQLException& e)
	{
		std::cerr << "getSyncFriendApplies SQLException: " << e.what() << std::endl;
		return {};
	}
}

std::vector<FriendInfo> MysqlDao::getSyncFriends(const std::string& uid, uint64_t version)
{
	auto connection = _pool->getConnection();
	if (!connection) return {};
	try
	{
		Defer defer([this, &connection] {
			_pool->returnConnection(std::move(connection));
		});

		std::unique_ptr<sql::PreparedStatement> pstmt(connection->_con->prepareStatement(R"(
			SELECT user.uid, user.name, user.email, fr.status, fr.create_time, fr.version
			FROM friend_relation AS fr
			INNER JOIN user ON fr.friend_uid = user.uid
			WHERE fr.self_uid = ? AND fr.version > ?
		)"));

		pstmt->setString(1, uid);
		pstmt->setUInt64(2, version);

		std::unique_ptr<sql::ResultSet> result(pstmt->executeQuery());
		std::vector<FriendInfo> friendList;

		while (result->next())
		{
			FriendInfo friendInfo;
			friendInfo._uid = std::to_string(result->getUInt64("uid"));
			friendInfo._username = result->getString("name");
			friendInfo._email = result->getString("email");
			friendInfo._status = result->getInt("status");
			friendInfo._avatarUrl = "";
			friendInfo._createTime = std::to_string(result->getUInt64("create_time"));
			friendInfo._version = std::to_string(result->getUInt64("version"));
			friendList.emplace_back(friendInfo);
		}
		return friendList;
	}
	catch (sql::SQLException& e)
	{
		std::cerr << "getSyncFriends SQLException: " << e.what() << std::endl;
		return {};
	}
}

std::vector<ConversationInfo> MysqlDao::getSyncConversations(const std::string& uid, uint64_t version)
{
	auto connection = _pool->getConnection();
	if (!connection) return {};
	try
	{
		Defer defer([this, &connection] {
			_pool->returnConnection(std::move(connection));
		});

		std::unique_ptr<sql::PreparedStatement> pstmt(connection->_con->prepareStatement(R"(
			SELECT * FROM conversation
			WHERE owner_uid = ? AND version > ?
		)"));

		pstmt->setString(1, uid);
		pstmt->setUInt64(2, version);

		std::unique_ptr<sql::ResultSet> result(pstmt->executeQuery());
		std::vector<ConversationInfo> conversationList;

		while (result->next())
		{
			ConversationInfo info;
			info.peerUid = result->getString("peer_uid");
			info.sessionType = result->getInt("session_type");
			info.lastMsgSenderUid = result->getString("last_msg_sender_uid");
			info.lastMsgContent = result->getString("last_msg_content");
			info.lastMsgType = result->getInt("last_msg_type");
			info.lastSeqId = std::to_string(result->getUInt64("last_seq_id"));
			info.unreadCount = result->getUInt("unread_count");
			info.updateTime = std::to_string(result->getUInt64("update_time"));
			info.lastMsgId = std::to_string(result->getUInt64("last_msg_id"));
			info.version = std::to_string(result->getUInt64("version"));
			conversationList.emplace_back(info);
		}
		return conversationList;
	}
	catch (sql::SQLException& e)
	{
		std::cerr << "getSyncConversations SQLException: " << e.what() << std::endl;
		return {};
	}
}

std::vector<MsgInfo> MysqlDao::getRepairMessages(const std::string& peerUid, const std::string& myUid, int sessionType,
	uint64_t fromSeq, uint64_t toSeq)
{
	auto connection = _pool->getConnection();
	if (!connection) return {};
	try
	{
		Defer defer([this, &connection] {
			_pool->returnConnection(std::move(connection));
		});

		// TODO:针对单聊和群聊，target_uid 的定义是不同的
		std::unique_ptr<sql::PreparedStatement> pstmt(connection->_con->prepareStatement(R"(
			SELECT * FROM chat_history
			WHERE target_uid = ? AND sender_uid = ? AND session_type = ? AND seq_id BETWEEN ? AND ?
			ORDER BY seq_id ASC
		)"));

		pstmt->setString(1, myUid);
		pstmt->setString(2, peerUid);
		pstmt->setInt(3, sessionType);
		pstmt->setUInt64(4, fromSeq);
		pstmt->setUInt64(5, toSeq);

		std::unique_ptr<sql::ResultSet> result(pstmt->executeQuery());
		std::vector<MsgInfo> msgList;

		while (result->next())
		{
			MsgInfo info;
			info.msgId = result->getString("msg_id");
			info.seqId = std::to_string(result->getUInt64("seq_id"));
			info.createTime = std::to_string(result->getUInt64("create_time"));
			info.peerUid = result->getString("target_uid");
			info.senderUid = result->getString("sender_uid");
			info.sessionType = result->getInt("session_type");
			info.msgData = result->getString("msg_content");
			info.msgType = result->getInt("msg_type");
			info.isRead = result->getInt("is_read");
			msgList.emplace_back(info);
		}
		return msgList;
	}
	catch (sql::SQLException& e)
	{
		std::cerr << "getSyncConversations SQLException: " << e.what() << std::endl;
		return {};
	}
}

bool MysqlDao::callAddFriendProc(const std::string& selfUid, const std::string& friendUid, int status,
                                 const std::string& createTime, uint64_t& newFriendVersion)
{
	auto connection = _pool->getConnection();
	if (!connection) return false;
	try
	{
		Defer defer([this, &connection] {
			_pool->returnConnection(std::move(connection));
		});

		std::unique_ptr<sql::PreparedStatement> pstmt(connection->_con->prepareStatement(R"(
			CALL addFriendRelation(?, ?, ?, ?, @friendVersion);
		)"));

		pstmt->setString(1, selfUid);
		pstmt->setString(2, friendUid);
		pstmt->setInt(3, status);
		pstmt->setUInt64(4, std::stoull(createTime));
		pstmt->execute();

		std::unique_ptr<sql::Statement> stmt(connection->_con->createStatement());
		std::unique_ptr<sql::ResultSet> result(stmt->executeQuery("SELECT @friendVersion as version"));

		if (result->next())
		{
			newFriendVersion = result->getUInt64("version");
			return true;
		}
		return false;
	}
	catch (sql::SQLException& e)
	{
		std::cerr << "callAddFriendProc SQLException: " << e.what() << std::endl;
		return false;
	}
}

