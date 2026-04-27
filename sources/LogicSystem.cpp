#include <utility>
#include "../headers/LogicSystem.h"
#include "../headers/CSession.h"
#include "../headers/MysqlMgr.h"
#include "../headers/RapidJsonMgr.h"
#include "../headers/RedisMgr.h"
#include "../headers/RouteGrpcClient.h"
#include "../headers/Snowflake.h"
#include "../headers/RabbitMQClient.h"
#include "../headers/ConfigMgr.h"

LogicSystem::LogicSystem() :
	_threadPool(std::thread::hardware_concurrency() * 4),
	_dbPool(std::thread::hardware_concurrency() * 2),
	_snowflake(std::make_shared<Snowflake>(1, 1))
{
	// 初始化 16 个并发车道（Strands）
    for (int i = 0; i < 16; ++i) {
        _strands.push_back(std::make_shared<boost::asio::strand<boost::asio::thread_pool::executor_type>>(_threadPool.get_executor()));
    }
	registerHandlers();
}

void LogicSystem::registerHandlers()
{
	// 当前操作都是在线程池里操作，需要注意线程安全问题
	_handlers[ID_CHAT_LOGIN] = [this](std::shared_ptr<LogicNode> logicNode)
	{
		auto session = logicNode->_curSession.lock();
		if (!session) {
			std::cerr << "Session expired before processing login." << std::endl;
			return;
		}
		auto msg = logicNode->_curMsgNode;
		// 解析Json字符串
		rapidjson::Document doc = RapidJsonMgr::parseJson(msg->_data);
		// 获取json对象的value值
		std::string uid = RapidJsonMgr::toString(doc, "uid");
		std::string token = RapidJsonMgr::toString(doc, "token");
		std::cout << "当前执行的登录逻辑，准备验证用户名和密码" << std::endl;
		rapidjson::Document sendDoc = RapidJsonMgr::createDocument();
		Defer defer([session, &sendDoc]
		{
			auto msg = std::make_shared<MsgNode>();
			msg->_data = RapidJsonMgr::toJsonString(sendDoc, false);
			msg->_id = static_cast<uint16_t>(ID_CHAT_LOGIN_RSP);
			msg->_len = static_cast<uint16_t>(msg->_data.size());
			session->postSend(msg);
		});


		// ===================== 压测与路由兼容逻辑 START =====================
		bool isStressTest = (token == "test_token");
		std::string realToken = token;

		if (isStressTest)
		{
			// 【压测专供】：模拟 GateServer，强行把当前测试用户的路由信息注册进 Redis！
			// 否则 chatToTargetUser 会认为该测试用户不在线，导致互相发消息全部被抛弃。
			rapidjson::Document mockRoute = RapidJsonMgr::createDocument();
			auto& cfg = ConfigMgr::getInstance();
			RapidJsonMgr::addMember(mockRoute, "serverName", cfg["SelfServer"]["Name"]);
			RapidJsonMgr::addMember(mockRoute, "token", token);
			RedisMgr::getServerConfigRedis().set(uid, RapidJsonMgr::toJsonString(mockRoute, false), std::chrono::hours(24));
		}
		else
		{
			// 【正式服逻辑】：从 Redis 获取网关写入的 Token 进行比对
			const auto& optionalString = RedisMgr::getServerConfigRedis().get(uid);
			if (!optionalString) {
				RapidJsonMgr::addMember(sendDoc, "error", ErrorCodes::UidInvalid);
				return;
			}
			rapidjson::Document redisDoc = RapidJsonMgr::parseJson(optionalString.value());
			if (redisDoc.HasMember("parse_error")) {
				RapidJsonMgr::addMember(sendDoc, "error", ErrorCodes::Error_Json);
				return;
			}
			realToken = RapidJsonMgr::toString(redisDoc, "token");
			if (realToken != token) {
				RapidJsonMgr::addMember(sendDoc, "error", ErrorCodes::TokenInvalid);
				return;
			}
		}
		// ===================== 压测与路由兼容逻辑 END =====================

		std::shared_ptr<UserConnectionCtx> connectionCtx;
		{
			std::unique_lock<std::shared_mutex> lock(_shared_mutex);
			// 命中缓存
			if (auto it = _users.find(uid); it != _users.end())
			{
				lock.unlock();
				std::scoped_lock<std::shared_mutex> connectinCtxLock(it->second->shared_mutex);
				connectionCtx = it->second;
				if (connectionCtx->curState == 2)
				{
					// 取消重连定时器
					if (connectionCtx->reconnectTimer)
						connectionCtx->reconnectTimer->cancel();
					// 取消Redis过期时间
					auto& redis = RedisMgr::getServerConfigRedis();
					redis.persist(uid);
				}
			}
			// 未命中缓存，新连接
			else
			{
				// 首次登录本台服务器
				connectionCtx = std::make_shared<UserConnectionCtx>();
				connectionCtx->uid = uid;
				_users[uid] = connectionCtx;
			}
		}

		assert(connectionCtx);
		connectionCtx->curState = 1;
		// 更新双向绑定
		connectionCtx->csession = session;
		session->setUserInfoUid(uid);

		RapidJsonMgr::addMember(sendDoc, "error", ErrorCodes::Success);
		// 查找UserInfo数据
		auto userInfo = getUserProfile(uid);
		RapidJsonMgr::addMember(sendDoc, "uid", uid);
		RapidJsonMgr::addMember(sendDoc, "name", userInfo->username);
		RapidJsonMgr::addMember(sendDoc, "email", userInfo->email);
		RapidJsonMgr::addMember(sendDoc, "token", token);
		std::cout << "user " << uid << " login success, the token is " << realToken << std::endl;
	};

	_handlers[ID_SEARCH_USER_REQ] = [this](std::shared_ptr<LogicNode> logicNode)
	{
		assert(logicNode->_curSession.lock());
		auto session = logicNode->_curSession.lock();
		rapidjson::Document doc = RapidJsonMgr::parseJson(logicNode->_curMsgNode->_data);
		const std::string& username = RapidJsonMgr::toString(doc, "targetUsername");
		const std::string& email = RapidJsonMgr::toString(doc, "targetEmail");

		rapidjson::Document rspDoc = RapidJsonMgr::createDocument();
		Defer defer([session, &rspDoc]
		{
			auto msg = std::make_shared<MsgNode>();
			msg->_data = RapidJsonMgr::toJsonString(rspDoc, false);
			msg->_id =static_cast<uint16_t>(ID_SEARCH_USER_RSP);
			msg->_len = static_cast<uint16_t>(msg->_data.size());
			session->postSend(msg);
		});

		// 查本地 / 查数据库 (封装为一个函数)
		auto result = MysqlMgr::getInstance()->searchUserInfoList(username, email);
		if (result.empty())
		{
			RapidJsonMgr::addMember(rspDoc, "error", ErrorCodes::UserNotFound);
			return;
		}
		RapidJsonMgr::addMember(rspDoc, "error", ErrorCodes::Success);

		// 组装JSON Array
		rapidjson::Value userArray(rapidjson::kArrayType);
		rapidjson::Document::AllocatorType& allocator = rspDoc.GetAllocator();
		for (const auto& [uid, userInfo] : result)
		{
			rapidjson::Value userObj(rapidjson::kObjectType);

			rapidjson::Value uidVal(userInfo->uid.c_str(), userInfo->uid.length(), allocator);
			userObj.AddMember("uid", uidVal, allocator);

			rapidjson::Value nameVal(userInfo->username.c_str(), userInfo->username.length(), allocator);
			userObj.AddMember("name", nameVal, allocator);

			rapidjson::Value emailVal(userInfo->email.c_str(), userInfo->email.length(), allocator);
			userObj.AddMember("email", emailVal, allocator);

			rapidjson::Value versionVal(userInfo->version.c_str(), userInfo->version.length(), allocator);
			userObj.AddMember("version", versionVal, allocator);

			userArray.PushBack(userObj, allocator);
		}
		// 将真实数组挂载到 rspDoc 的 searchList 字段上
		rspDoc.AddMember("searchList", userArray, allocator);
	};

	_handlers[ID_ADD_FRIEND_REQ] = [this](std::shared_ptr<LogicNode> logicNode)
	{
		assert(logicNode->_curSession.lock());
		auto session = logicNode->_curSession.lock();
		rapidjson::Document reqDoc = RapidJsonMgr::parseJson(logicNode->_curMsgNode->_data);
		assert(!reqDoc.HasMember("parse_error"));

		// 获取对方uid
		const std::string& targetUid = RapidJsonMgr::toString(reqDoc, "targetUid");
		// 获取greeting信息
		const std::string& greetingMsg = RapidJsonMgr::toString(reqDoc, "greeting");
		// 获取请求时间信息
		const std::string& applyTime = RapidJsonMgr::toString(reqDoc, "applyTime");
		// 获取本地uid
		const std::string& myUid = session->getUserInfoUid();

		// POST——DB线程池处理数据库相关操作
		boost::asio::post(_dbPool, [this, session, myUid, targetUid, greetingMsg, applyTime]
		{
			// 同步 MySQL 落盘并获取最新 Version
			uint64_t newVersion = 0;
			bool dbSuccess = MysqlMgr::getInstance()->callUpsertFriendApplyProc(myUid, targetUid, 0, greetingMsg, applyTime, "0", newVersion);
			if (!dbSuccess) return;

			// 删除 Redis 缓存 (Cache Aside)
			auto& redis = RedisMgr::getFriendApplyRedis();
			std::string memberKey = myUid + ":" + targetUid;
			redis.del("apply_data:" + memberKey);

			// 直接查Redis/Mysql，后者再更新到本地
			auto targetUserInfo = getUserProfile(targetUid);
			auto myUserInfo = getUserProfile(myUid);

			// 业务转发逻辑
			boost::asio::post(_threadPool, [=, this]
			{
				// 创建RSP-JSON文档
				rapidjson::Document rspDoc = RapidJsonMgr::createDocument();

				// 判断User是否存在
				if (!targetUserInfo || !myUserInfo)
				{
					RapidJsonMgr::addMember(rspDoc, "error", ErrorCodes::UserNotFound);
					return;
				}

				// 返回成功RSP回包
				RapidJsonMgr::addMember(rspDoc, "error", ErrorCodes::Success);
				RapidJsonMgr::addMember(rspDoc, "targetUid", targetUid);
				RapidJsonMgr::addMember(rspDoc, "applyVersion", std::to_string(newVersion));

				// 发送RSP回包
				auto rspMsg = std::make_shared<MsgNode>();
				rspMsg->_data = RapidJsonMgr::toJsonString(rspDoc, false);
				rspMsg->_id = static_cast<uint16_t>(ID_ADD_FRIEND_RSP);
				rspMsg->_len = static_cast<uint16_t>(rspMsg->_data.size());
				session->postSend(rspMsg);

				// 创建NOTIFY通知包
				rapidjson::Document notifyDoc = RapidJsonMgr::createDocument();
				RapidJsonMgr::addMember(notifyDoc, "fromUid", myUid);
				RapidJsonMgr::addMember(notifyDoc, "fromUsername", myUserInfo->username);
				RapidJsonMgr::addMember(notifyDoc, "fromUserEmail", myUserInfo->email);
				RapidJsonMgr::addMember(notifyDoc, "fromUserVersion", myUserInfo->version);
				RapidJsonMgr::addMember(notifyDoc, "applyTime", applyTime);
				RapidJsonMgr::addMember(notifyDoc, "greeting", greetingMsg);
				RapidJsonMgr::addMember(notifyDoc, "applyVersion", std::to_string(newVersion));

				// 转发投送
				chatToTargetUser(targetUid, RapidJsonMgr::toJsonString(notifyDoc, false), ID_ADD_FRIEND_NOTIFY);
			});
		});
	};

	_handlers[ID_AUTH_FRIEND_REQ] = [this](std::shared_ptr<LogicNode> logicNode)
	{
		assert(logicNode->_curSession.lock());
		auto session = logicNode->_curSession.lock();
		rapidjson::Document reqDoc = RapidJsonMgr::parseJson(logicNode->_curMsgNode->_data);
		assert(!reqDoc.HasMember("parse_error"));

		std::string applicantUid = RapidJsonMgr::toString(reqDoc, "applicantUid");
		std::string action = RapidJsonMgr::toString(reqDoc, "action");
		std::string handleTime = RapidJsonMgr::toString(reqDoc, "handleTime");
		std::string myUid = session->getUserInfoUid();

		int status = (action == "agree") ? 1 : 2;

		boost::asio::post(_dbPool, [this, session, applicantUid, myUid, status, action, handleTime]()
		{
			// 同步 MySQL 落盘并获取最新 Version
			uint64_t newVersion = 0;
			// 假设底层调用了专门处理 Auth 的存储过程：只 UPDATE status, handle_time 并递增 Version
			bool dbSuccess = MysqlMgr::getInstance()->callAuthFriendApplyProc(applicantUid, myUid, status, handleTime, newVersion);
			if (!dbSuccess) return;

			// 异步删除 Redis 缓存
			auto& redis = RedisMgr::getFriendApplyRedis();
			std::string memberKey = applicantUid + ":" + myUid;
			redis.del("apply_data:" + memberKey);

			// 如果是同意，顺便写入 MySQL 的 friend_list 双向关系
			uint64_t newFriendVersion = 0;
			if (action == "agree") {
				// 调用刚写的双向写入存储过程
				MysqlMgr::getInstance()->callAddFriendProc(myUid, applicantUid, status, handleTime, newFriendVersion);

				// 因为对方变成了好友，建议在这里一并把两人的 user_info 缓存从 Redis 里删掉，
				// 防止旧资料作祟 (可选)
				auto& userRedis = RedisMgr::getUserInfoRedis();
				userRedis.del("profile:" + myUid);
				userRedis.del("profile:" + applicantUid);
			}

			auto applicantUserInfo = getUserProfile(applicantUid); // A
			auto myUserInfo = getUserProfile(myUid); // B

			boost::asio::post(_threadPool, [=, this]
			{
				// 创建RSP-JSON文档
				rapidjson::Document rspDoc = RapidJsonMgr::createDocument();

				// 判断用户是否存在
				if (!myUserInfo || !applicantUserInfo)
				{
					RapidJsonMgr::addMember(rspDoc, "error", ErrorCodes::UserNotFound);
					return;
				}

				if (action == "agree")
					RapidJsonMgr::addMember(rspDoc, "friendVersion", std::to_string(newFriendVersion));

				// 返回成功RSP回包
				RapidJsonMgr::addMember(rspDoc, "error", ErrorCodes::Success);
				RapidJsonMgr::addMember(rspDoc, "action", action);
				RapidJsonMgr::addMember(rspDoc, "applicantUid", applicantUid);
				RapidJsonMgr::addMember(rspDoc, "applyVersion", std::to_string(newVersion));

				// 发送RSP回包
				auto rspMsg = std::make_shared<MsgNode>();
				rspMsg->_data = RapidJsonMgr::toJsonString(rspDoc, false);
				rspMsg->_id = static_cast<uint16_t>(ID_AUTH_FRIEND_RSP);
				rspMsg->_len = static_cast<uint16_t>(rspMsg->_data.size());
				session->postSend(rspMsg);

				// 创建NOTIFY通知包
				rapidjson::Document notifyDoc = RapidJsonMgr::createDocument();
				RapidJsonMgr::addMember(notifyDoc, "action", action);
				RapidJsonMgr::addMember(notifyDoc, "friendUid", myUid);
				RapidJsonMgr::addMember(notifyDoc, "friendName", myUserInfo->username);
				RapidJsonMgr::addMember(notifyDoc, "friendEmail", myUserInfo->email);
				RapidJsonMgr::addMember(notifyDoc, "handleTime", handleTime);
				RapidJsonMgr::addMember(notifyDoc, "applyVersion", std::to_string(newVersion));

				if (action == "agree")
					RapidJsonMgr::addMember(notifyDoc, "friendVersion", std::to_string(newFriendVersion));

				// 转发投递
				chatToTargetUser(applicantUid, RapidJsonMgr::toJsonString(notifyDoc, false), ID_AUTH_FRIEND_NOTIFY);
			});
		});
	};

	_handlers[ID_GET_FRIEND_LIST_REQ] = [this](std::shared_ptr<LogicNode> logicNode)
	{
		assert(logicNode->_curSession.lock());
		auto session = logicNode->_curSession.lock();
		rapidjson::Document reqDoc = RapidJsonMgr::parseJson(logicNode->_curMsgNode->_data);
		assert(!reqDoc.HasMember("parse_error"));

		const std::string& uid = RapidJsonMgr::toString(reqDoc, "uid");

		// 安全校验：只能拉取自己的好友列表
		if (uid != session->getUserInfoUid()) {
			rapidjson::Document rspDoc = RapidJsonMgr::createDocument();
			RapidJsonMgr::addMember(rspDoc, "error", ErrorCodes::UidInvalid);

			auto msg = std::make_shared<MsgNode>();
			msg->_data = RapidJsonMgr::toJsonString(rspDoc, false);
			msg->_id =static_cast<uint16_t>(ID_GET_FRIEND_LIST_RSP);
			msg->_len = static_cast<uint16_t>(msg->_data.size());
			session->postSend(msg);
			return;
		}

		boost::asio::post(_dbPool, [this, session, uid]
		{
			auto friendList = MysqlMgr::getInstance()->getFullFriends(uid);

			boost::asio::post(_threadPool, [session, uid, friendList = std::move(friendList)]
			{
				rapidjson::Document rspDoc = RapidJsonMgr::createDocument();
				RapidJsonMgr::addMember(rspDoc, "error", ErrorCodes::Success);

				rapidjson::Value friendArray = RapidJsonMgr::createArray();
				rapidjson::Document::AllocatorType& allocator = rspDoc.GetAllocator();

				for (const auto& friendInfo : friendList)
				{
					rapidjson::Value userObj = RapidJsonMgr::createObject();

					// 组装 uid
					rapidjson::Value uidVal(friendInfo._uid.c_str(), friendInfo._uid.length(), allocator);
					userObj.AddMember("uid", uidVal, allocator);

					// 组装 name
					rapidjson::Value nameVal(friendInfo._username.c_str(), friendInfo._username.length(), allocator);
					userObj.AddMember("name", nameVal, allocator);

					// 组装 email (判空保护)
					const std::string& emailStr = friendInfo._email.empty() ? "" : friendInfo._email;
					rapidjson::Value emailVal(emailStr.c_str(), emailStr.length(), allocator);
					userObj.AddMember("email", emailVal, allocator);

					// 组装 avatarUrl
					const std::string& avatarUrlStr = friendInfo._avatarUrl.empty() ? "" : friendInfo._avatarUrl;
					rapidjson::Value avatarUrVal(avatarUrlStr.c_str(), avatarUrlStr.length(), allocator);
					userObj.AddMember("avatarUrl", avatarUrVal, allocator);

					friendArray.PushBack(userObj, allocator);
				}
				rspDoc.AddMember("friendList", friendArray, allocator);

				auto msg = std::make_shared<MsgNode>();
				msg->_data = RapidJsonMgr::toJsonString(rspDoc, false);
				msg->_id = static_cast<uint16_t>(ID_GET_FRIEND_LIST_RSP);
				msg->_len = static_cast<uint16_t>(msg->_data.size());

				session->postSend(msg);

				for (const auto& friendInfo : friendList)
				{
					std::cout << "uid: " << friendInfo._uid << std::endl;
					std::cout << "username" << friendInfo._username << std::endl;
					std::cout << "email: " << friendInfo._email << std::endl;
				}
				std::cout << "User " << uid << " pulled friend list, count: " << friendList.size() << std::endl;
			});
		});
	};
	_handlers[ID_CHAT_MSG_REQ] = [this](std::shared_ptr<LogicNode> logicNode)
	{
		assert(logicNode->_curSession.lock());
		auto session = logicNode->_curSession.lock();
		rapidjson::Document reqDoc = RapidJsonMgr::parseJson(logicNode->_curMsgNode->_data);
		assert(!reqDoc.HasMember("parse_error"));

		rapidjson::Document rspDoc = RapidJsonMgr::createDocument();
		Defer defer([session, &rspDoc]
		{
			auto msg = std::make_shared<MsgNode>();
			msg->_data = RapidJsonMgr::toJsonString(rspDoc, false);
			msg->_id =static_cast<uint16_t>(ID_CHAT_MSG_RSP);
			msg->_len = static_cast<uint16_t>(msg->_data.size());
			session->postSend(msg);
		});

		// 客户端id
		const std::string& clientMsgId = RapidJsonMgr::toString(reqDoc, "clientMsgId");
		// 接收方Uid
		const std::string& targetUid = RapidJsonMgr::toString(reqDoc, "targetUid");
		// 发送方Uid
		const std::string& senderUid = RapidJsonMgr::toString(reqDoc, "senderUid");
		// 会话类型
		int sessionType = RapidJsonMgr::toInt(reqDoc, "sessionType");
		// 信息类型
		int msgType = RapidJsonMgr::toInt(reqDoc, "msgType");
		// 信息内容
		const std::string& msgData = RapidJsonMgr::toString(reqDoc, "msgData");
		// 发送序列
		const std::string& seqId = RapidJsonMgr::toString(reqDoc, "seqId");
		// 创建时间
		const std::string& createTime = RapidJsonMgr::toString(reqDoc, "createTime");
		// 是否已读
		int isRead = RapidJsonMgr::toInt(reqDoc, "isRead");
		// 当前会话Uid
		const std::string& myUid = session->getUserInfoUid();

		if (senderUid != myUid) // 如果发送消息在Login前进行，就会myUid为空触发乱序防御
		{
			std::cerr << "非法Uid: " << "SenderUid: " << senderUid << ", MyUid: " << myUid <<std::endl;
			RapidJsonMgr::addMember(rspDoc, "error", ErrorCodes::UidInvalid);
			RapidJsonMgr::addMember(rspDoc, "clientMsgId", clientMsgId);
			RapidJsonMgr::addMember(rspDoc, "seqId", seqId);
			return;
		}

		// Redis 幂等性校验 (去重防刷核心)
		auto& redis = RedisMgr::getMsgInfoRedis();
		std::string redisKey = "ClientMsgId:" + clientMsgId;
		std::string msgId;
		sw::redis::OptionalString optionalString = redis.get(redisKey);
		if (optionalString)
		{
			// 更新Redis过期时间
			redis.set(redisKey, msgId, std::chrono::seconds(600));

			// 客户端的超时重发
			std::cout << "[去重拦截] 收到重复的消息请求: " << clientMsgId << std::endl;

			// 从 Redis 的 Value 中解析出之前生成的 msgId
			msgId = optionalString.value();

			// 再次给发送方回执 ACK，不转发，不写库
			RapidJsonMgr::addMember(rspDoc, "error", ErrorCodes::Success);
			RapidJsonMgr::addMember(rspDoc, "clientMsgId", clientMsgId);
			RapidJsonMgr::addMember(rspDoc, "msgId", msgId);
			RapidJsonMgr::addMember(rspDoc, "seqId", seqId);
			return;
		}

		// 首次接收消息
		msgId = std::to_string(_snowflake->nextId());
		redis.set(redisKey, msgId, std::chrono::seconds(600));
		RapidJsonMgr::addMember(rspDoc, "error", ErrorCodes::Success);
		RapidJsonMgr::addMember(rspDoc, "clientMsgId", clientMsgId);
		RapidJsonMgr::addMember(rspDoc, "msgId", msgId);
		RapidJsonMgr::addMember(rspDoc, "seqId", seqId); // 告诉客户端哪条消息确认送达了
		RapidJsonMgr::addMember(rspDoc, "version", msgId);
		RapidJsonMgr::addMember(rspDoc, "peerUid", targetUid);
		RapidJsonMgr::addMember(rspDoc, "sessionType", sessionType);

		// 辅助数据
		std::string sessionTypeStr = std::to_string(sessionType);
		double zsetScore = static_cast<double>(std::stoull(msgId) >> 12);
		std::string targetPeerConvId = myUid + ":" + sessionTypeStr;
		std::string senderPeerConvId = targetUid + ":" + sessionTypeStr;

		// Redis 实时会话状态更新 (ZSET + HASH)
		auto& convRedis = RedisMgr::getConvInfoRedis();

		// 是否为自聊 (文件传输助手/自我备忘录)
		bool isSelfChat = (myUid == targetUid);

		// 更新接收方 (Target) 的会话状态
		if (!isSelfChat)
		{
			std::string targetZsetKey = "conv_zset:" + targetUid;
			std::string targetInfoKey = std::string("conv_info:").append(targetUid).append(":").append(targetPeerConvId);
			if (convRedis.exists(targetInfoKey))
			{
				auto pipe = convRedis.pipeline();
				pipe.zadd(targetZsetKey, targetPeerConvId, zsetScore);
				pipe.zremrangebyrank(targetZsetKey, 0, -1001);
				pipe.hincrby(targetInfoKey, "unread_count", 1);
				pipe.hincrby(targetInfoKey, "last_seq_id", 1);
				pipe.hset(targetInfoKey, "last_msg_content", msgData);
				pipe.hset(targetInfoKey, "last_msg_sender_uid", senderUid);
				pipe.hset(targetInfoKey, "last_msg_type", std::to_string(msgType));
				pipe.hset(targetInfoKey, "last_msg_id", msgId);
				pipe.hset(targetInfoKey, "version", msgId);	// 精确版本号
				pipe.hset(targetInfoKey, "update_time", createTime);
				pipe.expire(targetZsetKey, std::chrono::hours(24));
				pipe.expire(targetInfoKey, std::chrono::hours(24));
				pipe.exec();
			}
		}

		// 更新发送方 (Sender) 的会话状态
		std::string senderZsetKey = "conv_zset:" + myUid;
		std::string senderInfoKey = std::string("conv_info:").append(myUid).append(":").append(senderPeerConvId);
		if (convRedis.exists(senderInfoKey))
		{
			auto pipe = convRedis.pipeline();
			pipe.zadd(senderZsetKey, senderPeerConvId, zsetScore);
			pipe.zremrangebyrank(senderZsetKey, 0, -1001);
			pipe.hincrby(senderInfoKey , "last_seq_id", 1);
			pipe.hset(senderInfoKey, "last_msg_content", msgData);
			pipe.hset(senderInfoKey, "last_msg_sender_uid", senderUid);
			pipe.hset(senderInfoKey, "last_msg_type", std::to_string(msgType));
			pipe.hset(senderInfoKey, "last_msg_id", msgId);
			pipe.hset(senderInfoKey, "version", msgId);	// 精确版本号
			pipe.hset(senderInfoKey, "update_time", createTime);
			pipe.expire(senderZsetKey, std::chrono::hours(24));
			pipe.expire(senderInfoKey, std::chrono::hours(24));
			pipe.exec();
		}

		// 创建通知Json包
		rapidjson::Document notifyDoc = RapidJsonMgr::createDocument();
		RapidJsonMgr::addMember(notifyDoc, "clientMsgId", clientMsgId);
		RapidJsonMgr::addMember(notifyDoc, "msgId", msgId);
		RapidJsonMgr::addMember(notifyDoc, "senderUid", myUid);
		RapidJsonMgr::addMember(notifyDoc, "targetUid", targetUid);
		RapidJsonMgr::addMember(notifyDoc, "sessionType", sessionType);
		RapidJsonMgr::addMember(notifyDoc, "msgData", msgData);
		RapidJsonMgr::addMember(notifyDoc, "msgType", msgType);
		RapidJsonMgr::addMember(notifyDoc, "seqId", seqId); // 把序列号原封不动传给接收方
		RapidJsonMgr::addMember(notifyDoc, "createTime", createTime);
		RapidJsonMgr::addMember(notifyDoc, "isRead", isRead);

		const std::string& jsonStr = RapidJsonMgr::toJsonString(notifyDoc, false);

		// 通过 RouteGrpcClient 瞬间扔给 RouteServer (非阻塞)
		chatToTargetUser(targetUid, jsonStr, ReqId::ID_CHAT_MSG_NOTIFY);

		// 第一版
		// boost::asio::post(_dbPool, [myUid, targetUid, msgData, seqId]
		// {
		// 	bool success = MysqlMgr::getInstance()->insertChatMessage(myUid, targetUid, msgData, seqId);
		// 	if (!success)
		// 	{
		// 		// TODO:如果插入失败，可以将其写入本地日志，后续用脚本补偿重试
		// 		std::cerr << "Failed to save msg " << seqId << " to DB from " << myUid << " to " << targetUid << std::endl;
		// 	}
		// });

		// 第二版，接入RabbitMQ完成Mysql的异步读写
		RabbitMQClient::getInstance()->publish("chat.msg.save", jsonStr);
	};
	_handlers[ID_CHAT_MSG_NOTIFY_ACK] = [this](std::shared_ptr<LogicNode> logicNode)
	{
		// 目前服务器采用 Push + Pull 结合策略，不维护内存重传队列
		// 所以收到客户端的送达回执后，暂时不需要做复杂的内存消除逻辑
		// 未来如果需要实现类似 WhatsApp 的 "消息已送达 (Deliver)" 双勾状态，可在此处投递 MQ 异步更新状态表
		std::cout << "收到接收方的ACK信息" << std::endl;
		// TODO: 后期增加Mysql的chat_history的send_status字段，并在NOTIFY_ACK后再设置为一
		// TODO: ID_CHAT_MSG_REQ只能代表服务器成功接收发送方消息，避免服务器突然崩溃导致发送方误以为服务器成功发出，需要send_status字段

		// std::cout << "[QoS] Client explicitly received msg_id: " << msgId << std::endl;
	};
	_handlers[ID_HEARTBEAT_REQ] = [this](std::shared_ptr<LogicNode> logicNode) {
		assert(logicNode->_curSession.lock());
		auto session = logicNode->_curSession.lock();
		rapidjson::Document reqDoc = RapidJsonMgr::parseJson(logicNode->_curMsgNode->_data);
		assert(!reqDoc.HasMember("parse_error"));

		// 心跳包什么业务逻辑都不用做，asyncReadHead中已经updateActiveTime()
		// 为了测延迟，可以直接回一个空包过去
		Defer defer([session]
		{
			auto msg = std::make_shared<MsgNode>();
			msg->_data = "{}";
			msg->_id = static_cast<uint16_t>(ID_HEARTBEAT_RSP);
			msg->_len = static_cast<uint16_t>(msg->_data.size());
			session->postSend(msg);
		});
	};

	_handlers[ID_SYNC_INIT_REQ]= [this](std::shared_ptr<LogicNode> logicNode)
	{
		assert(logicNode->_curSession.lock());
		auto session = logicNode->_curSession.lock();
		rapidjson::Document reqDoc = RapidJsonMgr::parseJson(logicNode->_curMsgNode->_data);
		assert(!reqDoc.HasMember("parse_error"));

		const std::string& myUid = session->getUserInfoUid();

		// 客户端传上来的本地最大雪花 version
		uint64_t clientFriendVer = std::stoull(RapidJsonMgr::toString(reqDoc, "friendVersion"));
		uint64_t clientConvVer = std::stoull(RapidJsonMgr::toString(reqDoc, "convVersion"));
		uint64_t clientApplyVer = std::stoull(RapidJsonMgr::toString(reqDoc, "friendApplyVersion"));

		// 将高 IO 的查库操作抛入数据库专属线程池
		boost::asio::post(_dbPool, [this, session, myUid, clientConvVer, clientFriendVer, clientApplyVer]
		{
			try
			{
				// 直穿 MySQL 拉取增量 (绝对的强一致性，不查 Redis)
				auto deltaApplies = MysqlMgr::getInstance()->getSyncFriendApplies(myUid, clientApplyVer);
				auto deltaFriends = MysqlMgr::getInstance()->getSyncFriends(myUid, clientFriendVer);
				auto deltaConvs = MysqlMgr::getInstance()->getSyncConversations(myUid, clientConvVer);

				std::cout << "[Sync] 直连 MySQL 拉取完毕 | Applies: " << deltaApplies.size()
						  << " | Friends: " << deltaFriends.size()
						  << " | Convs: " << deltaConvs.size() << std::endl;

				// 组合增量数据
				boost::asio::post(_dbPool, [this, myUid, session,
					deltaApplies = std::move(deltaApplies),
					deltaFriends = std::move(deltaFriends),
					deltaConvs = std::move(deltaConvs)]() mutable
				{
					try
					{
						rapidjson::Document rspDoc = RapidJsonMgr::createDocument();
						auto& allocator = rspDoc.GetAllocator();
						RapidJsonMgr::addMember(rspDoc, "error", ErrorCodes::Success);

						// 挂载 FriendApply 增量数组
						rapidjson::Value applyArray(rapidjson::kArrayType);
						for (const auto& apply : deltaApplies)
						{
							rapidjson::Value applyObj = RapidJsonMgr::createObject();
							applyObj.AddMember("fromUid", rapidjson::Value(apply.fromUid.data(), apply.fromUid.size(), allocator), allocator);
							applyObj.AddMember("toUid", rapidjson::Value(apply.toUid.data(), apply.toUid.size(), allocator), allocator);
							applyObj.AddMember("status", rapidjson::Value(apply.status), allocator);
							applyObj.AddMember("greeting", rapidjson::Value(apply.greeting.data(), apply.greeting.size(), allocator), allocator);
							applyObj.AddMember("applyTime", rapidjson::Value(apply.applyTime.data(), apply.applyTime.size(), allocator), allocator);
							applyObj.AddMember("handleTime", rapidjson::Value(apply.handleTime.data(), apply.handleTime.size(), allocator), allocator);
							applyObj.AddMember("applyVersion", rapidjson::Value(apply.version.data(), apply.version.size(), allocator), allocator);

							std::string targetProfileUid = (myUid == apply.toUid) ? apply.fromUid : apply.toUid;
							auto profile = getUserProfile(targetProfileUid);
							if (profile) {
								applyObj.AddMember("targetUsername", rapidjson::Value(profile->username.data(), profile->username.size(), allocator), allocator);
								applyObj.AddMember("targetEmail", rapidjson::Value(profile->email.data(), profile->email.size(), allocator), allocator);
								applyObj.AddMember("targetVersion", rapidjson::Value(profile->version.data(), profile->version.size(), allocator), allocator);
							}
							applyArray.PushBack(applyObj, allocator);
						}
						rspDoc.AddMember("deltaApplies", applyArray, allocator);

						// 挂载 Friend 增量数组
						rapidjson::Value friendArray(rapidjson::kArrayType);
						for (const auto& f: deltaFriends)
						{
							rapidjson::Value obj = RapidJsonMgr::createObject();
							obj.AddMember("uid", rapidjson::Value(f._uid.data(), f._uid.size(), allocator), allocator);
							obj.AddMember("username", rapidjson::Value(f._username.data(), f._username.size(), allocator), allocator);
							obj.AddMember("email", rapidjson::Value(f._email.data(), f._email.size(), allocator), allocator);
							obj.AddMember("avatarUrl", rapidjson::Value(f._avatarUrl.data(), f._avatarUrl.size(), allocator), allocator);
							obj.AddMember("status", rapidjson::Value(f._status), allocator);
							obj.AddMember("version", rapidjson::Value(f._version.data(), f._version.size(), allocator), allocator);
							obj.AddMember("createTime", rapidjson::Value(f._createTime.data(), f._createTime.size(), allocator), allocator);
							friendArray.PushBack(obj, allocator);
						}
						rspDoc.AddMember("deltaFriends", friendArray, allocator);

						// 挂载 Conversation 增量数组
						rapidjson::Value convArray(rapidjson::kArrayType);
						for (const auto& c : deltaConvs) {
							rapidjson::Value obj = RapidJsonMgr::createObject();
							obj.AddMember("peerUid", rapidjson::Value(c.peerUid.data(), c.peerUid.size(), allocator), allocator);
							obj.AddMember("sessionType", rapidjson::Value(c.sessionType), allocator);
							obj.AddMember("lastMsgSenderUid", rapidjson::Value(c.lastMsgSenderUid.data(), c.lastMsgSenderUid.size(),allocator), allocator);
							obj.AddMember("lastMsgContent", rapidjson::Value(c.lastMsgContent.data(), c.lastMsgContent.size(), allocator), allocator);
							obj.AddMember("lastMsgType", rapidjson::Value(c.lastMsgType), allocator);
							obj.AddMember("lastSeqId", rapidjson::Value(c.lastSeqId.data(), c.lastSeqId.size(), allocator), allocator);
							obj.AddMember("unreadCount", rapidjson::Value(c.unreadCount), allocator);
							obj.AddMember("version", rapidjson::Value(c.version.data(), c.version.size(), allocator), allocator);
							obj.AddMember("updateTime", rapidjson::Value(c.updateTime.data(), c.updateTime.size(), allocator), allocator);
							obj.AddMember("lastMsgId", rapidjson::Value(c.lastMsgId.data(), c.lastMsgId.size(), allocator), allocator);
							convArray.PushBack(obj, allocator);
						}
						rspDoc.AddMember("deltaConvs", convArray, allocator);

						// 发送回包
						auto msg = std::make_shared<MsgNode>();
						msg->_data = RapidJsonMgr::toJsonString(rspDoc);
						msg->_id = static_cast<uint16_t>(ID_SYNC_INIT_RSP);
						msg->_len = static_cast<uint16_t>(msg->_data.size());
						session->postSend(msg);
					}
					catch (std::exception& e)
					{
						std::cerr << "[Sync] 同步数据组装 JSON 出错: " << e.what() << std::endl;
					}
				});

			}
			catch (const std::exception& e)
			{
				std::cerr << "[Sync] 数据库直查拉取增量出错: " << e.what() << std::endl;
				throw;
			}
		});
	};

	_handlers[ID_CHAT_MSG_READ_REQ] = [this](std::shared_ptr<LogicNode> logicNode)
	{
		try
		{
			assert(logicNode->_curSession.lock());
			auto session = logicNode->_curSession.lock();
			rapidjson::Document reqDoc = RapidJsonMgr::parseJson(logicNode->_curMsgNode->_data);
			assert(!reqDoc.HasMember("parse_error"));

			std::string peerUid = RapidJsonMgr::toString(reqDoc, "peerUid");
			int sessionType = RapidJsonMgr::toInt(reqDoc, "sessionType");
			std::string myUid = session->getUserInfoUid();

			std::string sessionTypeStr = std::to_string(sessionType);
			std::string myConvId = peerUid + ":" + sessionTypeStr;
			std::string myInfoKey = "conv_info:" + myUid + ":" + myConvId;

			boost::asio::post(_dbPool, [=, this] {
				try
				{
					auto& convRedis = RedisMgr::getConvInfoRedis();
					if (convRedis.exists(myInfoKey)) {
						// 直接将 unread_count 覆盖为 0
						auto pipe = convRedis.pipeline();
						pipe.hset(myInfoKey, "unread_count", "0");
						pipe.expire(myInfoKey, std::chrono::hours(24));
						pipe.exec();
					}

					// 更新chat_history表
					MysqlMgr::getInstance()->updateAllMsgReadStatus(peerUid, myUid, sessionType);

					// 更新conversation表
					MysqlMgr::getInstance()->clearConvUnreadCount(myUid, peerUid, sessionType);

					boost::asio::post(_threadPool, [=, this]
					{
						try
						{
							// 组装 RSP 给自己 (ACK)
							rapidjson::Document rspDoc = RapidJsonMgr::createDocument();
							RapidJsonMgr::addMember(rspDoc, "error", ErrorCodes::Success);
							RapidJsonMgr::addMember(rspDoc, "peerUid", peerUid);
							RapidJsonMgr::addMember(rspDoc, "sessionType", sessionType);
							// 给发送方回包
							auto msg = std::make_shared<MsgNode>();
							msg->_data = RapidJsonMgr::toJsonString(rspDoc);
							msg->_id = static_cast<uint16_t>(ID_CHAT_MSG_READ_RSP);
							msg->_len = static_cast<uint16_t>(msg->_data.size());
							session->postSend(msg);

							// 组装透传包，通知对方：“你发给我的消息，我已经全看了” (对方 UI 上的已送达变成已读)
							rapidjson::Document notifyDoc = RapidJsonMgr::createDocument();
							RapidJsonMgr::addMember(notifyDoc, "readerUid", myUid);
							RapidJsonMgr::addMember(notifyDoc, "sessionType", sessionType);
							chatToTargetUser(peerUid, RapidJsonMgr::toJsonString(notifyDoc, false), ID_CHAT_MSG_READ_NOTIFY);
						}
						catch (const std::exception& e)
						{
							std::cerr << "ID_CHAT_MSG_READ_REQ第三层出错： " << e.what() << std::endl;
						}
					});
				}
				catch (std::exception& e)
				{
					std::cerr << "ID_CHAT_MSG_READ_REQ第二层出错："  << e.what() << std::endl;
				}

			});
		}
		catch (const std::exception& e)
		{
			std::cerr << "ID_CHAT_MSG_READ_REQ第一层出错: " << e.what() << std::endl;
		}
	};

	_handlers[ID_SYNC_MSG_REQ] = [this](std::shared_ptr<LogicNode> logicNode)
	{
		try
		{
			auto session = logicNode->_curSession.lock();
			if (!session)
			{
				std::cerr << "_curSession.lock失败，目标CSession为空" << std::endl;
				return;
			}

			rapidjson::Document reqDoc = RapidJsonMgr::parseJson(logicNode->_curMsgNode->_data);
			if (reqDoc.HasMember("parse_error"))
			{
				std::cerr << "parse失败，目标JSON格式错误" << std::endl;
				return;
			}

			const std::string& peerUid = RapidJsonMgr::toString(reqDoc, "peerUid");
			int sessionType = RapidJsonMgr::toInt(reqDoc, "sessionType");
			const std::string& startVersion = RapidJsonMgr::toString(reqDoc, "startVersion");
			const std::string& endVersion = RapidJsonMgr::toString(reqDoc, "endVersion");
			const std::string& myUid = session->getUserInfoUid();

			std::cout << "[Sync_Debug] 收到历史拉取请求，Peer: " << peerUid
			<< ", 要求的起步版本号: " << startVersion
			<< ", 要求的结尾版本号: " << endVersion
			<< std::endl;

			boost::asio::post(_dbPool, [session, myUid, peerUid, sessionType, startVersion, endVersion]
			{
				try
				{
					int limit = 20; // 每次拉取 20 条
					auto msgList = MysqlMgr::getInstance()->getSyncMessages(peerUid, myUid, sessionType, startVersion, endVersion, limit);
					std::cout << "拉取的数据条目: " << msgList.size() << std::endl;
					std::cout << "对方Uid: " << peerUid  << " " << "我方Uid: " << myUid << std::endl;

					rapidjson::Document rspDoc = RapidJsonMgr::createDocument();
					rapidjson::Document::AllocatorType& allocator = rspDoc.GetAllocator();
					RapidJsonMgr::addMember(rspDoc, "error", ErrorCodes::Success);
					RapidJsonMgr::addMember(rspDoc, "peerUid", peerUid);
					RapidJsonMgr::addMember(rspDoc, "sessionType", sessionType);
					RapidJsonMgr::addMember(rspDoc, "hasMore", msgList.size() < limit ? 0 : 1);
					rapidjson::Value msgArray(rapidjson::kArrayType);
					for (const auto& msg : msgList)
					{
						rapidjson::Value msgObj = RapidJsonMgr::createObject();
						msgObj.AddMember("clientMsgId", rapidjson::Value(msg.clientMsgId.data(), msg.clientMsgId.size(),allocator), allocator);
						msgObj.AddMember("msgId", rapidjson::Value(msg.msgId.data(), msg.msgId.size(), allocator), allocator);
						msgObj.AddMember("seqId", rapidjson::Value(msg.seqId.data(), msg.seqId.size(), allocator), allocator);
						msgObj.AddMember("createTime", rapidjson::Value(msg.createTime.data(), msg.createTime.size(), allocator), allocator);
						msgObj.AddMember("peerUid", rapidjson::Value(msg.peerUid.data(), msg.peerUid.size(), allocator), allocator);
						msgObj.AddMember("sessionType", rapidjson::Value(msg.sessionType), allocator);
						msgObj.AddMember("senderUid", rapidjson::Value(msg.senderUid.data(), msg.senderUid.size(), allocator), allocator);
						msgObj.AddMember("msgData", rapidjson::Value(msg.msgData.data(), msg.msgData.size(), allocator), allocator);
						msgObj.AddMember("msgType", rapidjson::Value(msg.msgType), allocator);
						msgObj.AddMember("isRead", rapidjson::Value(msg.isRead), allocator);
						msgArray.PushBack(msgObj, allocator);
					}

					rspDoc.AddMember("msgList", msgArray, allocator);
					//  将数据发回给客户端
					auto rspMsg = std::make_shared<MsgNode>();
					rspMsg->_data = RapidJsonMgr::toJsonString(rspDoc, false);
					rspMsg->_id = static_cast<uint16_t>(ID_SYNC_MSG_RSP);
					rspMsg->_len = static_cast<uint16_t>(rspMsg->_data.size());

					session->postSend(rspMsg);
				}
				catch (const std::exception& e)
				{
					std::cerr << "ID_SYNC_MSG_REQ的第二层出错： " << e.what() << std::endl;
				}

			});
		}
		catch (const std::exception& e)
		{
			std::cerr << "ID_SYNC_MSG_REQ的第一层出错：: " << e.what() << std::endl;
		}
	};

	_handlers[ID_MSG_REPAIR_REQ] = [this](std::shared_ptr<LogicNode> logicNode)
	{
		auto session = logicNode->_curSession.lock();
		if (!session)
		{
			std::cerr << "_curSession.lock失败，目标CSession为空" << std::endl;
			return;
		}

		rapidjson::Document reqDoc = RapidJsonMgr::parseJson(logicNode->_curMsgNode->_data);
		if (reqDoc.HasMember("parse_error"))
		{
			std::cerr << "parse失败，目标JSON格式错误" << std::endl;
			return;
		}

		// 当功能稳定且正常后删除下面操作，酌情删除下面操作增加并发能力
		{
			std::scoped_lock<std::mutex> lock(_repairMonitorMutex);
			auto now = std::chrono::steady_clock::now();
			auto duration = std::chrono::duration_cast<std::chrono::seconds>(now - _lastRepairCheckTime).count();
			if (duration < 60) // 1分钟内
			{
				if (++_repairCount > 100) // 单机每分钟超过100次补洞视为系统异常
				{
					std::cerr << "[CRITICAL] 消息修补请求过快！可能存在逻辑空洞，UID: " << session->getUserInfoUid() << std::endl;
					// 在测试环境下可以通过 throw 快速锁定
					throw std::runtime_error("Message Repair Storm Detected!");
				}
			}
			else
			{
				_repairCount = 1;
				_lastRepairCheckTime = now;
			}
		}

		// 正常解析
		rapidjson::Document doc = RapidJsonMgr::parseJson(logicNode->_curMsgNode->_data);
		std::string peerUid = RapidJsonMgr::toString(doc, "peerUid");
		int sessionType = RapidJsonMgr::toInt(doc, "sessionType");
		uint64_t fromSeq = std::stoull(RapidJsonMgr::toString(doc, "fromSeq"));
		uint64_t toSeq = std::stoull(RapidJsonMgr::toString(doc, "toSeq"));
		std::string myUid = session->getUserInfoUid();

		auto startTime = std::chrono::steady_clock::now();
		std::cout << "[Repair] 收到补洞请求 | UID: " << myUid << " | Peer: " << peerUid << " | Range: ["
				  << fromSeq << "," << toSeq << "]" << std::endl;

		// 异步投入数据库线程池
        boost::asio::post(_dbPool, [=]
        {
            // 同步查库
            auto msgList = MysqlMgr::getInstance()->getRepairMessages(peerUid, myUid, sessionType, fromSeq, toSeq);

            auto endTime = std::chrono::steady_clock::now();
            auto cost = std::chrono::duration_cast<std::chrono::milliseconds>(endTime - startTime).count();

            // 判断结果
            if (msgList.empty())
            {
                // 数据库也没有，说明是“永久性空洞”，下发重置指令
                std::cout << "[Repair] 数据库无记录，下发 RESET_CMD | Cost: " << cost << "ms" << std::endl;

                rapidjson::Document resetDoc = RapidJsonMgr::createDocument();
            	RapidJsonMgr::addMember(resetDoc, "peerUid", peerUid);
                RapidJsonMgr::addMember(resetDoc, "sessionType", sessionType);
                RapidJsonMgr::addMember(resetDoc, "resetToSeq", std::to_string(toSeq + 1));

                auto msg = std::make_shared<MsgNode>();
                msg->_data = RapidJsonMgr::toJsonString(resetDoc);
                msg->_id = static_cast<uint16_t>(ID_SEQ_RESET_CMD);
                msg->_len = static_cast<uint16_t>(msg->_data.size());
                session->postSend(msg);
            }
            else
            {
                // 组装数据并回包
                std::cout << "[Repair] 补洞成功，找到 " << msgList.size() << " 条消息 | Cost: " << cost << "ms" << std::endl;

                rapidjson::Document rspDoc = RapidJsonMgr::createDocument();
                auto& allocator = rspDoc.GetAllocator();
                RapidJsonMgr::addMember(rspDoc, "error", ErrorCodes::Success);

                rapidjson::Value msgArray(rapidjson::kArrayType);
                for (const auto& msg : msgList) {
                    rapidjson::Value obj = RapidJsonMgr::createObject();
                    obj.AddMember("msgId", rapidjson::Value(msg.msgId.data(), msg.msgId.size(),allocator), allocator);
                    obj.AddMember("seqId", rapidjson::Value(msg.seqId.data(), msg.seqId.size(), allocator), allocator);
                	obj.AddMember("createTime", rapidjson::Value(msg.createTime.data(), msg.createTime.size(), allocator), allocator);
                    obj.AddMember("msgData", rapidjson::Value(msg.msgData.data(), msg.msgData.size(), allocator), allocator);
                    obj.AddMember("senderUid", rapidjson::Value(msg.senderUid.data(), msg.senderUid.size(), allocator), allocator);
                	obj.AddMember("targetUid", rapidjson::Value(msg.peerUid.data(), msg.peerUid.size(), allocator), allocator);
                	obj.AddMember("sessionType", rapidjson::Value(msg.sessionType), allocator);
                	obj.AddMember("isRead", rapidjson::Value(msg.isRead), allocator);
					obj.AddMember("msgType", rapidjson::Value(msg.msgType), allocator);
					msgArray.PushBack(obj, allocator);
                }
                rspDoc.AddMember("msgList", msgArray, allocator);

                auto rspMsg = std::make_shared<MsgNode>();
                rspMsg->_data = RapidJsonMgr::toJsonString(rspDoc, false);
                rspMsg->_id = static_cast<uint16_t>(ID_MSG_REPAIR_RSP);
                rspMsg->_len = static_cast<uint16_t>(rspMsg->_data.size());
                session->postSend(rspMsg);
            }
       });
	};
}

std::shared_ptr<UserInfo> LogicSystem::getUserProfile(const std::string& uid)
{
	auto& redis = RedisMgr::getUserInfoRedis();
	std::string cacheKey = "profile:" + uid;

	//	先查 Redis
	auto optStr = redis.get(cacheKey);
	if (optStr) {
		rapidjson::Document doc = RapidJsonMgr::parseJson(optStr.value());
		auto user = std::make_shared<UserInfo>();
		user->uid = uid;
		user->username = RapidJsonMgr::toString(doc, "username");
		user->email = RapidJsonMgr::toString(doc, "email");
		user->version = RapidJsonMgr::toString(doc, "version");
		return user;
	}

	// 缓存击穿，查 MySQL
	auto user = MysqlMgr::getInstance()->getUserInfo(uid);
	if (user) {
		// 回写 Redis，设置个 24 小时过期
		rapidjson::Document doc = RapidJsonMgr::createDocument();
		RapidJsonMgr::addMember(doc, "username", user->username);
		RapidJsonMgr::addMember(doc, "email", user->email);
		RapidJsonMgr::addMember(doc, "version", user->version);
		redis.set(cacheKey, RapidJsonMgr::toJsonString(doc, false), std::chrono::hours(24));
	}
	return user;
}

// 需要交给LogicSystem的线程池，否则长时间获取不到锁，导致CSession的io线程会卡死在close()内部的操作，导致其ioc不能进行处理其他异步操作
void LogicSystem::onSessionDisconnected(const std::string& uid, const boost::asio::any_io_executor& ex, std::weak_ptr<CSession> oldSession)
{
	boost::asio::post(_threadPool, [this, uid, ex, oldSession]
	{
		std::shared_ptr<UserConnectionCtx> connectionCtx{};
		{
			std::shared_lock<std::shared_mutex> lock(_shared_mutex);
			auto iter = _users.find(uid);
			// 缓存表里没有
			if (iter == _users.end())
			{
				std::cout << "onSessionDisconnected逻辑里，缓存内未有该uid" << std::endl;
				return;
			}
			connectionCtx = iter->second;
		}
		assert(connectionCtx);
		{
			std::scoped_lock<std::shared_mutex> lock(connectionCtx->shared_mutex);
			auto old_sp = oldSession.lock();
			auto cur_sp = connectionCtx->csession.lock();
			if (old_sp == cur_sp)
			{
				std::cout << "开启用户重连" << std::endl;
				connectionCtx->curState = 2;
				connectionCtx->csession.reset();	// 清空旧的、已经失效的物理连接
				connectionCtx->reconnectTimer = std::make_shared<boost::asio::steady_timer>(ex);
				connectionCtx->reconnectTimer->expires_after(std::chrono::seconds(120));
				connectionCtx->reconnectTimer->async_wait([uid, this](const boost::system::error_code& ec)
				{
					/// 注意该计时器是由ioc来调度异步操作的，其回调执行线程是IO线程，需要再把逻辑操作扔给LogicSystem的线程池
					if (ec == boost::asio::error::operation_aborted) {
						// 被 cancel() 了！说明用户在 5 分钟内重连成功了
						std::cout << "CSession重连成功" << std::endl;
						return;
					}
					// 时间到了，没被 cancel，说明用户真的离线
					executeRealOffline(uid);
				});
				// 同步给Redis里添加TTL
				auto& redis = RedisMgr::getServerConfigRedis();
				redis.expire(uid, std::chrono::seconds(150));
			}
		}
	});
}

void LogicSystem::executeRealOffline(const std::string& uid)
{
	std::cout << "User " << uid << " 彻底离线，开始清理资源" << std::endl;
	boost::asio::post(_threadPool, [this, uid]
	{
		std::unique_lock<std::shared_mutex> lock(_shared_mutex);
		auto it = _users.find(uid);
		if (it != _users.end() && it->second->curState == 2)
		{
			// 本地删除
			_users.erase(it);
			// redis删除
			auto& redis = RedisMgr::getServerConfigRedis();
			if (auto optionalString = redis.get(uid))
			{
				rapidjson::Document doc = RapidJsonMgr::parseJson(optionalString.value());
				auto& cfg = ConfigMgr::getInstance();
				const std::string& jsonStrServerName = RapidJsonMgr::toString(doc, "serverName");
				if (cfg["SelfServer"]["Name"] == jsonStrServerName)
				{
					redis.del(uid);
					std::cout << "[Redis] 用户 " << uid << " 的在线状态已从本服务器彻底清除。" << std::endl;
				}
				else
					std::cout << "[Redis] 用户 " << uid << " 已在其他服务器(" << jsonStrServerName << ")登录，跳过删除。" << std::endl;
			}
			std::cout << "User " << uid << " offline timeout. Cleared from cache." << std::endl;
		}
	});
}

void LogicSystem::chatToTargetUser(const std::string& targetUid, const std::string& msg, int msgId)
{
	// 先查本地有无targetUid连接，有直接走本地，无走路由
	auto& serverConfigRedis = RedisMgr::getServerConfigRedis();
	auto optionalString = serverConfigRedis.get(targetUid);
	if (!optionalString)
	{
		std::cout << "Target User " << targetUid << " is completely offline." << std::endl;
		return;
	}
	rapidjson::Document doc = RapidJsonMgr::parseJson(optionalString.value());
	assert(!doc.HasMember("parse_error"));
	if (RapidJsonMgr::toString(doc, "serverName") == serverName)
		deliverMsgToLocalUser(targetUid, msg, msgId);
	else
		// 调用 gRPC 客户端，发送给 RouteServer
		RouteGrpcClient::getInstance()->forwardMessage(targetUid, msg, msgId);
}

void LogicSystem::deliverMsgToLocalUser(const std::string& targetUid, const std::string& msgData, int msgId)
{
	boost::asio::post(_threadPool, [targetUid, msgData, msgId, this]
	{
		std::shared_ptr<UserConnectionCtx> targetConnectionCtx{};
		{
			// 必要查找，避免在路由转发时期，用户缓存被删除
			// 安全地从本地缓存查找目标用户
			std::shared_lock<std::shared_mutex> lock(_shared_mutex);
			if (_users.contains(targetUid))
				targetConnectionCtx = _users[targetUid];
		}
		// 本地没找到，或者处于断线重连等待期
		{
			if (!targetConnectionCtx)
			{
				std::cout << "Target User " << targetUid << " disconnected locally just now." << std::endl;
				return;
			}

			{
				std::shared_lock<std::shared_mutex> lock(targetConnectionCtx->shared_mutex);
				if (targetConnectionCtx->curState != 1)
				{
					std::cout << "Target User " << targetUid << " Reconnecting or Disconnecting." << std::endl;
					return;
				}
			}
			
			// 找到目标用户的物理连接
			auto session = targetConnectionCtx->csession.lock();
			if (!session) {
				return;
			}
			auto msgNode = std::make_shared<MsgNode>();
			msgNode->_id = static_cast<uint16_t>(msgId);
			msgNode->_data = msgData;
			msgNode->_len = static_cast<uint16_t>(msgData.size());
			session->postSend(msgNode);
		}
	});
}

void LogicSystem::postMsgToQueue(std::shared_ptr<LogicNode> logicNode)
{
	if (_stop.load(std::memory_order_acquire))
	{
		std::cout << "Server is stopping, reject new message." << std::endl;
		return;
	}

	auto session = logicNode->_curSession.lock();
	if (!session)
	{
		std::cerr << "Session has stopped" << std::endl;
		return;
	}

	if (logicNode->_curMsgNode->_id == ID_CHAT_MSG_REQ)
	{
		std::hash<std::string> hasher;
		size_t strand_idx = hasher(session->getUserInfoUid()) % _strands.size();
		// 发送消息必须保证时序也一致性
		boost::asio::post(*_strands[strand_idx], [logicNode, this]
		{
			try
			{
				_handlers[ID_CHAT_MSG_REQ](logicNode);
			}
			catch (std::exception& e)
			{
				std::cerr << "_handlers[ID_CHAT_MSG_REQ]处理出错: " << e.what() << std::endl;
			}
		});
		return;
	}

	boost::asio::post(_threadPool, [logicNode, this]
	{
		try
		{
			auto ptr = logicNode->_curSession.lock();
			if (!ptr)
			{
				std::cerr << "Session has stopped" << std::endl;
				return;
			}
			uint16_t id = logicNode->_curMsgNode->_id;
			// 注意，因为_handlers是类共享资源，当前任务是可能多个CSession投递到线程池里面的，再多个任务线程执行
			// 所以_handlers需要处理数据冲突
			if (_handlers.contains(id))
				_handlers[id](logicNode);
			else
				std::cerr << "InValid ID" << std::endl;
		}
		catch (const std::exception& e)
		{
			std::cerr << "_handlers处理出错： " << e.what() << std::endl;
		}
	});
}

void LogicSystem::stop()
{
	bool excepted = false;
	if (_stop.compare_exchange_strong(excepted, true))
	{
		std::cout << "LogicSystem is stopping... waiting for pending tasks to finish." << std::endl;
		_threadPool.join();
		std::cout << "LogicSystem stopped gracefully." << std::endl;
	}
}

LogicSystem::~LogicSystem()
{
	stop();
}

// LogicNode类实现
LogicNode::LogicNode(std::weak_ptr<CSession> curSession, std::shared_ptr<MsgNode> curMsgNode) :
	_curSession(std::move(curSession)),
	_curMsgNode(curMsgNode)
{

}