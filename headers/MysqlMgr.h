#ifndef MYSQLMGR_H
#define MYSQLMGR_H

#include "../headers/const.h"
#include "Singleton.h"
#include "MysqlDao.h"

class MysqlMgr: public Singleton<MysqlMgr>
{
	friend class Singleton<MysqlMgr>;
public:
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

	std::vector<MsgInfo> getRepairMessages(const std::string& peerUid, const std::string& myUid, int sessionType, uint64_t fromSeq, uint64_t toSeq);

private:
	MysqlMgr() = default;

	MysqlDao _dao;
};

#endif //MYSQLMGR_H
