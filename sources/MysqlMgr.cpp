#include "../headers/MysqlMgr.h"

std::optional<uint64_t> MysqlMgr::registerUser(const std::string& user, const std::string& password, const std::string& email)
{
	return _dao.registerUser(user, password, email);
}

bool MysqlMgr::checkEmail(const std::string& user, const std::string& email)
{
	return _dao.checkEmail(user, email);
}

bool MysqlMgr::checkPwd(const std::string& user, const std::string& password, UserInfo& userInfo)
{
	return _dao.checkPwd(user, password, userInfo);
}

bool MysqlMgr::updatePassword(const std::string& email, const std::string& password)
{
	return _dao.updatePassword(email, password);
}

std::shared_ptr<UserInfo> MysqlMgr::getUserInfo(const std::string& uid)
{
	return _dao.getUserInfo(uid);
}

std::unordered_map<std::string, std::shared_ptr<UserInfo>> MysqlMgr::searchUserInfoList(const std::string& username, const std::string& email)
{
	return _dao.searchUserInfoList(username, email);
}

bool MysqlMgr::upsertFriendApply(const std::string& fromUid, const std::string& toUid, int status,
	const std::string& greeting, const std::string& applyTime, const std::string& handleTime, uint64_t version)
{
	return _dao.upsertFriendApply(fromUid, toUid, status, greeting, applyTime, handleTime, version);
}

bool MysqlMgr::insertChatMessage(const std::string& senderUid, const std::string& targetUid, const std::string& msgData,
                                 const std::string& seq)
{
	return _dao.insertChatMessage(senderUid, targetUid, msgData, seq);
}

std::vector<FriendInfo> MysqlMgr::getFullFriends(const std::string& myUid)
{
	return _dao.getFullFriends(myUid);
}

std::vector<ConversationInfo> MysqlMgr::getAllConversations(const std::string& myUid)
{
	return _dao.getAllConversations(myUid);
}

std::vector<MsgInfo> MysqlMgr::getSyncMessages(const std::string& senderUid, const std::string& targetUid, int sessionType,
	const std::string& startVersion, const std::string& endVersion, int limit)
{
	return _dao.getSyncMessages(senderUid, targetUid, sessionType, startVersion, endVersion, limit);
}

std::shared_ptr<FriendApplyInfo> MysqlMgr::getFriendApply(const std::string& fromUid, const std::string& toUid)
{
	return _dao.getFriendApply(fromUid, toUid);
}

bool MysqlMgr::updateAllMsgReadStatus(const std::string& senderUid, const std::string& targetUid, int sessionType)
{
	return _dao.updateAllMsgReadStatus(senderUid, targetUid, sessionType);
}

bool MysqlMgr::clearConvUnreadCount(const std::string& ownerUid, const std::string& peerUid, int sessionType)
{
	return _dao.clearConvUnreadCount(ownerUid, peerUid, sessionType);
}

bool MysqlMgr::callUpsertFriendApplyProc(const std::string& fromUid, const std::string& toUid, int status,
	const std::string& greeting, const std::string& applyTime, const std::string& handleTime, uint64_t& version)
{
	return _dao.callUpsertFriendApplyProc(fromUid, toUid, status, greeting, applyTime, handleTime, version);
}

bool MysqlMgr::callAuthFriendApplyProc(const std::string& fromUid, const std::string& toUid, int newStatus,
	const std::string& handleTime, uint64_t& outVersion)
{
	return _dao.callAuthFriendApplyProc(fromUid, toUid, newStatus, handleTime, outVersion);
}

bool MysqlMgr::callAddFriendProc(const std::string& selfUid, const std::string& friendUid, int status,
	const std::string& createTime, uint64_t& newFriendVersion)
{
	return _dao.callAddFriendProc(selfUid, friendUid, status, createTime, newFriendVersion);
}

std::vector<FriendApplyInfo> MysqlMgr::getSyncFriendApplies(const std::string& uid, uint64_t version)
{
	return _dao.getSyncFriendApplies(uid, version);
}

std::vector<FriendInfo> MysqlMgr::getSyncFriends(const std::string& uid, uint64_t version)
{
	return _dao.getSyncFriends(uid, version);
}

std::vector<ConversationInfo> MysqlMgr::getSyncConversations(const std::string& uid, uint64_t version)
{
	return _dao.getSyncConversations(uid, version);
}

std::vector<MsgInfo> MysqlMgr::getRepairMessages(const std::string& peerUid, const std::string& myUid, int sessionType,
	uint64_t fromSeq, uint64_t toSeq)
{
	return _dao.getRepairMessages(peerUid, myUid, sessionType, fromSeq, toSeq);
}
