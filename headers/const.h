#ifndef CONST_H
#define CONST_H

#include <map>
#include <unordered_map>
#include <unordered_set>
#include <functional>
#include <boost/asio.hpp>
#include <boost/beast/http.hpp>
#include <boost/beast.hpp>
#include <boost/asio/ssl.hpp>
#include <boost/asio/thread_pool.hpp>
#include <boost/asio/post.hpp>
#include <boost/uuid.hpp>
#include "openssl/sha.h"
#include "openssl/rand.h"
#include "Singleton.h"
#include <optional>
#include <queue>
#include <mutex>
#include <condition_variable>
#include <iomanip>
#include <sstream>
#include <fstream>
#include <memory>
#include <string>
#include <coroutine>

namespace beast = boost::beast;
namespace http = beast::http;
namespace net = boost::asio;
namespace ssl = boost::asio::ssl;

using tcp = boost::asio::ip::tcp;
using boost::asio::use_awaitable;
inline constexpr auto use_nothrow_awaitable = boost::asio::as_tuple(use_awaitable);
enum ErrorCodes
{
	Success = 0,
	Error_DB = 1000, // 数据库错误
	Error_Json = 1001,	// Json解析错误
	RPCFailed = 1002,	// RPC请求失败
	VarifyExpired = 1003, // 验证码过期
	VarifyCodeError = 1004, // 验证码错误
	UserExited = 1005, // 用户已经存在
	PasswdError = 1006, // 密码错误
	EmailNotMatch = 1007, // 邮箱不匹配
	PasswdUpFailed = 1008, // 密码更新失败
	PasswdInvalid = 1009, // 登录密码错误
	UidInvalid = 1010, // 非法uid
	TokenInvalid = 1011, // 非法token
	UserNotFound = 1012, // 未找到用户
	UserOffline = 1013 // 用户断线
};

enum ReqId
{
	ID_CHAT_LOGIN = 1005,
	ID_CHAT_LOGIN_RSP = 1006,
	ID_CHAT_SEND = 1007,  // 客户端发送消息
	ID_CHAT_RECEIVE = 1008, // 客户端接收消息
	ID_SEARCH_USER_REQ = 1009, // 客户端查询用户
	ID_SEARCH_USER_RSP = 1010, // 应答客户端的查询请求
	ID_ADD_FRIEND_REQ = 1011, // 客户端添加好友请求
	ID_ADD_FRIEND_RSP = 1012, // 客户端添加好友响应
	ID_ADD_FRIEND_NOTIFY = 1013, // 服务器端通知客户端
	ID_AUTH_FRIEND_REQ = 1014, // 客户端同意或拒绝请求，并通知服务器
	ID_AUTH_FRIEND_RSP = 1015, // 客户端同意或拒绝响应
	ID_AUTH_FRIEND_NOTIFY = 1016, // 服务器携带Target客户端状态通知源客户端
	ID_GET_FRIEND_LIST_REQ = 1017, // 客户端拉取好友列表的请求
	ID_GET_FRIEND_LIST_RSP = 1018,   // 客户端拉取好友列表的响应
	ID_CHAT_MSG_REQ = 1019,   // 客户端发送单聊文本消息
	ID_CHAT_MSG_RSP = 1020,   // 服务器回执（如果需要处理失败重发，暂保留）
	ID_CHAT_MSG_NOTIFY = 1021, // 服务器下发单聊文本消息给接收方
	ID_CHAT_MSG_NOTIFY_ACK = 1022,
	ID_HEARTBEAT_REQ = 1023, // 客户端心跳请求
	ID_HEARTBEAT_RSP = 1024, // 客户端心跳响应
	ID_SYNC_INIT_REQ = 1025, // 初始化同步请求
	ID_SYNC_INIT_RSP = 1026, // 初始化同步响应
	ID_CHAT_MSG_READ_REQ = 1027, // 客户端已读请求
	ID_CHAT_MSG_READ_RSP = 1028, // 客户端已读响应
	ID_CHAT_MSG_READ_NOTIFY = 1029, // 服务器已读通知
	ID_SYNC_MSG_REQ = 1030, // 客户端拉取历史记录请求
	ID_SYNC_MSG_RSP = 1031, // 客户端拉取历史记录响应
	ID_MSG_REPAIR_REQ = 1032, // 客户端补洞请求
	ID_MSG_REPAIR_RSP = 1033, // 客户端修补消息响应
	ID_SEQ_RESET_CMD = 1034,  // 服务端强制重置序列号指令
};

// 这里不能用std::string, 因为string是运行期堆内存分配的，而constexpr需要编译期确定，所以采用std::string_view
inline constexpr std::string_view CODE_PREFIX = "code_";

extern std::string serverName;

// Defer类 （延期作用，为分支结束提供抽象，避免每个分支结束都写一致的处理逻辑）（该类通过RAII实现）
class Defer
{
public:
	// 接收一个可调用对象（lambda必须是可拷贝的，因为function的要求，内部可调用对象必须可拷贝）
	explicit Defer(std::function<void()> func) : _func(std::move(func)) {}

	// 设置是否关闭defer对象, 默认启动
	void setEnabled(bool enabled) { _enabled = enabled; }

	~Defer() { if (_enabled) _func(); }
private:
	std::function<void()> _func;
	bool _enabled = true;
};

class CSession;

// 下面是“逻辑用户”
struct UserInfo
{
	std::string uid; // 用户UID
	std::string username; // 用户名字
	std::string email; // 用户邮箱
	std::string version; // 用户版本
};

struct UserConnectionCtx
{
	std::string uid;
	std::weak_ptr<CSession> csession; // 绑定最新的物理通信通道
	int curState = 1;                     // ONLINE / RECONNECTING
	std::shared_ptr<boost::asio::steady_timer> reconnectTimer; // 专属定时器
	std::shared_mutex shared_mutex; // 保护单个用户数据的私有锁
};

struct FriendInfo {
	std::string _uid;
	std::string _username;
	std::string _email;
	std::string _avatarUrl;
	int _status = 1;    // 0：拉黑或删除，1：正常
	std::string _version; // 版本号
	std::string _createTime;
};

struct FriendApplyInfo
{
	std::string fromUid;
	std::string toUid;
	int status;
	std::string greeting;
	std::string applyTime;
	std::string handleTime;
	std::string version;
};

struct ConversationInfo {
	std::string ownerUid;
	std::string peerUid;        // 对端 UID
	int sessionType = 1;    // 1-单聊，2-群聊
	std::string lastMsgSenderUid;
	std::string lastMsgContent; // 最后一条消息摘要
	int lastMsgType;
	std::string lastSeqId;      // 会话全局最新（最后）的消息序列ID
	unsigned int unreadCount = 0;    // 未读消息数
	std::string updateTime;  // 最后活跃时间
	std::string lastMsgId;
	std::string version; // 版本号
};

struct MsgInfo {
	std::string clientMsgId; // 客户端生成的 UUID (本地数据库绝对主键)
	std::string msgId;       // 服务端生成的雪花 ID (发送成功或接收时才有)
	std::string seqId;  // 消息序列号 (TCP 滑动窗口防乱序核心)
	std::string createTime; // 消息产生的时间戳 (毫秒级，分页拉取的核心游标)

	// === 路由与归属 ===
	std::string peerUid;     // 对端 UID (当前聊天窗口的对象)
	int sessionType = 1; // 1-单聊, 2-群聊 (方便后续扩展)
	std::string senderUid;   // 真正的发送者 UID

	// === 内容与状态 ===
	std::string msgData;     // 消息内容
	int msgType = 1;     // 1-文本, 2-图片等
	// int sendStatus = 1;  // 发送状态：0-发送中, 1-成功, 2-失败
	int isRead = 0;      // 0：未读， 1：已读
};
#endif //CONST_H
