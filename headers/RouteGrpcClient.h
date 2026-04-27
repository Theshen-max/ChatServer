#ifndef ROUTEGRPCCLIENT_H
#define ROUTEGRPCCLIENT_H

#include "const.h"
#include <grpcpp/grpcpp.h>
#include "RouteServer.grpc.pb.h"

using grpc::Channel;
using grpc::ClientContext;
using grpc::Status;
using grpc::ClientBidiReactor;
using message::RoutePackage;
using message::RouteService;

enum class RouteState
{
	Uninited,   // 未初始化 / 已彻底熔断 (Circuit Open)
	Connecting, // 正在尝试握手建联中 (TCP/HTTP2 握手期)
	Connected   // 握手成功，流已完全就绪可以发业务数据
};

class RouteGrpcClientReactor;

/// 当前运行在业务线程
class RouteGrpcClient: public Singleton<RouteGrpcClient>
{
	friend class Singleton<RouteGrpcClient>;
public:
	bool init();

	void clear();

	// 暴露给 LogicSystem 调用的安全发送接口
	void forwardMessage(const std::string& targetUid, const std::string& msgData, int msgId);

	// 供 Reactor 调用的回调
	void onStreamConnected();
	void onStreamClosed(RouteGrpcClientReactor* reactor);

private:
	RouteGrpcClient() = default;

	// 连接到 RouteServer
	void startStream();

	bool do_init();

	void do_clear();

private:
	// 双向流客户端反应器
	RouteGrpcClientReactor* _reactor = nullptr;

	// RPC客户端配置数据
	std::unique_ptr<RouteService::Stub> _stub;
	std::unique_ptr<ClientContext> _context;

	// 重连状态机变量
	std::unique_ptr<boost::asio::steady_timer> _reconnectTimer;
	int _currentReconnectDelay = 2000;
	int _reconnectCount = 0;
	constexpr static int MAX_RECONNECT_DELAY = 30000; // 最大退避 30 秒
	constexpr static int MAX_RECONNECT_COUNT = 10;

	// 并发安全与状态
	std::mutex _clientMutex; // 保护状态和缓冲队列
	RouteState _state{RouteState::Uninited};

	// 待发送缓冲区 (Pending Buffer)
	std::queue<RoutePackage> _pendingQueue;
	constexpr static size_t MAX_PENDING_SIZE = 10000; // 防止断网时内存无限膨胀，溢出则丢弃老数据
};

/// 运行在GRPC线程（内部线程池）
class RouteGrpcClientReactor: public ClientBidiReactor<RoutePackage, RoutePackage>
{
public:
	RouteGrpcClientReactor(RouteService::Stub* stub, ClientContext* ctx);

	void postSend(const RoutePackage& package);

	void OnReadDone(bool ok) override;

	void OnWriteDone(bool ok) override;

	void OnDone(const grpc::Status& status) override;

private:
	RoutePackage _recvMsg;

	RoutePackage _sendMsg;

	std::queue<RoutePackage> _sendQueue;

	std::mutex _mutex;

	bool _isWriting;

	bool _isHandshakeDone;
};

#endif //ROUTEGRPCCLIENT_H
