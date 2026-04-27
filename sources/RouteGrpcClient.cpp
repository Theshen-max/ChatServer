#include "../headers/RouteGrpcClient.h"
#include "../headers/ConfigMgr.h"
#include "../headers/LogicSystem.h"
#include "../headers/AsioIOContextPool.h"

bool RouteGrpcClient::init()
{
	// 避免重复初始化
	std::scoped_lock<std::mutex> lock(_clientMutex);
	return do_init();
}

bool RouteGrpcClient::do_init()
{
	if (_state != RouteState::Uninited) return true;

	if (!_stub) // Channel、Stub可以复用
	{
		auto& cfg = ConfigMgr::getInstance();
		const std::string routeIp = cfg["RouteServer"]["Host"];
		const std::string routePort = cfg["RouteServer"]["Port"];
		std::shared_ptr<Channel> channel = grpc::CreateChannel(routeIp + ":" + routePort, grpc::InsecureChannelCredentials());
		_stub = RouteService::NewStub(channel);
	}

	_reconnectCount = 0;
	_currentReconnectDelay = 2000;

	// 此时仅仅是准备发起连接，状态切为 Connecting
	_state = RouteState::Connecting;
	std::cout << "[RouteGrpcClient] LogicServer 正在初始化与 RouteServer 的骨干网络..." << std::endl;

	// 发起实际的流创建
	startStream();
	return true;
}

void RouteGrpcClient::clear()
{
	std::scoped_lock<std::mutex> lock(_clientMutex);
	do_clear();
}

void RouteGrpcClient::do_clear()
{
	std::cout << "[RouteGrpcClient] 触发清理与熔断机制，状态切换为 Uninited！" << std::endl;

	_state = RouteState::Uninited;

	if (_reconnectTimer) _reconnectTimer->cancel();
	if (_context) _context->TryCancel();
	_reactor = nullptr;

	std::queue<RoutePackage> emptyQueue;
	_pendingQueue.swap(emptyQueue);
}

void RouteGrpcClient::startStream()
{
	// 上下文、reactor必须全新
	_context = std::make_unique<grpc::ClientContext>();
	_reactor = new RouteGrpcClientReactor(_stub.get(), _context.get());

	// 发起握手链接
	auto& cfg = ConfigMgr::getInstance();
	RoutePackage handshakePkg;
	handshakePkg.set_serverid(cfg["SelfServer"]["Name"]);
	_reactor->postSend(handshakePkg);

	// 打印日志
	std::cout << "Connected to RouteServer, Handshake sent as: " << cfg["SelfServer"]["Name"] << std::endl;
}

void RouteGrpcClient::onStreamConnected()
{
	std::scoped_lock<std::mutex> lock(_clientMutex);
	_state = RouteState::Connected;
	std::cout << "[RouteGrpcClient] 骨干网握手成功 (Connected)！开始清空暂存队列..." << std::endl;

	// 倾泻所有在 Connecting 期间暂存的消息
	while (!_pendingQueue.empty()) {
		if (_reactor) {
			_reactor->postSend(_pendingQueue.front());
		}
		_pendingQueue.pop();
	}
}

void RouteGrpcClient::onStreamClosed(RouteGrpcClientReactor* reactor)
{
	std::scoped_lock<std::mutex> lock(_clientMutex);

	if (_reactor != reactor) return;

	// 安全置空
	_reactor = nullptr;

	// 被主动熔断，无需挣扎
	if (_state == RouteState::Uninited) return;

	_state = RouteState::Connecting;
	_reconnectCount++;
	if (_reconnectCount > MAX_RECONNECT_COUNT)
	{
		std::cerr << "[RouteGrpcClient] FATAL: 骨干网重连次数达到上限，触发彻底熔断！" << std::endl;
		do_clear();
		return;
	}

	// 懒加载
	if (!_reconnectTimer)
	{
		auto& ioc = AsioIoContextPool::getInstance()->getNextIoContext();
		_reconnectTimer = std::make_unique<boost::asio::steady_timer>(ioc);
	}

	std::cout << "[RouteGrpcClient] 骨干网断开，准备第 " << _reconnectCount << " 次重连..." << std::endl;

	_reconnectTimer->expires_after(std::chrono::milliseconds(_currentReconnectDelay));
	_reconnectTimer->async_wait([this](const boost::system::error_code& ec)
	{
		std::scoped_lock<std::mutex> lock(_clientMutex);
		if (!ec && _state == RouteState::Connecting) {
			startStream();
		}
	});

	_currentReconnectDelay *= 2;
	_currentReconnectDelay = std::min(_currentReconnectDelay, MAX_RECONNECT_DELAY);
}

void RouteGrpcClient::forwardMessage(const std::string& targetUid, const std::string& msgData, int msgId)
{
	// 组装数据
	RoutePackage pkg;
	pkg.set_targetuid(targetUid);
	pkg.set_jsondata(msgData);
	pkg.set_msgid(msgId);

	{
		std::scoped_lock<std::mutex> lock(_clientMutex);
		if (_state == RouteState::Uninited)
		{
			std::cout << "[RouteGrpcClient] 处于熔断状态，懒加载唤醒 Init()..." << std::endl;
			do_init();
		}

		if (_state == RouteState::Connecting)
		{
			if (_pendingQueue.size() >= MAX_PENDING_SIZE)
				_pendingQueue.pop(); // 弹出最老的数据
			_pendingQueue.emplace(pkg);
			return;
		}

		if (_reactor) {
			_reactor->postSend(pkg);
		}
	}
}



// RouteGrpcClientReactor 的实现
RouteGrpcClientReactor::RouteGrpcClientReactor(RouteService::Stub* stub, ClientContext* ctx):
	_isWriting(false),
	_isHandshakeDone(false)
{
	stub->async()->RouteMessageStream(ctx, this);

	StartRead(&_recvMsg);

	StartCall();	// 启动 gRPC 客户端的流生命周期
}

void RouteGrpcClientReactor::postSend(const RoutePackage& package)
{
	std::scoped_lock<std::mutex> lock(_mutex);
	_sendQueue.emplace(package);

	if (!_isWriting)
	{
		_isWriting = true;
		_sendMsg = _sendQueue.front();
		StartWrite(&_sendMsg);
	}
}

void RouteGrpcClientReactor::OnReadDone(bool ok)
{
	if (!ok) return;
	// 收到 RouteServer 下发的路由消息！
	std::cout << "[RouteGrpcClient] Received message for: " << _recvMsg.targetuid() << std::endl;

	// 检查是否有业务错误码（比如对方离线，退回来的信）
	if (_recvMsg.error() != ErrorCodes::Success)
	{
		std::cerr << "Route Error Code: " << _recvMsg.error() << std::endl;
	}
	else
		LogicSystem::getInstance()->deliverMsgToLocalUser(_recvMsg.targetuid(), _recvMsg.jsondata(), _recvMsg.msgid());

	_recvMsg.Clear();
	StartRead(&_recvMsg);
}

void RouteGrpcClientReactor::OnWriteDone(bool ok)
{
	bool notifyConnected = false;
	{
		std::scoped_lock<std::mutex> lock(_mutex);
		_sendQueue.pop();

		if (ok && !_isHandshakeDone) {
			_isHandshakeDone = true;
			notifyConnected = true;
		}

		if (ok && !_sendQueue.empty())
		{
			_sendMsg = _sendQueue.front();
			StartWrite(&_sendMsg);
		}
		else
			_isWriting = false;
	}

	if (notifyConnected)
		RouteGrpcClient::getInstance()->onStreamConnected();
}

void RouteGrpcClientReactor::OnDone(const grpc::Status& status)
{
	// 触发OnDone时，代表该双向流使命结束，不可复用
	std::cout << "[RouteGrpcClientReactor] Stream closed. Status: " << status.error_code() << " msg: " << status.error_message() << std::endl;

	RouteGrpcClient::getInstance()->onStreamClosed(this);

	delete this;
}