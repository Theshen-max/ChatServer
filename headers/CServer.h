#ifndef CSERVER_H
#define CSERVER_H
#include "const.h"
#include "../headers/CSession.h"

class CServer: public std::enable_shared_from_this<CServer>
{
public:
	CServer(net::io_context& ctx, unsigned short port);

	void cleanSession(const std::string& uuid);	// 当Session会话无效时，从map中移除

	void startAccept();

private:
	void handleAccept(std::shared_ptr<CSession> session);

	void initSSL();

	void startHeartbeatCheck();

	void startServerReport();

private:
	net::io_context& _ioc;

	tcp::acceptor _acceptor;

	ssl::context _sslContext;

	std::unordered_map<std::string, std::shared_ptr<CSession>> _sessions;

	std::mutex _mutex;

	boost::asio::steady_timer _heartbeatTimer;

	boost::asio::steady_timer _reportTimer;
};

#endif //CSERVER_H
