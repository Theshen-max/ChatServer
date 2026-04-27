#ifndef MSGNODE_H
#define MSGNODE_H

#include "const.h"

class LogicSystem;
enum MsgType
{
	RecvMsg,
	SendMsg
};

class MsgNode
{
public:
	MsgNode();

	void clear();

	uint16_t _id;

	uint16_t _len;

	MsgType _type;

	std::string _data;
};

class SendNode
{
public:
	SendNode(std::shared_ptr<MsgNode> msg);

	auto getBuffer() -> std::vector<boost::asio::const_buffer>&;

private:
	std::vector<boost::asio::const_buffer> _dataBody;
};

#endif //MSGNODE_H
