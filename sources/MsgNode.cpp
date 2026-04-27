//
// Created by 27044 on 26-3-20.
//

#include "../headers/MsgNode.h"
MsgNode::MsgNode() :
	_id(0),
	_len(0),
	_type(MsgType::RecvMsg)
{

}

void MsgNode::clear()
{
	_id = 0;
	_len = 0;
	_type = MsgType::RecvMsg;
	_data = "";
}

SendNode::SendNode(std::shared_ptr<MsgNode> msg)
{
	assert(msg != nullptr);
	_dataBody.emplace_back(&msg->_id, sizeof(msg->_id));
	_dataBody.emplace_back(&msg->_len, sizeof(msg->_len));
	_dataBody.emplace_back(msg->_data.c_str(), msg->_data.size());
}

auto SendNode::getBuffer() -> std::vector<boost::asio::const_buffer>&
{
	return _dataBody;
}
