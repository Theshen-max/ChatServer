//
// Created by 27044 on 26-3-10.
//
#include "../headers/ConfigMgr.h"
#include "../headers/RedisMgr.h"

sw::redis::Redis& RedisMgr::getRedis()
{
	static sw::redis::Redis redis(initConnectionOptions(Default), initPoolOptions(Default));
	return redis;
}

sw::redis::Redis& RedisMgr::getUserInfoRedis()
{
	static sw::redis::Redis redis(initConnectionOptions(UserInfo), initPoolOptions(UserInfo));
	return redis;
}

sw::redis::Redis& RedisMgr::getMsgInfoRedis()
{
	static sw::redis::Redis redis(initConnectionOptions(MsgInfo), initPoolOptions(MsgInfo));
	return redis;
}

sw::redis::Redis& RedisMgr::getConvInfoRedis()
{
	static sw::redis::Redis redis(initConnectionOptions(ConvInfo), initPoolOptions(ConvInfo));
	return redis;
}

sw::redis::Redis& RedisMgr::getUserListRedis()
{
	static sw::redis::Redis redis(initConnectionOptions(UserList), initPoolOptions(UserList));
	return redis;
}

sw::redis::Redis& RedisMgr::getServerConfigRedis()
{
	static sw::redis::Redis redis(initConnectionOptions(ServerConfig), initPoolOptions(ServerConfig));
	return redis;
}

sw::redis::Redis& RedisMgr::getFriendApplyRedis()
{
	static sw::redis::Redis redis(initConnectionOptions(FriendApply), initPoolOptions(FriendApply));
	return redis;
}

sw::redis::ConnectionOptions RedisMgr::initConnectionOptions(RedisDB db)
{
	sw::redis::ConnectionOptions opts;

	opts.host = ConfigMgr::getInstance()["Redis"]["Host"];
	opts.port = std::stoi(ConfigMgr::getInstance()["Redis"]["Port"]);
	opts.password = ConfigMgr::getInstance()["Redis"]["Passwd"];
	opts.socket_timeout = std::chrono::milliseconds(200);	// 网络IO超时机制， 200ms
	opts.db = db;

	return opts;
}

sw::redis::ConnectionPoolOptions RedisMgr::initPoolOptions(RedisDB db)
{
	size_t connSize{};
	switch (db)
	{
		case Default: connSize = 5; break;
		case UserInfo: connSize = 5; break;
		case MsgInfo: connSize = 5; break;
		case ConvInfo: connSize = 5; break;
		case UserList: connSize = 5; break;
		case ServerConfig: connSize = 5; break;
		case FriendApply: connSize = 5; break;
		default: connSize = 5; break;
	}

	sw::redis::ConnectionPoolOptions opts;
	opts.size = connSize;
	opts.wait_timeout = std::chrono::milliseconds(100); // 单次等待最大限额，避免长时间等待空闲连接
	return opts;
}