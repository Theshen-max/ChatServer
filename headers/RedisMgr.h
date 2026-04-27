#ifndef REDISMGR_H
#define REDISMGR_H

#include <sw/redis++/redis++.h>

class RedisMgr
{
	enum RedisDB
	{
		Default = 0,
		UserInfo = 1,
		MsgInfo = 2,
		ConvInfo = 3,
		UserList = 4,
		FriendApply = 5,
		ServerConfig = 10
	};

public:
	static sw::redis::Redis& getRedis();

	static sw::redis::Redis& getUserInfoRedis();

	static sw::redis::Redis& getMsgInfoRedis();

	static sw::redis::Redis& getConvInfoRedis();

	static sw::redis::Redis& getUserListRedis();

	static sw::redis::Redis& getServerConfigRedis();

	static sw::redis::Redis& getFriendApplyRedis();

	RedisMgr(const RedisMgr&) = delete;

	RedisMgr& operator=(const RedisMgr&) = delete;

private:
	RedisMgr() = default;

	static sw::redis::ConnectionOptions initConnectionOptions(RedisDB db = Default);

	static sw::redis::ConnectionPoolOptions initPoolOptions(RedisDB db = Default);
};


#endif //REDISMGR_H
