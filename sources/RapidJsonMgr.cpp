//
// Created by 27044 on 26-3-17.
//

#include "../headers/RapidJsonMgr.h"

#include <boost/asio/execution/allocator.hpp>


std::string RapidJsonMgr::toJsonString(const rapidjson::Document& doc, bool format)
{
	// 序列化缓冲区
	rapidjson::StringBuffer buffer;
	if (format)
	{
		// 有格式的Json字符串
		rapidjson::PrettyWriter<rapidjson::StringBuffer> writer(buffer);
		doc.Accept(writer);
	}
	else
	{
		// 无格式的Json字符串
		rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
		doc.Accept(writer);
	}
	return buffer.GetString();
}

std::string RapidJsonMgr::friendInfoToJsonString(const FriendInfo& friendInfo, bool format)
{
	rapidjson::Document doc = createDocument();
	addMember(doc, "uid", friendInfo._uid);
	addMember(doc, "username", friendInfo._username);
	addMember(doc, "email", friendInfo._email);
	addMember(doc, "avatarUrl", friendInfo._avatarUrl);
	addMember(doc, "status", friendInfo._status);
	addMember(doc, "create_time", friendInfo._createTime);
	addMember(doc, "version", friendInfo._version);

	return toJsonString(doc, format);
}

rapidjson::Document RapidJsonMgr::parseJson(const std::string& jsonStr)
{
	// 空文档对象用来存储解析后的字符串
	rapidjson::Document doc;
	if (doc.Parse(jsonStr.c_str()).HasParseError())
	{
		rapidjson::Document errDoc;
		errDoc.SetObject();
		auto& allocator = doc.GetAllocator();
		errDoc.AddMember("parse_error", rapidjson::Value("json parse error", allocator), allocator);
		return errDoc;
	}
	return doc;
}

FriendInfo RapidJsonMgr::parseFriendInfo(const std::string& jsonStr)
{
	rapidjson::Document doc = parseJson(jsonStr);
	if (doc.HasMember("parse_error")) throw std::runtime_error("parse_error");

	std::string uid = toString(doc, "uid");
	std::string username = toString(doc, "username");
	std::string email = toString(doc, "email");
	std::string avatarUrl = toString(doc, "avatarUrl");
	int status = toInt(doc, "status");
	std::string version = toString(doc, "version");
	std::string createTime = toString(doc, "create_time");
	return {uid, username, email, avatarUrl, status, version, createTime};
}

std::string RapidJsonMgr::toString(const rapidjson::Value& value, const char* key, const std::string& defaultValue)
{
	// 判断是不是对象
	if (!value.IsObject()) {
		return defaultValue;
	}

	// find查找键值 (返回的迭代器指向的是个类似pair的结构)
	auto it = value.FindMember(key);

	// 判断键存不存在
	if (it == value.MemberEnd()) {
		return defaultValue;
	}

	// 判断值类型对不对
	if (!it->value.IsString()) {
		return defaultValue;
	}

	return it->value.GetString();
}

int RapidJsonMgr::toInt(const rapidjson::Value& value, const char* key, int defaultValue)
{
	if (!value.IsObject()) return defaultValue;
	auto it = value.FindMember(key);
	if (it == value.MemberEnd()) return defaultValue;
	if (!it->value.IsInt()) return defaultValue;
	return it->value.GetInt();
}

uint64_t RapidJsonMgr::toUInt64(const rapidjson::Value& value, const char* key, uint64_t defaultValue)
{
	if (!value.IsObject()) return defaultValue;
	auto it = value.FindMember(key);
	if (it == value.MemberEnd()) return defaultValue;
	if (!it->value.IsUint64()) return defaultValue;
	return it->value.GetUint64();
}

int64_t RapidJsonMgr::toInt64(const rapidjson::Value& value, const char* key, int64_t defaultValue)
{
	if (!value.IsObject()) return defaultValue;
	auto it = value.FindMember(key);
	if (it == value.MemberEnd()) return defaultValue;
	if (!it->value.IsInt64()) return defaultValue;
	return it->value.GetInt64();
}

bool RapidJsonMgr::toBool(const rapidjson::Value& value, const char* key, bool defaultValue)
{
	if (!value.IsObject()) return defaultValue;
	auto it = value.FindMember(key);
	if (it == value.MemberEnd()) return defaultValue;
	if (!it->value.IsBool()) return defaultValue;
	return it->value.GetBool();
}

rapidjson::Document RapidJsonMgr::createDocument()
{
	rapidjson::Document doc;
	doc.SetObject();
	return doc;
}

rapidjson::Value RapidJsonMgr::createArray()
{
	return rapidjson::Value(rapidjson::kArrayType);
}

rapidjson::Value RapidJsonMgr::createObject()
{
	return rapidjson::Value(rapidjson::kObjectType);
}

void RapidJsonMgr::addMember(rapidjson::Document& doc, const char* key, int value)
{
	auto& allocator = doc.GetAllocator();
	if (doc.HasMember(key))
		doc[key].SetInt(value);
	else
		doc.AddMember(rapidjson::Value(key, allocator), rapidjson::Value(value), allocator);
}

void RapidJsonMgr::addMember(rapidjson::Document& doc, const char* key, int64_t value)
{
	auto& allocator = doc.GetAllocator();
	if (doc.HasMember(key))
		doc[key].SetInt64(value);
	else
		doc.AddMember(rapidjson::Value(key, allocator), rapidjson::Value(value), allocator);
}

void RapidJsonMgr::addMember(rapidjson::Document& doc, const char* key, uint64_t value)
{
	auto& allocator = doc.GetAllocator();
	if (doc.HasMember(key))
		doc[key].SetUint64(value);
	else
		doc.AddMember(rapidjson::Value(key, allocator), rapidjson::Value(value), allocator);
}

void RapidJsonMgr::addMember(rapidjson::Document& doc, const char* key, const std::string& value)
{
	auto& allocator = doc.GetAllocator();
	if (doc.HasMember(key))
		doc[key].SetString(value.c_str(), allocator);
	else
		doc.AddMember(rapidjson::Value(key, allocator), rapidjson::Value(value.c_str(), value.size(),allocator), allocator);
}
