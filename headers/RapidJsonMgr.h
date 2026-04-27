//
// Created by 27044 on 26-3-17.
//

#ifndef RAPIDJSONWRAPPER_H
#define RAPIDJSONWRAPPER_H

#include "const.h"
#include <google/protobuf/stubs/port.h>
#include <rapidjson/document.h>
#include <rapidjson/writer.h>
#include <rapidjson/prettywriter.h>
#include <rapidjson/stringbuffer.h>

class RapidJsonMgr
{
public:
	static std::string toJsonString(const rapidjson::Document &doc, bool format = true);

	static std::string friendInfoToJsonString(const FriendInfo& friendInfo, bool format = true);

	static rapidjson::Document parseJson(const std::string &jsonStr);

	static FriendInfo parseFriendInfo(const std::string &jsonStr);

	static std::string toString(const rapidjson::Value& value, const char* key, const std::string& defaultValue = "");

	static int toInt(const rapidjson::Value& value, const char* key, int defaultValue = 0);

	static uint64_t toUInt64(const rapidjson::Value& value, const char* key, uint64_t defaultValue = 0);

	static int64_t toInt64(const rapidjson::Value& value, const char* key, int64_t defaultValue = 0);

	static bool toBool(const rapidjson::Value& value, const char* key, bool defaultValue = false);

	// 创建一个空的Document(顶层)
	static rapidjson::Document createDocument();

	// 创建一个数组
	static rapidjson::Value createArray();

	// 创建一个对象
	static rapidjson::Value createObject();

	// 添加数值int成员
	static void addMember(rapidjson::Document& doc, const char* key, int value);

	// 添加数值int64_t成员
	static void addMember(rapidjson::Document& doc, const char* key, int64_t value);

	// 添加数值uint64_t成员
	static void addMember(rapidjson::Document& doc, const char* key, uint64_t value);

	// 添加字符串成员
	static void addMember(rapidjson::Document& doc, const char* key, const std::string& value);
};

#endif //RAPIDJSONWRAPPER_H
