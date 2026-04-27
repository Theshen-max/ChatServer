#ifndef CONFIGMGR_H
#define CONFIGMGR_H
#include "const.h"

struct SectionInfo
{
	std::unordered_map<std::string, std::string> _sectionDatas;
	std::string& operator[](const std::string& key)
	{
		return _sectionDatas[key];
	}

	const auto& data() const
	{
		return _sectionDatas;
	}

	auto data()
	{
		return _sectionDatas;
	}
};
/// ConfigMgr只管理两层树结构
class ConfigMgr
{
public:
	SectionInfo& operator[](const std::string& key);

	const std::unordered_map<std::string, SectionInfo>& data() const;

	std::unordered_map<std::string, SectionInfo>& data();

	static ConfigMgr& getInstance();

	ConfigMgr(const ConfigMgr&) = delete;
	ConfigMgr& operator=(const ConfigMgr&) = delete;

private:
	std::unordered_map<std::string, SectionInfo> _configMap;
	ConfigMgr();
};


#endif //CONFIGMGR_H
