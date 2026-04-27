#include "../headers/ConfigMgr.h"

#include <filesystem>
#include <iostream>
#include <boost/property_tree/ptree.hpp>
#include <boost/property_tree//ini_parser.hpp>

ConfigMgr::ConfigMgr()
{
	std::filesystem::path currentPath = std::filesystem::current_path();
	std::filesystem::path configPath = currentPath / "config.ini";
	std::cout << "configPath is " << configPath << std::endl;

	boost::property_tree::ptree pt;
	boost::property_tree::ini_parser::read_ini(configPath.string(), pt);

	// 存储
	for (const auto& [name, children]: pt)
	{
		SectionInfo sectionInfo;
		for (const auto& [childKey, childValue]: children)
		{
			sectionInfo[childKey] = childValue.get_value<std::string>();
		}
		_configMap[name] = sectionInfo;
	}

	// 输出
	for (const auto& [name, sectionInfo]: _configMap)
	{
		std::cout << "[" << name << "]" << std::endl;
		for (const auto& [key, value]: sectionInfo.data())
		{
			std::cout << key << " = " << value << std::endl;
		}
	}
}

SectionInfo& ConfigMgr::operator[](const std::string& key)
{
	return _configMap[key];
}

const std::unordered_map<std::string, SectionInfo>& ConfigMgr::data() const
{
	return _configMap;
}

std::unordered_map<std::string, SectionInfo>& ConfigMgr::data()
{
	return _configMap;
}

ConfigMgr& ConfigMgr::getInstance()
{
	static ConfigMgr instance;
	return instance;
}
