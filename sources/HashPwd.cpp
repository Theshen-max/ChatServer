//
// Created by 27044 on 26-3-17.
//

#include "../headers/HashPwd.h"

std::string PasswordCrypto::generateSalt(size_t len)
{
	std::string salt(len, '\0');

	if (RAND_bytes(reinterpret_cast<unsigned char*>(&salt[0]), len) != 1)
	{
		throw std::runtime_error("RAND_bytes failed");
	}

	return salt;
}

std::string PasswordCrypto::hashPassword(const std::string& password, const std::string& salt)
{
	// 将密码与盐组合成复杂字符串
	std::string combined = password + salt;
	// 创建hash缓冲区
	unsigned char hash[SHA256_DIGEST_LENGTH];
	SHA256(reinterpret_cast<const unsigned char*>(combined.c_str()), combined.length(), hash);
	return toHex(hash, SHA256_DIGEST_LENGTH);
}

std::string PasswordCrypto::toHex(unsigned char* hash, size_t len)
{
	std::stringstream ss;
	for (size_t i = 0; i < len; i++)
	{
		// hash[i]是字节类型，(C++17后用std::byte表示，不是unsigned char), 所以强转成int，避免当成字符读取
		ss << std::setw(2) << std::setfill('0') << std::hex << static_cast<int>(hash[i]);
	}
	return ss.str();
}
