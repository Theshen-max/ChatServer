#ifndef HASHPWD_H
#define HASHPWD_H
#include "const.h"

class PasswordCrypto
{
public:
	static std::string generateSalt(size_t len);

	static std::string hashPassword(const std::string& password, const std::string& salt);

	static std::string toHex(unsigned char* hash, size_t len);
};

#endif //HASHPWD_H
