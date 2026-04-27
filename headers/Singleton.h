#ifndef SINGLETON_H
#define SINGLETON_H

#include <memory>
#include <iostream>
template <typename T>
class Singleton
{
public:
	Singleton(const Singleton&) = delete;
	Singleton& operator=(const Singleton&) = delete;
protected:
	Singleton() = default;
public:
	static std::shared_ptr<T> getInstance()
	{
		// 这里是new的话，就只用将本类声明为子类的友元类，不然就要声明std::make_shared是友元函数
		static std::shared_ptr<T> instance(new T);
		return instance;
	}

	void printAddress()
	{
		std::cout << getInstance().get() << std::endl;
	}

	~Singleton() // 这里因为派生类不会通过基类（指针或引用）来访问派生类，所以不需要虚析构
	{
		std::cout << "this is singleton destruct" << std::endl;
	}
};

#endif //SINGLETON_H
