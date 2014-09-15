#include <iostream>
#include "testservconfig.h"
#include "testserv.h"
#include "../common/network/epollnet.h"
#include "../common/log/TextLog.h"
#include "../common/network/cluster.h"


pthread_mutex_t deadlock = PTHREAD_MUTEX_INITIALIZER;

int main(int argc, char** argv)
{
	if(argc != 2)
	{
		std::cerr << "wrong arguments given, usage: Server Config" << std::endl;
		return -1;
	}

	daemon(1, 0);

//		读取配置
	TestServConfig servCfg;
	if(servCfg.ReadCfgFile(argv[1]))
	{
		return -1;
	}

	LogConfig logCfg;
	strcpy(logCfg.szpLogPath, servCfg.szpLogPath);
	strcpy(logCfg.szpLogName, servCfg.szpLogName);
	TextLog log;
	if(log.Init(&logCfg))
	{
		std::cerr << "Log Init Error" << std::endl;
		return 1;
	}

//		初始化服务器和客户端对象
	LcAbstractNet* pServNet = new LcEpollNet();
	if(pServNet->Init(&servCfg, log))
	{
		return 1;
	}

//		初始化机群对象
	TestServ serv(&log, pServNet, NULL, &servCfg);

	if(pServNet->StartThread())
	{
		log.Write("pServNet StartThread error");
		return 1;
	}

	serv.MainFun();

	return 0;
}
