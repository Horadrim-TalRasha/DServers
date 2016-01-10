#include <string.h>
#include "epollnet.h"
#include "../baseconfig.h"
#include "../io/FileUtil.h"

LcEpollNet::LcEpollNet()
{
	m_lsnSocket = -1;

	m_pIORecvQueue = NULL;
	m_pIOWorkQueue = NULL;
	m_pIOSndQueue = NULL;
	m_ppConnOverLaps = NULL;
	m_epSocket = -1;
	m_pEpollEvs = NULL;
	m_szpRecvPackMem = NULL;
	m_szpSndPackMem = NULL;
	m_pChecker = NULL;
}

LcEpollNet::~LcEpollNet()
{


}

int LcEpollNet::Init(BaseConfig* pBaseConfig, TextLog& textLog)
{
	if(pthread_mutex_init(&m_cSyncSendData, NULL))
	{
		m_txlNetLog->Write("mutex init error");
		return 1;
	}

	if(m_ConnectTable.Init(pBaseConfig->m_uiMaxOverLapNum))
	{
		m_txlNetLog->Write("connect hash table init error");
		return 1;
	}

	m_txlNetLog = &textLog;
	m_pBaseConfig = pBaseConfig;

	struct timeval tvTimeOut;
	tvTimeOut.tv_sec = 0;
	tvTimeOut.tv_usec = 300;

	struct linger lin;
	lin.l_onoff = 1;
	lin.l_linger = 0;
	unsigned int uiReuseAddr = 1;	


	m_lsnSocket = socket(AF_INET, SOCK_STREAM, 0);
	if(m_lsnSocket < -1)
	{
		m_txlNetLog->Write("socket create error!");
		return -1;
	}

	setsockopt(m_lsnSocket, SOL_SOCKET, SO_RCVTIMEO, &tvTimeOut, sizeof(tvTimeOut));
	setsockopt(m_lsnSocket, SOL_SOCKET, SO_SNDTIMEO, &tvTimeOut, sizeof(tvTimeOut));
	setsockopt(m_lsnSocket, SOL_SOCKET, SO_REUSEADDR, (char*)&uiReuseAddr, sizeof(uiReuseAddr));
	setsockopt(m_lsnSocket, SOL_SOCKET, SO_LINGER, (char*)&lin, sizeof(lin));

	if(FileUtil::SetNoBlock(m_lsnSocket))
	{
		m_txlNetLog->Write("SetNoBlock m_lsnSocket error!");
		return -1;
	}

	m_pIORecvQueue = new OverLap[pBaseConfig->m_uiMaxOverLapNum];
	m_pIOWorkQueue = new OverLap[pBaseConfig->m_uiMaxOverLapNum];
	m_pIOSndQueue = new OverLap[pBaseConfig->m_uiMaxOverLapNum];
	m_ppConnOverLaps = new OverLap*[pBaseConfig->m_uiMaxOverLapNum];

	m_szpRecvPackMem = new char [pBaseConfig->m_uiMaxOverLapNum * pBaseConfig->m_uiMaxPacketSize];
	m_szpWorkPackMem = new char [pBaseConfig->m_uiMaxOverLapNum * pBaseConfig->m_uiMaxPacketSize];
	m_szpSndPackMem = new char [pBaseConfig->m_uiMaxOverLapNum * pBaseConfig->m_uiMaxPacketSize];
	m_pEpollEvs = new struct epoll_event[pBaseConfig->m_uiConcurrentNum];
	m_pChecker = new LcFullPktChecker();

	if(m_pIORecvQueue == NULL || m_pIOWorkQueue == NULL || m_pIOSndQueue == NULL ||
	   m_szpRecvPackMem == NULL || m_szpWorkPackMem == NULL || m_szpSndPackMem == NULL || 
	   m_pEpollEvs == NULL || m_pChecker == NULL || m_ppConnOverLaps == NULL)
	{
		m_txlNetLog->Write("no enough memory for server!");
		return -1;
	}

	if(m_IONetConnQue.Init(pBaseConfig->m_uiMaxOverLapNum))
	{
		m_txlNetLog->Write("io conn queue init error");
		return -1;
	}

	if(m_IONetWorkMemQue.Init(pBaseConfig->m_uiMaxOverLapNum))
	{
		m_txlNetLog->Write("work memory queue init error");
		return -1;
	}

	if(m_IONetWorkQue.Init(pBaseConfig->m_uiMaxOverLapNum))
	{
		m_txlNetLog->Write("io work queue init error");
		return -1;
	}

	if(m_IONetSndMemQue.Init(pBaseConfig->m_uiMaxOverLapNum))
	{
		m_txlNetLog->Write("snd memory queue init error");
		return -1;
	}

	for(int i = 0; i < (int)pBaseConfig->m_uiMaxOverLapNum; i++)
	{
		m_pIORecvQueue[i].szpComBuf = m_szpRecvPackMem + (i * pBaseConfig->m_uiMaxPacketSize);
		m_pIOWorkQueue[i].szpComBuf = m_szpWorkPackMem + (i * pBaseConfig->m_uiMaxPacketSize);
		m_pIOSndQueue[i].szpComBuf = m_szpSndPackMem + (i * pBaseConfig->m_uiMaxPacketSize);

		m_IONetConnQue.Push((long)&m_pIORecvQueue[i]);
		m_IONetWorkMemQue.Push((long)&m_pIOWorkQueue[i]);
		m_IONetSndMemQue.Push((long)&m_pIOSndQueue[i]);
	}

	if(BindAndLsn(pBaseConfig->m_iBackLog, pBaseConfig->m_usServPort))
	{
		return -1;
	}
	return 0;
}

void LcEpollNet::RemoveConnect(OverLap* pOverLap)
{
	if(m_ConnectTable.DeleteByKey(pOverLap->u64SessionID))	//	在哈希表中查找连接
	{
		return;
	}

	struct epoll_event ev;
	epoll_ctl(m_epSocket, EPOLL_CTL_DEL, pOverLap->fd, &ev);	//	在epoll中删掉侦听的socket
	close(pOverLap->fd);										//	关闭socket
	pOverLap->u64SessionID = 0;
	pOverLap->fd = -1;
	ReleaseSndList(pOverLap);				//	释放发送链
	m_IONetConnQue.Push((long)pOverLap);	//	放回内存队列
}

void LcEpollNet::RemoveConnectByID(const unsigned long long& u64SessionID)
{
	OverLap* pOverLap = NULL;
	if(m_ConnectTable.DeleteByKey(u64SessionID, pOverLap))	//	在哈希表中查找连接
	{
		return;
	}

	struct epoll_event ev;
	epoll_ctl(m_epSocket, EPOLL_CTL_DEL, pOverLap->fd, &ev);	//	在epoll中删掉侦听的socket
	close(pOverLap->fd);										//	关闭socket
	pOverLap->u64SessionID = 0;
	pOverLap->fd = -1;
	ReleaseSndList(pOverLap);				//	释放发送链
	m_IONetConnQue.Push((long)pOverLap);	//	放回内存队列中
}

void LcEpollNet::GetRequest(long& lptr)
{
	m_IONetWorkQue.Pop(lptr);
}

void LcEpollNet::ReleaseRequest(const long& lptr)
{
	m_IONetWorkMemQue.Push(lptr);
}

void LcEpollNet::RequestSnd(long& lptr)
{
	m_IONetSndMemQue.Pop(lptr);
}

void LcEpollNet::ReleaseSndReq(const long& lptr)
{
	m_IONetSndMemQue.Push(lptr);
}

void LcEpollNet::SendData(OverLap* pSndOverLap)
{
	if(pSndOverLap == NULL)
	{
		return;
	}
	else if(pSndOverLap->uiSndComLen == 0)
	{
		m_IONetSndMemQue.Push((long)pSndOverLap);
		return;
	}

	OverLap* pOverLap = NULL;
	if(m_ConnectTable.Search(pSndOverLap->u64SessionID, pOverLap) == 0)
	{
		OverLap* pTmp = pOverLap;
		pthread_mutex_lock(&m_cSyncSendData);
		while(pTmp->pSndList)
		{
			pTmp = pTmp->pSndList;
		}

		pTmp->pSndList = pSndOverLap;
		pthread_mutex_unlock(&m_cSyncSendData);

		struct epoll_event ev;
		ev.events = EPOLLOUT | EPOLLIN | EPOLLET;
		ev.data.ptr = (void*)pOverLap;
		if(epoll_ctl(m_epSocket, EPOLL_CTL_MOD, pOverLap->fd, &ev) == -1)
		{
			m_txlNetLog->Write("epoll_ctl_Mod error SendData Error");
			RemoveConnect(pOverLap);
		}

		return;
	}

	m_txlNetLog->Write("SessionID %llu not found", pSndOverLap->u64SessionID);
}

int LcEpollNet::BindAndLsn(const int& iBackLog, const unsigned short& usPort)
{
	sockaddr_in svrSockAddr;
	svrSockAddr.sin_family = AF_INET;
	svrSockAddr.sin_port = htons(usPort);
	svrSockAddr.sin_addr.s_addr = htonl(INADDR_ANY);

	m_epSocket = epoll_create(m_pBaseConfig->m_uiConcurrentNum);
	if(m_epSocket == -1)
	{
		m_txlNetLog->Write("epoll_create error!");
		return -1;
	}

	struct epoll_event ev;
	ev.events = EPOLLIN;
	ev.data.fd = m_lsnSocket;
	if(epoll_ctl(m_epSocket, EPOLL_CTL_ADD, m_lsnSocket, &ev) == -1)
	{
		m_txlNetLog->Write("epoll_ctl add lsnSocket error!");
		return -1;
	}

	if(bind(m_lsnSocket, (sockaddr*)&svrSockAddr, sizeof(svrSockAddr)) == -1)
	{
		m_txlNetLog->Write("bind lsnSocket error!");
		return -1;
	}

	if(listen(m_lsnSocket, iBackLog) == -1)
	{
		m_txlNetLog->Write("listen lsnSocket error!");
		return -1;
	}
	return 0;
}

int LcEpollNet::StartThread()
{
	pthread_t heartbeat_thrd;
    int ret = 0;


    pthread_t posix_thrd;
    ret = pthread_create(&posix_thrd, NULL, Thread_NetServ, this);
   	if(ret)
   	{
	    m_txlNetLog->Write("pthread_create Thread_NetServ error!");
    	return -1;
   	}

    pthread_t posix_thrd_1;
    ret = pthread_create(&posix_thrd_1, NULL, Thread_NetServ, this);
   	if(ret)
   	{
	    m_txlNetLog->Write("pthread_create Thread_NetServ error!");
    	return -1;
   	}

    pthread_t posix_thrd_2;
    ret = pthread_create(&posix_thrd_2, NULL, Thread_NetServ, this);
   	if(ret)
   	{
	    m_txlNetLog->Write("pthread_create Thread_NetServ error!");
    	return -1;
   	}

	ret = pthread_create(&heartbeat_thrd, NULL, Thread_HeartBeat, this);
	if(ret)
	{
		m_txlNetLog->Write("pthread_create Thread_HeartBeat error!");
		return -1;
	}
	return 0;
}

void* LcEpollNet::Thread_NetServ(void* param)
{
	LcEpollNet* pNet = (LcEpollNet*)param;

	sockaddr_in cliSockAddr;
	socklen_t addrLen = sizeof(sockaddr);

    pNet->m_txlNetLog->Write("Thread %llu was created.", pthread_self());
	while(1)
	{
		int nfds = epoll_wait(pNet->m_epSocket, pNet->m_pEpollEvs, pNet->m_pBaseConfig->m_uiConcurrentNum, -1);
		if(nfds < 0 && errno == EINTR)
		{
			continue;
		}
		else if(nfds < 0)
		{
			pNet->m_txlNetLog->Write("epoll_wait error!");
			return NULL;
		}

        pNet->m_txlNetLog->Write("Thread %llu was awakened.", pthread_self());
		for(int i = 0; i < nfds; i++)
		{
			if(pNet->m_pEpollEvs[i].data.fd == pNet->m_lsnSocket)
			{
				int fd = accept(pNet->m_lsnSocket, (sockaddr*)&cliSockAddr, &addrLen);
				if(fd == -1)
				{
					pNet->m_txlNetLog->Write("accept from %s:%u error", inet_ntoa(cliSockAddr.sin_addr), ntohs(cliSockAddr.sin_port));
					continue;
				}

				if(FileUtil::SetNoBlock(fd))
				{
					close(fd);
					pNet->m_txlNetLog->Write("peer(%s:%u, fd: %u) set no block error", inet_ntoa(cliSockAddr.sin_addr), ntohs(cliSockAddr.sin_port), fd);
					continue;
				}

				pNet->m_txlNetLog->Write("accept from %s:%u success", inet_ntoa(cliSockAddr.sin_addr), ntohs(cliSockAddr.sin_port));
				pNet->EpollAccept(fd, cliSockAddr.sin_addr.s_addr, cliSockAddr.sin_port);
			}
			else if(pNet->m_pEpollEvs[i].events & EPOLLIN)
			{
				pNet->EpollRecv((OverLap*)pNet->m_pEpollEvs[i].data.ptr);
			}
			else if(pNet->m_pEpollEvs[i].events & EPOLLOUT)
			{
				pNet->EpollSend((OverLap*)pNet->m_pEpollEvs[i].data.ptr);	
			}
			else
			{
				OverLap* pOverLap = (OverLap*)pNet->m_pEpollEvs[i].data.ptr;
				pNet->RemoveConnect(pOverLap);		
			}
		}
	}

	pNet->m_txlNetLog->Write("thread is going to be over");
	return NULL;
}

void* LcEpollNet::Thread_HeartBeat(void* param)
{
	LcEpollNet* pNet = (LcEpollNet*)param;
	while(1)
	{
		unsigned int uiConnNum = 0;
		pNet->m_ConnectTable.GetValues(pNet->m_ppConnOverLaps, uiConnNum);
		unsigned long long tNow = time(NULL);
		for(unsigned int u = 0; u < uiConnNum; u++)
		{
			OverLap* pOverLap = pNet->m_ppConnOverLaps[u];
			if(tNow - pOverLap->u64HeartBeatRecv > pNet->m_pBaseConfig->m_uiHeartBeatInterval)
			{
				pNet->m_txlNetLog->Write("Connect %llu HeartBeat OverTime", pOverLap->u64SessionID);
				// to do close connect
				pNet->RemoveConnect(pOverLap);
			}
		}
		sleep(pNet->m_pBaseConfig->m_uiHeartBeatInterval);
	}
	return NULL;
}

void LcEpollNet::EpollAccept(const int& fd, const unsigned int& uiPeerIP, const unsigned short& usPeerPort)
{
	long ptr = 0;
	m_IONetConnQue.Pop(ptr);
	OverLap* pOverLap = (OverLap*)ptr;

	/***		初始化重叠结构，初始化的成员今后可能会扩展		****/
	pOverLap->uiComLen = m_pBaseConfig->m_uiMaxPacketSize;
	pOverLap->uiFinishLen = 0;
	pOverLap->uiPeerIP = uiPeerIP;
	pOverLap->usPeerPort = usPeerPort;
	pOverLap->fd = fd;
	pOverLap->u64PacketRecv = 0;
	pOverLap->u64PacketSnd = 0;
	pOverLap->uiPacketLen = 0;
	pOverLap->u64HeartBeatRecv = time(NULL);
	/***		初始化重叠结构，初始化的成员今后可能会扩展		****/

	struct epoll_event ev;
	ev.events = EPOLLIN | EPOLLET;
	ev.data.ptr = (void*)pOverLap;

	if(epoll_ctl(m_epSocket, EPOLL_CTL_ADD, fd, &ev) == -1)
	{
		m_IONetConnQue.Push(ptr);
		close(fd);
		char szaPeerIP[20];
		sprintf(szaPeerIP, "%u.%u.%u.%u", *((unsigned char*)&uiPeerIP), *((unsigned char*)&uiPeerIP + 1), *((unsigned char*)&uiPeerIP + 2), *((unsigned char*)&uiPeerIP + 3));
		m_txlNetLog->Write("add Peer(%s:%u) to epoll error", szaPeerIP, usPeerPort);
	}
	
	unsigned long long u64ConnectID = time(NULL);
	pOverLap->u64SessionID = u64ConnectID;
	m_ConnectTable.Insert(u64ConnectID, pOverLap);
}

void LcEpollNet::EpollRecv(OverLap* pOverLap)
{
	int ret = 0;
	char szaPeerIP[20];
	IP2Str(pOverLap->uiPeerIP, szaPeerIP);
	bool bIsHeadChked = false;

	while(1)
	{
		ret = recv(pOverLap->fd, pOverLap->szpComBuf + pOverLap->uiFinishLen, pOverLap->uiComLen, 0);
		if(ret == 0 || ret > (int)pOverLap->uiComLen)
		{
			RemoveConnect(pOverLap);
			m_txlNetLog->Write("error: serv peer(%s:%u) close", szaPeerIP, ntohs(pOverLap->usPeerPort));
			return;
		}
		else if(ret == -1 && errno == EAGAIN)
		{
			// no data
			break;
		}
		else if(ret == -1)
		{
			// error when recv data
			RemoveConnect(pOverLap);
			m_txlNetLog->Write("error: serv recv data from peer(%s:%u) error", szaPeerIP, ntohs(pOverLap->usPeerPort));
			return;
		}

		m_txlNetLog->Write("got data(%d byte) from peer(%s:%u)", ret, szaPeerIP, ntohs(pOverLap->usPeerPort));
		pOverLap->uiComLen -= ret;
		pOverLap->uiFinishLen += ret;

		switch(CheckPacket(pOverLap, bIsHeadChked))
		{
		case 0:
			continue;
		case 1:		//	包头校验失败，删除连接
		case 2:		//	包尾校验失败，删除连接
			RemoveConnect(pOverLap);
			return;
		case 3:		//	长度小于包头长度，继续从缓存中读取数据
		case 4:		//	长度小于全包长度，继续从缓存中读取数据
			continue;
		}
	}
}

void LcEpollNet::EpollSend(OverLap* pOverLap)
{
	int fd = pOverLap->fd;

	pthread_mutex_lock(&m_cSyncSendData);
	OverLap* pSndList = pOverLap->pSndList;
	pOverLap->pSndList = NULL;
	pthread_mutex_unlock(&m_cSyncSendData);

	while(pSndList)
	{
		pSndList->uiSndFinishLen = 0;
		while(pSndList->uiSndComLen > 0)
		{
			int ret = send(fd, pSndList->szpComBuf + pSndList->uiSndFinishLen, pSndList->uiSndComLen, MSG_NOSIGNAL);
			if(ret == -1 && errno == EAGAIN)	// 系统缓存写满重新放入发送链表
			{
				// to do 发起epoll写事件
				SendData(pSndList);
				return;
			}
			else if(ret == 0 || ret > (int)pSndList->uiSndComLen)
			{
				RemoveConnect(pOverLap);
				return;
			}
			else if(ret == -1)
			{
				RemoveConnect(pOverLap);
				return;
			}

			pSndList->uiSndFinishLen += ret;
			pSndList->uiSndComLen -= ret;
		}

		pOverLap->u64PacketSnd += 1;
		m_txlNetLog->Write("u64PacketRecv = %u u64PacketSnd = %u, pSndOverLap->uiSndComLen = %u, pSndOverLap->uiSndFinishLen = %u", pOverLap->u64PacketRecv, pOverLap->u64PacketSnd, pSndList->uiSndComLen, pSndList->uiSndFinishLen);
		OverLap* pTmp = pSndList;			//	将发送的重叠结构指针指向一个临时指针
		pSndList = pSndList->pSndList;		//	准备发送下一个
		pTmp->pSndList = NULL;				//	断开和后面的发送链
		m_IONetSndMemQue.Push((long)pTmp);		//	放回发送内存队列
	}
}

int LcEpollNet::CheckPacket(OverLap* pOverLap, bool& bIsHeadChked)
{
	while(1)
	{
		if(!bIsHeadChked)	// 校验包头
		{
			switch(m_pChecker->CheckPacketHead(pOverLap, m_pBaseConfig->m_uiHeadPacketSize))
			{
			case 0:	//	包头校验成功，bIsHeadChked设为true
				bIsHeadChked = true;
				m_txlNetLog->Write("check head success");
				break;
			case 1:	//	读取包的长度小于包头的长度
				m_txlNetLog->Write("%d bytes is not long enough to check head", pOverLap->uiFinishLen);
				return 3;
			case 2:	//	校验失败
				m_txlNetLog->Write("error: check head error, connect will be closed ID_Code = %u(expectd: %u)", *(unsigned int*)pOverLap->szpComBuf, IDENTIFY_CODE);
				return 1;
			}
		}

		unsigned int uiPacketLen = *(unsigned int*)(pOverLap->szpComBuf + OFFSET_PACKET_LEN);
		switch(m_pChecker->CheckPacketEnd(pOverLap, m_pBaseConfig->m_uiHeadPacketSize, m_pBaseConfig->m_uiMaxPacketSize))	//	校验包尾
		{
		case 0:	//	包尾校验成功，bIsHeadChked设为false，移动内存，并重新给重叠结构的uiComLen和uiFinishLen赋值校验下一个包，然后将完整的包送到工作线程
			m_txlNetLog->Write("check end success");
			bIsHeadChked = false;
			SendToWorkQue(pOverLap, uiPacketLen);
			memcpy(pOverLap->szpComBuf, pOverLap->szpComBuf + uiPacketLen, pOverLap->uiFinishLen - uiPacketLen);
			pOverLap->uiFinishLen -= uiPacketLen;
			pOverLap->uiComLen = m_pBaseConfig->m_uiMaxPacketSize - pOverLap->uiFinishLen;
			break;
		case 1:	//	读取包的长度小于包的长度
			m_txlNetLog->Write("packet len: %d, read len: %d not long enough to check end", uiPacketLen, pOverLap->uiFinishLen);
			return 4;
		case 2:	//	包尾校验失败
			m_txlNetLog->Write("error: check end error, connect will be closed");
			return 2;
		}
	}

	return 0;
}

void LcEpollNet::SendToWorkQue(OverLap* pOverLap, const unsigned int& uiPacketLen)
{
	pOverLap->u64PacketRecv += 1;		// 收包数+1
	if(*(unsigned int*)(pOverLap->szpComBuf + 4) == HEART_BEAT)
	{
		m_txlNetLog->Write("recv heart beat packet");
		pOverLap->u64HeartBeatRecv = time(NULL);
	}
	long lWorkMem = 0;
	m_IONetWorkMemQue.Pop(lWorkMem);
	OverLap* pWorkOverLap = (OverLap*)lWorkMem;
	memcpy(pWorkOverLap->szpComBuf, pOverLap->szpComBuf, uiPacketLen);

	pWorkOverLap->u64SessionID = pOverLap->u64SessionID;
	pWorkOverLap->uiPacketLen = uiPacketLen;
	pWorkOverLap->uiPeerIP = pOverLap->uiPeerIP;
	pWorkOverLap->usPeerPort = pOverLap->usPeerPort;
	pWorkOverLap->u64PacketRecv = pOverLap->u64PacketRecv;
	pWorkOverLap->u64PacketSnd = pOverLap->u64PacketSnd;

	m_IONetWorkQue.Push((long)pWorkOverLap);
}

void LcEpollNet::ReleaseSndList(OverLap* pOverLap)
{
	pthread_mutex_lock(&m_cSyncSendData);
	OverLap* pSndOverLap = pOverLap->pSndList;
	pOverLap->pSndList = NULL;
	pthread_mutex_unlock(&m_cSyncSendData);

	while(pSndOverLap)
	{
		OverLap* pTmpOverLap = pSndOverLap;
		pSndOverLap = pSndOverLap->pSndList;
		m_IONetSndMemQue.Push((long)pTmpOverLap);
		pTmpOverLap->pSndList = NULL;
	}

}
