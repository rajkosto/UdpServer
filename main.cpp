#include <iostream>
#include <string>
#include <map>
#include <memory>
#include <chrono>
#include <cassert>
#include <sstream>
#include <boost/bind/bind.hpp>
#include <boost/asio.hpp>

static const char PROGRAM_VERSION[] = "UdpServer 0.2";

using u8 = std::uint8_t;
using u16 = std::uint16_t;
using u32 = std::uint32_t;
using u64 = std::uint64_t;
using usize = std::size_t;
using ushort = unsigned short;
using std::string;

using boost::asio::ip::udp;
using boost::asio::ip::tcp;

using ByteVector = std::vector<u8>;
using IoContext = boost::asio::io_context;
using ErrorCode = boost::system::error_code;
using SystemError = boost::system::system_error;
using TcpEndpoint = tcp::endpoint;
using UdpEndpoint = udp::endpoint;

static u64 MonotonicMicrosecondNow()
{
	//Get the current time point from the steady clock
	const auto now = std::chrono::steady_clock::now();

	//Convert the time point to microseconds since the epoch of the steady clock
	auto now_us = std::chrono::time_point_cast<std::chrono::microseconds>(now).time_since_epoch();

	//return the current time in microseconds
	return static_cast<u64>(now_us.count());
}

struct DistributedPacket
{
	using SharedPtr = std::shared_ptr<DistributedPacket>;
	DistributedPacket(string src, const void* srcBytes, usize srcLen) : creationTime(MonotonicMicrosecondNow()), recvSource(std::move(src))
	{
		if (srcLen > 0)
		{
			contents.resize(srcLen);
			memcpy(contents.data(),srcBytes,contents.size());
		}
	}

	u64 creationTime;
	string recvSource;
	ByteVector contents;
	SharedPtr nextPacket;
};

class Subscriber
{
public:
	virtual ~Subscriber() {}
	virtual void sendResponse(DistributedPacket::SharedPtr thePacket) = 0;
};

class Publisher
{
public:
	virtual ~Publisher() {}
	virtual usize addClient(const std::shared_ptr<Subscriber>& newClient) = 0;
	virtual usize delClient(const std::weak_ptr<Subscriber>& oldClient) = 0;
};

template<typename T>
static bool IsWeakPtrEqual(const std::weak_ptr<T>& a, const std::weak_ptr<T>& b) 
{
	//Neither a is before b, nor b is before a implies a and b share ownership or are both empty
	return !a.owner_before(b) && !b.owner_before(a);
}

class ListenerBase : public Publisher
{
public:
	virtual bool stop() = 0;
	virtual void initiateReceive() = 0;

	using SharedSubPtr = std::shared_ptr<Subscriber>;
	using WeakSubPtr = std::weak_ptr<Subscriber>;

protected:
	auto activeClients() { return clientsPtr; }
	usize numClients() const { return clientsPtr->size(); }
	auto clientsBegin() { return activeClients()->begin(); }
	auto clientsEnd() { return activeClients()->end(); }

public:
	usize addClient(const SharedSubPtr& newClient) override
	{
		const usize oldNumClients = numClients();
		auto it = std::find_if(clientsBegin(),clientsEnd(),[&newClient](const auto& wp) { return wp.lock() == newClient; });
		if (it != clientsEnd())
			return oldNumClients;

		activeClients()->push_back(newClient);
		return oldNumClients;
	}	
	usize delClient(const WeakSubPtr& oldClient) override
	{
		const usize oldNumClients = numClients();
		//Remove matching weak_ptrs using the erase-remove idiom
		activeClients()->erase(std::remove_if(clientsBegin(),clientsEnd(),[&oldClient](const auto& wp) { return IsWeakPtrEqual(wp,oldClient); }),clientsEnd());
		return oldNumClients;
	}
protected:
	auto inactiveClients()
	{
		if (clientsPtr == &oneClients)
			return &twoClients;
		else if (clientsPtr == &twoClients)
			return &oneClients;
		else
			return static_cast<WeakSubsVector*>(nullptr);
	}

	virtual usize sendToClients(DistributedPacket::SharedPtr thePacket)
	{
		if (lastSent)
		{
			assert(!lastSent->nextPacket);
			lastSent->nextPacket = thePacket;
		}

		const usize oldNumClients = numClients();
		auto& act = *activeClients();
		auto& inact = *inactiveClients();

		inact.clear();
		inact.reserve(act.size());
		for (auto& wp : act)
		{
			auto sp = wp.lock();
			if (!sp)
				continue;

			inact.emplace_back(sp);
			sp->sendResponse(thePacket);
		}

		clientsPtr = &inact;
		lastSent = std::move(thePacket);

		return oldNumClients;
	}

private:
	using WeakSubsVector = std::vector<std::weak_ptr<Subscriber>>;
	WeakSubsVector oneClients;
	WeakSubsVector* clientsPtr = &oneClients;
	WeakSubsVector twoClients;	
	WeakSubsVector* clientsAlt = &twoClients;
	DistributedPacket::SharedPtr lastSent;
};

class UdpListener : public ListenerBase, public std::enable_shared_from_this<UdpListener>
{
	static constexpr usize RECEIVE_BUFFER_SIZE = 2048;
public:
	UdpListener(IoContext& ioCtx, UdpEndpoint listenAddr) : udpSocket(ioCtx,listenAddr)
	{ 
		bindAddr = udpSocket.local_endpoint();
		std::cout << "UDP listener " << bindAddr << " created" << std::endl;
	}
	~UdpListener() 
	{ 
		try
		{
			stop(); 
			std::cout << "UDP listener " << bindAddr << " destroyed" << std::endl; 
		}
		catch (const SystemError& e)
		{
			std::cout << string("Error ") + e.what() + " destroying UDP listener [" << bindAddr << "]: " << e.code().message() << std::endl; 
		}
	}

	void initiateReceive() override
	{
		udpSocket.async_receive_from(
			boost::asio::buffer(localRecvBuf),currSource,
			boost::bind(&UdpListener::handleReceive,shared_from_this(),
				boost::asio::placeholders::error,
				boost::asio::placeholders::bytes_transferred));
	}

	bool stop() override
	{
		if (!udpSocket.is_open())
			return false;

		udpSocket.close();
		return true;
	}
	usize addClient(const SharedSubPtr& newClient) override
	{
		const usize oldNumClients = ListenerBase::addClient(newClient);
		const usize newNumClients = numClients();

		if (newNumClients != oldNumClients)
			std::cout << "UDP listener " << bindAddr << " number of clients " << oldNumClients << " -> " << numClients() << std::endl;

		return oldNumClients;
	}
	usize delClient(const WeakSubPtr& oldClient) override
	{
		const usize oldNumClients = ListenerBase::delClient(oldClient);
		const usize newNumClients = numClients();

		if (newNumClients != oldNumClients)
			std::cout << "UDP listener " << bindAddr << " number of clients " << oldNumClients << " -> " << newNumClients << std::endl;

		return oldNumClients;
	}
private:
	void handleReceive(const ErrorCode& error, usize numBytes)
	{
		if (error == boost::asio::error::operation_aborted)
		{
			std::cout << "Source connection from " << currSource << " to " << bindAddr << " closed due to no longer being needed." << std::endl;
			return;
		}
		else if (error)
		{
			std::cout << "Error " << error.message() << " receiving packet on bound addr [" << bindAddr << "] (bytes transferred " << numBytes << ")" << std::endl;
		}
		else 
		{
			if (numBytes < 1)
			{
				std::cout << "Received 0 byte packet from " << currSource << " to " << bindAddr << "!" << std::endl;
			}

			DistributedPacket::SharedPtr thePacket;
			{
				std::ostringstream strBuf; strBuf << currSource;
				thePacket = std::make_shared<DistributedPacket>(strBuf.str(),localRecvBuf.data(),numBytes);
			}			

			const usize oldNumClients = ListenerBase::sendToClients(std::move(thePacket));
			const usize newNumClients = numClients();
			if (newNumClients != oldNumClients)
			{
				std::cout << "UDP listener " << bindAddr << " number of clients " << oldNumClients << " -> " << newNumClients << std::endl;
			}

			initiateReceive();
		}
	}

	udp::socket udpSocket;
	UdpEndpoint bindAddr;
	UdpEndpoint currSource;
	std::array<uint8_t,RECEIVE_BUFFER_SIZE> localRecvBuf;
};

#if defined(BOOST_ASIO_HAS_POSIX_STREAM_DESCRIPTOR)
class PosixListener : public ListenerBase, public std::enable_shared_from_this<PosixListener>
{
public:
	PosixListener(IoContext& ioCtx, int fileNo, usize recvBufSize) : pipeStream(ioCtx), theFileNo(fileNo)
	{
		pipeStream.assign(fileNo);
		localRecvBuf.resize(recvBufSize);
		std::cout << "Pipe listener created for " << fileNo << " with buffer size " << localRecvBuf.size() << std::endl;
	}
	~PosixListener() 
	{ 
		try
		{
			stop(); 
			std::cout << "Pipe listener for " << theFileNo << " destroyed" << std::endl; 
		}
		catch (const SystemError& e)
		{
			std::cout << string("Error ") + e.what() + " destroying Pipe listener for " << theFileNo << ": " << e.code().message() << std::endl; 
		}
	}

	void initiateReceive() override
	{
		boost::asio::async_read(pipeStream,
			boost::asio::buffer(localRecvBuf),
			boost::bind(&PosixListener::handleReceive,shared_from_this(),
				boost::asio::placeholders::error,
				boost::asio::placeholders::bytes_transferred));
	}

	bool stop() override
	{
		if (!pipeStream.is_open())
			return false;

		pipeStream.release();
		return true;
	}
	usize addClient(const SharedSubPtr& newClient) override
	{
		const usize oldNumClients = ListenerBase::addClient(newClient);
		const usize newNumClients = numClients();

		if (newNumClients != oldNumClients)
			std::cout << "Pipe listener for " << theFileNo << " number of clients " << oldNumClients << " -> " << newNumClients << std::endl;

		return oldNumClients;
	}
	usize delClient(const WeakSubPtr& oldClient) override
	{
		const usize oldNumClients = ListenerBase::delClient(oldClient);
		const usize newNumClients = numClients();

		if (newNumClients != oldNumClients)
			std::cout << "Pipe listener for " << theFileNo << " number of clients " << oldNumClients << " -> " << newNumClients << std::endl;

		return oldNumClients;
	}
private:
	void handleReceive(const ErrorCode& error, usize numBytes)
	{
		if (error == boost::asio::error::operation_aborted)
		{
			std::cout << "Pipe for " << theFileNo << " closed due to no longer being needed." << std::endl;
			return;
		}
		else if (error)
		{
			std::cout << "Error " << error.message() << " receiving packet on fileNo " << theFileNo << " (bytes transferred " << numBytes << ")" << std::endl;
		}
		else
		{
			if (numBytes < 1)
			{
				std::cout << "Received 0 byte packet from pipe!" << std::endl;
			}

			auto thePacket = std::make_shared<DistributedPacket>("-",localRecvBuf.data(),numBytes);
			const usize oldNumClients = ListenerBase::sendToClients(std::move(thePacket));
			const usize newNumClients = numClients();
			if (newNumClients != oldNumClients)
			{
				std::cout << "Pipe listener for " << theFileNo << " number of clients " << oldNumClients << " -> " << newNumClients << std::endl;
			}

			initiateReceive();
		}
	}

	int theFileNo = 0;
	boost::asio::posix::stream_descriptor pipeStream;
	ByteVector localRecvBuf;
};
static std::shared_ptr<PosixListener> g_pipeListener;
#endif

static UdpEndpoint ParseIpPortString(const char* ipPortStr)
{
	UdpEndpoint outPoint;

	//Find the position of the colon separating the IP address from the port number.
	const char* colonPtr = strchr(ipPortStr,':');
	if (colonPtr == nullptr)
		return outPoint;

	const int portNum = atoi(colonPtr+1);
	if (portNum <= 0 || portNum >= 65535)
		return outPoint;

	try
	{
		const auto ipAddr = boost::asio::ip::address::from_string(string(ipPortStr,colonPtr-ipPortStr));
		outPoint = UdpEndpoint(ipAddr,static_cast<ushort>(portNum));
	}
	catch (const SystemError&) 
	{
		outPoint = UdpEndpoint{};
	}

	return outPoint;
}

static string IpPortToString(const UdpEndpoint& theEndpoint)
{
	string outStr;
	if (theEndpoint != UdpEndpoint{})
	{
		char tempBuf[16];
		sprintf(tempBuf,":%d",static_cast<int>(theEndpoint.port()));
		outStr = theEndpoint.address().to_string() + tempBuf;
	}
	return outStr;
}

class HttpServer
{
public:
	HttpServer(IoContext& theIoCtx, TcpEndpoint listenAddr, string path) : ioCtx(theIoCtx), tempSocket(theIoCtx), acceptor(theIoCtx,listenAddr), pathPrefix(std::move(path)) { initiateAccept(); }

	void initiateAccept();
	std::shared_ptr<ListenerBase> getListener(TcpEndpoint requestor, const string& requestPath)
	{
		std::shared_ptr<ListenerBase> retListener;

		if (requestPath.length() < 1 || requestPath[0] != '/')
			return retListener;

		if (requestPath.length()-1 < pathPrefix.length() || strncmp(requestPath.c_str()+1,pathPrefix.c_str(),pathPrefix.length()))
			return retListener;

		if (requestPath[1+pathPrefix.length()] != '/')
			return retListener;

		const char* actualPath = &requestPath[1+pathPrefix.length()+1];
		std::cout << requestor << " actual PATH: |" << actualPath << "|" << std::endl;
#if defined(BOOST_ASIO_HAS_POSIX_STREAM_DESCRIPTOR)
		if (!strcmp(actualPath,"-") && g_pipeListener)
			return g_pipeListener;
#endif

		UdpEndpoint udpEndpoint = ParseIpPortString(actualPath);
		if (udpEndpoint == UdpEndpoint{})
			return retListener;

		string parsedEndpoint = IpPortToString(udpEndpoint);
		std::cout << requestor << " parsed IpPort: |" << parsedEndpoint << "|" << std::endl;

		auto udpIt = listeners.find(parsedEndpoint);
		if (udpIt != listeners.end())
			retListener = udpIt->second.lock();

		//create a new listener and put it in the map
		if (!retListener)
		{
			try
			{
				retListener = std::make_shared<UdpListener>(ioCtx,udpEndpoint);
			}
			catch (const SystemError& e)
			{
				std::cout << string("Error ") + e.what() + " creating UDP listener [" << udpEndpoint << "]: " << e.code().message() << std::endl;
				throw e; //it will be converted into internal server error
			}

			if (udpIt == listeners.end())
				listeners.emplace(std::move(parsedEndpoint),retListener);
			else
				udpIt->second = retListener;

			retListener->initiateReceive();
		}

		return retListener;
	}
private:
	void handleAccept(const ErrorCode& error);

	IoContext& ioCtx;
	tcp::socket tempSocket;
	tcp::acceptor acceptor;
	string pathPrefix;
	std::map<string,std::weak_ptr<ListenerBase>> listeners;
};

struct HttpRequest
{
	string path;
	std::map<string,string> headers;
	int responseCode = 0;
	const char* responseString = "ERR";
	string responseText;
};

class HttpClient : public Subscriber, public std::enable_shared_from_this<HttpClient>
{
	static constexpr usize LINE_BUFFER_SIZE = 1024;
public:
	HttpClient(HttpServer& theServer, tcp::socket&& acceptedSock) : parentServer(theServer), clientSocket(std::move(acceptedSock)) 
	{ 
		remoteEndpoint = clientSocket.remote_endpoint(); 
		
		//Option to enable TCP_NODELAY
		boost::asio::ip::tcp::no_delay option(true);
		clientSocket.set_option(option);
	}
	~HttpClient() 
	{ 
		try
		{
			stop(); 
			std::cout << "HTTP client " << remoteEndpoint << " destroyed" << std::endl; 
		}
		catch (const SystemError& e)
		{
			std::cout << string("Error ") + e.what() + " destroying HTTP client [" << remoteEndpoint << "]: " << e.code().message() << std::endl; 
		}
	}

	bool initiateReceive()
	{
		const usize startOffset = recvBufBytes;
		const usize availSize = localRecvBuf.size()-startOffset;
		if (availSize > localRecvBuf.size())
			return false;

		clientSocket.async_read_some(boost::asio::buffer(&localRecvBuf[startOffset],availSize),
			boost::bind(&HttpClient::handleReceive,shared_from_this(),boost::asio::placeholders::error,boost::asio::placeholders::bytes_transferred)
		);
		return true;
	}

	bool stop()
	{
		if (myListener)
			myListener->delClient(weak_from_this());

		if (!clientSocket.is_open())
			return false;

		clientSocket.close();
		return true;
	}

	void sendResponse(DistributedPacket::SharedPtr thePacket)
	{
		if (thePacket->contents.size() < 1)
			return;

		if (!currentlySending)
			initiateSend(std::move(thePacket));
	}

protected:
	//if false, connection will be closed after sending response
	bool processRequest(HttpRequest& req)
	{
		if (req.responseCode == 0) //if not yet errored
		{
			try
			{
				myListener = parentServer.getListener(remoteEndpoint,req.path);
				if (!myListener) //invalid path
				{
					req.responseCode = 404;
					req.responseString = "Not found";
					req.responseText = "Invalid path: " + req.path + '\n';
				}
			}
			catch (const SystemError& e)
			{
				req.responseCode = 500;
				req.responseString = "Internal server error";
				req.responseText = string("Error ") + e.what() + "creating UDP listener: " + e.code().message() + '\n';
			}
		}

		bool allOk = false;
		if (req.responseCode == 0) //still not errored
		{
			req.responseCode = 200;
			req.responseString = "OK";
			allOk = true;
		}

		string headers;
		char tempBuf[256];
		sprintf(tempBuf,
			"HTTP/1.1 %d %s\r\n"
			"Server: %s\r\n"
			"Connection: close\r\n",
			req.responseCode,req.responseString,PROGRAM_VERSION);

		headers = tempBuf;
		if (req.responseText.length())
		{
			sprintf(tempBuf,
				"Content-Type: text/plain\r\n"
				"Content-Length: %zu\r\n",
				req.responseText.length());

			headers += tempBuf;
		}
		else
			headers += "Content-Type: application/octet-stream\r\n";

		headers += "\r\n";
		headers += req.responseText;

		sendResponse(std::make_shared<DistributedPacket>("HTTP",headers.c_str(),headers.length()));
		if (allOk && myListener)
			myListener->addClient(shared_from_this());

		return allOk;
	}

private:
	//returning false here will close connection
	bool handleLine(char* line)
	{
		usize lineLen = strlen(line);
		if (lineLen < 1)
		{
			std::cout << remoteEndpoint << " sent empty line!" << std::endl;
			return false;
		}
		if (line[lineLen-1] != '\r')
		{
			std::cout << remoteEndpoint << " sent incomplete line: |" << line << "|" << std::endl;
			return false;
		}

		line[--lineLen] = 0;
		if (tempRequest.path.size() < 1) //first line must be http path
		{
			if (strncmp(line,"GET ",4))
			{
				//invalid request type
				tempRequest.path = "ERROR";
				tempRequest.responseCode = 400;
				tempRequest.responseText = "Only GET method is allowed";
			}
			else
			{
				tempRequest.path = &line[4];
				const auto endPos = tempRequest.path.find_first_of(' ');
				if (endPos != tempRequest.path.npos)
					tempRequest.path.resize(endPos);

				if (tempRequest.path.length() < 1)
				{
					tempRequest.path = "ERROR";
					tempRequest.responseCode = 400;
					tempRequest.responseText = "No request path!";
				}
				else
				{
					std::cout << remoteEndpoint << " sent GET |" << tempRequest.path << "|" << std::endl;
				}
			}
		}
		else if (lineLen > 0)
		{
			auto colonPtr = strchr(line,':');
			if (colonPtr == nullptr)
			{
				std::cout << remoteEndpoint << " sent header line without colon: |" << line << "|" << std::endl;
			}
			else
			{
				*colonPtr = 0;
				string headerLeft = line;
				string headerRight = &colonPtr[1];
				//std::cout << remoteEndpoint << " sent header " << headerLeft << ":" << headerRight << std::endl;
				tempRequest.headers[std::move(headerLeft)] = std::move(headerRight);
			}
		}
		else //begin request as empty newline was sent
		{
			std::cout << remoteEndpoint << " wants to begin request!" << std::endl;
			
			HttpRequest theRequest = std::move(tempRequest);
			tempRequest = HttpRequest{};
			return processRequest(theRequest);			
		}

		return true;
	}

	void handleReceive(const ErrorCode& error, usize numBytes)
	{
		if (error)
		{
			if (error == boost::asio::error::eof)
				std::cout << "HTTP client " << remoteEndpoint << " cleanly closed connection." << std::endl;
			else if (error == boost::asio::error::connection_reset)
				std::cout << "HTTP client " << remoteEndpoint << " connection reset by peer." << std::endl;
			else
				std::cout << "Error " << error.message() << " receiving line from http client " << remoteEndpoint << "(bytes transferred " << numBytes << ")" << std::endl;

			return;
		}

		recvBufBytes += numBytes;
		usize currStart = 0;
		usize currEnd = 0;
		bool handleError = false;
		for (usize i=currStart; i<recvBufBytes; i++)
		{
			if (localRecvBuf[i] == '\n')
			{
				currEnd = i;
				localRecvBuf[currEnd] = 0;
				if (!handleLine(&localRecvBuf[currStart]))
					handleError = true;

				currStart = currEnd+1;
			}
		}

		if (currEnd < currStart)
		{
			const usize remainBytes = recvBufBytes - currStart;
			if (remainBytes > 0)
				memmove(localRecvBuf.data(),&localRecvBuf[currStart],remainBytes);

			recvBufBytes = remainBytes;
		}

		if (!handleError)
			initiateReceive();
	}

	void initiateSend(DistributedPacket::SharedPtr thePacket)
	{
		//std::cout << "Sending " << thePacket->contents.size() << "B " << thePacket->creationTime << " packet from " << thePacket->recvSource << " to " << remoteEndpoint << std::endl;

		currentlySending = std::move(thePacket);
		clientSocket.async_send(boost::asio::buffer(currentlySending->contents),
			boost::bind(&HttpClient::handleSend,shared_from_this(),boost::asio::placeholders::error,boost::asio::placeholders::bytes_transferred));
	}

	void handleSend(const ErrorCode& error, usize bytesWritten)
	{
		if (error)
			std::cout << "Error " << error.message() << " sending data to http client " << remoteEndpoint << "(bytes transferred " << bytesWritten << ")" << std::endl;

		if (!error && currentlySending && currentlySending->nextPacket)
			initiateSend(currentlySending->nextPacket);
		else
			currentlySending.reset();
	}
private:
	HttpServer& parentServer;
	tcp::socket clientSocket;
	TcpEndpoint remoteEndpoint;
	usize recvBufBytes = 0;
	HttpRequest tempRequest;
	std::shared_ptr<ListenerBase> myListener;	
	std::array<char,LINE_BUFFER_SIZE> localRecvBuf;
	DistributedPacket::SharedPtr currentlySending;
};


void HttpServer::initiateAccept() 
{
	acceptor.async_accept(tempSocket,
		boost::bind(&HttpServer::handleAccept,this,boost::asio::placeholders::error));
}

void HttpServer::handleAccept(const ErrorCode& error) 
{
	if (!error) 
	{
		std::make_shared<HttpClient>(*this,std::move(tempSocket))->initiateReceive();
	}
	else 
	{
		std::cerr << "HTTP server accept error: " << error.message() << std::endl;
		return;
	}

	tempSocket = tcp::socket(ioCtx);
	initiateAccept();
}

#include <boost/program_options.hpp>
namespace po = boost::program_options;

int main(int argc, char* argv[]) 
{
	ushort httpPort = 6033;
	string httpAddress = "0.0.0.0";	
	string httpFolder = "udp";
	int pipeRecvBufSize = 1316;

	TcpEndpoint httpListenEndpoint;
	try 
	{
		po::options_description desc("Allowed options");
		desc.add_options()
			("help,h", "produce help message")
			("bufsize,b", po::value<int>(&pipeRecvBufSize)->default_value(pipeRecvBufSize), "Size of stdin receive buffer size in bytes")
			("address,a", po::value<std::string>(&httpAddress)->default_value(httpAddress), "IP address to bind HTTP server on")
			("port,p", po::value<unsigned short>(&httpPort)->default_value(httpPort), "Port to listen for HTTP requests on")
			("folder,f", po::value<std::string>(&httpFolder)->default_value(httpFolder), "HTTP requests must start with this folder");

		po::variables_map vm;
		po::store(po::parse_command_line(argc,argv,desc), vm);
		po::notify(vm);

		if (vm.count("help")) 
		{
			std::cout << desc << "\n";
			return 1;
		}

		httpListenEndpoint = TcpEndpoint(boost::asio::ip::make_address(httpAddress),httpPort);
	}
	catch (std::exception& e) 
	{
		std::cerr << "Error parsing options: " << e.what() << std::endl;
		return 1;
	}

	IoContext ioCtx;
	std::cout << "Starting " << PROGRAM_VERSION << " HTTP server on " << httpListenEndpoint << " with folder name " << httpFolder << std::endl;
#if defined(BOOST_ASIO_HAS_POSIX_STREAM_DESCRIPTOR)
	if (pipeRecvBufSize > 0)
	{
		g_pipeListener = std::make_shared<PosixListener>(ioCtx,STDIN_FILENO,pipeRecvBufSize);
		g_pipeListener->initiateReceive();
	}	
#endif
	HttpServer server(ioCtx,httpListenEndpoint,httpFolder);
	ioCtx.run();

#if defined(BOOST_ASIO_HAS_POSIX_STREAM_DESCRIPTOR)
	g_pipeListener.reset();
#endif
	return 0;
}