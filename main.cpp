#include <ctime>
#include <iostream>
#include <string>
#include <map>
#include <memory>
#include <boost/bind/bind.hpp>
#include <boost/asio.hpp>

using u8 = std::uint8_t;
using u16 = std::uint16_t;
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

class Subscriber
{
public:
	virtual ~Subscriber() {}
	virtual void sendResponse(const void* dataBuf, usize dataLen) = 0;
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
	usize addClient(const std::shared_ptr<Subscriber>& newClient) override
	{
		const usize oldNumClients = clients.size();
		auto it = std::find_if(clients.cbegin(),clients.cend(),[&newClient](const auto& wp) { return wp.lock() == newClient; });
		if (it != clients.cend())
			return oldNumClients;

		clients.push_back(newClient);
		return oldNumClients;
	}
	usize delClient(const std::weak_ptr<Subscriber>& oldClient) override
	{
		const usize oldNumClients = clients.size();
		//Remove matching weak_ptrs using the erase-remove idiom
		clients.erase(std::remove_if(clients.begin(),clients.end(),[&oldClient](const auto& wp) { return IsWeakPtrEqual(wp,oldClient); }),clients.end());
		return oldNumClients;
	}
protected:
	virtual usize sendToClients(const void* dataBuf, usize numBytes)
	{
		const usize oldNumClients = clients.size();
		if (oldNumClients > 0)
		{
			//Remove expired weak_ptrs using the erase-remove idiom
			clients.erase(std::remove_if(clients.begin(),clients.end(),[](const auto& wp) { return wp.expired(); }),clients.end());
		}

		if (numBytes)
		{
			for (auto& wp : clients)
			{
				auto sp = wp.lock();
				if (!sp)
					continue;

				sp->sendResponse(dataBuf,numBytes);
			}
		}
		return oldNumClients;
	}

	std::vector<std::weak_ptr<Subscriber>> clients;
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
			boost::bind(&UdpListener::handleReceive,this,
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
	usize addClient(const std::shared_ptr<Subscriber>& newClient) override
	{
		const usize oldNumClients = ListenerBase::addClient(newClient);
		const usize newNumClients = clients.size();

		if (newNumClients != oldNumClients)
			std::cout << "UDP listener " << bindAddr << " number of clients " << oldNumClients << " -> " << clients.size() << std::endl;

		return oldNumClients;
	}
	usize delClient(const std::weak_ptr<Subscriber>& oldClient) override
	{
		const usize oldNumClients = ListenerBase::delClient(oldClient);
		const usize newNumClients = clients.size();

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

		const usize oldNumClients = ListenerBase::sendToClients(localRecvBuf.data(),numBytes);
		const usize newNumClients = clients.size();
		if (newNumClients != oldNumClients)
		{
			std::cout << "UDP listener " << bindAddr << " number of clients " << oldNumClients << " -> " << newNumClients << std::endl;
		}

		if (!error)
			initiateReceive();
	}

	udp::socket udpSocket;
	UdpEndpoint bindAddr;
	UdpEndpoint currSource;
	std::array<uint8_t,RECEIVE_BUFFER_SIZE> localRecvBuf;
};

#if defined(BOOST_ASIO_HAS_POSIX_STREAM_DESCRIPTOR)
class PosixListener : public ListenerBase, public std::enable_shared_from_this<PosixListener>
{
	static constexpr usize RECEIVE_BUFFER_SIZE = 65536;
public:
	PosixListener(IoContext& ioCtx, int fileNo) : pipeStream(ioCtx), theFileNo(fileNo)
	{ 
		pipeStream.assign(fileNo);
		std::cout << "Pipe listener created for " << fileNo << std::endl;
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
			boost::bind(&PosixListener::handleReceive,this,
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
	usize addClient(const std::shared_ptr<Subscriber>& newClient) override
	{
		const usize oldNumClients = ListenerBase::addClient(newClient);
		const usize newNumClients = clients.size();

		if (newNumClients != oldNumClients)
			std::cout << "Pipe listener for " << theFileNo << " number of clients " << oldNumClients << " -> " << clients.size() << std::endl;

		return oldNumClients;
	}
	usize delClient(const std::weak_ptr<Subscriber>& oldClient) override
	{
		const usize oldNumClients = ListenerBase::delClient(oldClient);
		const usize newNumClients = clients.size();

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

		const usize oldNumClients = ListenerBase::sendToClients(localRecvBuf.data(),numBytes);
		const usize newNumClients = clients.size();
		if (newNumClients != oldNumClients)
		{
			std::cout << "Pipe listener for " << theFileNo << " number of clients " << oldNumClients << " -> " << newNumClients << std::endl;
		}

		if (!error)
			initiateReceive();
	}

	int theFileNo = 0;
	boost::asio::posix::stream_descriptor pipeStream;
	std::array<uint8_t,RECEIVE_BUFFER_SIZE> localRecvBuf;
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
		char tempBuf[256];
		sprintf(tempBuf,"%s:%d",theEndpoint.address().to_string().c_str(),static_cast<int>(theEndpoint.port()));
		outStr = tempBuf;
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
		if (!strcmp(actualPath,"-"))
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
		}

		if (udpIt == listeners.end())
			listeners.emplace(std::move(parsedEndpoint),retListener);
		else
			udpIt->second = retListener;

		retListener->initiateReceive();
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
	static constexpr usize SEND_BUFFER_SIZE = 4096;
public:
	HttpClient(HttpServer& theServer, tcp::socket&& acceptedSock) : parentServer(theServer), clientSocket(std::move(acceptedSock)) { remoteEndpoint = clientSocket.remote_endpoint(); }
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

	void sendResponse(const void* dataBuf, usize dataLen)
	{
		if (dataLen < 1)
			return;

		const usize oldSize = queuedData.size();
		const usize newSize = oldSize + dataLen;
		queuedData.resize(newSize);
		memcpy(&queuedData[oldSize],dataBuf,dataLen);
		if (!sendingData)
			initiateSend();
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
			"Server: UdpServer 0.1\r\n"
			"Connection: close\r\n",
			req.responseCode,req.responseString);

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

		sendResponse(headers.c_str(),headers.length());
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
				std::cout << remoteEndpoint << " sent header " << headerLeft << ":" << headerRight << std::endl;
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

	bool initiateSend()
	{
		if (sendingData || queuedData.size() < 1)
			return false;

		usize bytesToSend = 0;
		if (queuedData.size() > localSendBuf.size())
		{
			bytesToSend = localSendBuf.size();
			memcpy(localSendBuf.data(),queuedData.data(),bytesToSend);
			const usize remainingDataLen = queuedData.size()-bytesToSend;
			memmove(queuedData.data(),&queuedData[bytesToSend],remainingDataLen);
			queuedData.resize(remainingDataLen);
		}
		else
		{
			bytesToSend = queuedData.size();
			memcpy(localSendBuf.data(),queuedData.data(),bytesToSend);
			queuedData.resize(0);
		}

		sendingData = true;
		clientSocket.async_send(boost::asio::buffer(localSendBuf.data(),bytesToSend),
			boost::bind(&HttpClient::handleSend,shared_from_this(),boost::asio::placeholders::error,boost::asio::placeholders::bytes_transferred));
		return true;
	}

	void handleSend(const ErrorCode& error, usize bytesWritten)
	{
		if (error)
			std::cout << "Error " << error.message() << " sending data to http client " << remoteEndpoint << "(bytes transferred " << bytesWritten << ")" << std::endl;

		sendingData = false;
		if (queuedData.size() > 0)
			initiateSend();
	}
private:
	HttpServer& parentServer;
	tcp::socket clientSocket;
	TcpEndpoint remoteEndpoint;
	usize recvBufBytes = 0;
	std::shared_ptr<ListenerBase> myListener;
	bool sendingData = false;
	ByteVector queuedData;
	HttpRequest tempRequest;
	std::array<char,LINE_BUFFER_SIZE> localRecvBuf;
	std::array<uint8_t,SEND_BUFFER_SIZE> localSendBuf;
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

	TcpEndpoint httpListenEndpoint;
	try 
	{
		po::options_description desc("Allowed options");
		desc.add_options()
			("help,h", "produce help message")
			("address,a", po::value<std::string>(&httpAddress)->default_value("0.0.0.0"), "IP address to bind HTTP server on")
			("port,p", po::value<unsigned short>(&httpPort)->default_value(6033), "Port to listen for HTTP requests on")
			("folder,f", po::value<std::string>(&httpFolder)->default_value("udp"), "HTTP requests must start with this folder");

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

	std::cout << "Using IP address: " << httpAddress << std::endl;
	std::cout << "Listening on port: " << httpPort << std::endl;
	std::cout << "Folder name: " << httpFolder << std::endl;

	IoContext ioCtx;
#if defined(BOOST_ASIO_HAS_POSIX_STREAM_DESCRIPTOR)
	g_pipeListener = std::make_shared<PosixListener>(ioCtx,STDIN_FILENO);
	g_pipeListener->initiateReceive();
#endif
	HttpServer server(ioCtx,httpListenEndpoint,httpFolder);
	ioCtx.run();

	return 0;
}