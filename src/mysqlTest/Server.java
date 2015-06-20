package mysqlTest;

import org.apache.thrift.transport.TTransportException;

import newhybrid.HException;
import server.HServer;

public class Server {
	public static void main(String[] args) throws HException, TTransportException{
		HServer server=new HServer();
		server.start();
	}
}
