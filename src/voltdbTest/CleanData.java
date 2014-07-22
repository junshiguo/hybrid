package voltdbTest;

import java.io.IOException;

import org.voltdb.client.Client;
import org.voltdb.client.NoConnectionsException;
import org.voltdb.client.ProcCallException;

import utility.DBManager;

public class CleanData {
	public static Client client;
	
	public static void main(String[] args) throws NoConnectionsException, IOException, ProcCallException, InterruptedException{
		client = DBManager.connectVoltdb("127.0.0.1");
		int tenantNumber = 100;
		for(int i = 0; i < tenantNumber; i++){
			client.callProcedure("@AdHoc", "TRUNCATE TABLE warehouse"+i);
			client.callProcedure("@AdHoc", "TRUNCATE TABLE district"+i);
			client.callProcedure("@AdHoc", "TRUNCATE TABLE item"+i);
			client.callProcedure("@AdHoc", "TRUNCATE TABLE new_orders"+i);
			client.callProcedure("@AdHoc", "TRUNCATE TABLE order_line"+i);
			client.callProcedure("@AdHoc", "TRUNCATE TABLE orders"+i);
			client.callProcedure("@AdHoc", "TRUNCATE TABLE stock"+i);
			client.callProcedure("@AdHoc", "TRUNCATE TABLE customer"+i);
			client.callProcedure("@AdHoc", "TRUNCATE TABLE history"+i);
		}
		client.close();
	}

}
