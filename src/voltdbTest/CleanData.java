package voltdbTest;

import java.io.IOException;

import org.voltdb.client.Client;
import org.voltdb.client.NoConnectionsException;
import org.voltdb.client.ProcCallException;

import utility.DBManager;

public class CleanData {
	public static Client client;
	
	public static void main(String[] args) throws NoConnectionsException, IOException, ProcCallException{
		client = DBManager.connectVoltdb("10.20.2.211");
		for(int i = 0; i < 9; i++){
			client.callProcedure("PRTruncateAll", i);
		}
	}

}
