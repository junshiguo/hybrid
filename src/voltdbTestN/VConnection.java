package voltdbTestN;

import org.voltdb.client.Client;

import utility.DBManager;

public class VConnection extends Thread {
	int ID;
	String serverlist = "127.0.0.1";
	Client voltdbConn;
	VTenant father;
	
	public VConnection(String sl, VTenant f, int ID){
		this.father = f;
		this.serverlist = sl;
		this.ID = ID;
	}
	
	public void run(){
		voltdbConn = DBManager.connectVoltdb(serverlist);
		if(voltdbConn == null) {
			System.out.println("VoltDB connection problem...");
			return;
		}
		
		new VDriver(father.id, ID);
		
		try {
			voltdbConn.close();
		} catch ( InterruptedException e) {
			e.printStackTrace();
		}
	}

}
