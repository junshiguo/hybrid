package voltdbTest;

import org.voltdb.client.Client;

import utility.DBManager;
import utility.Sequence;


public class VTenant extends Thread {
	public static VTenant[] tenants;
	public static void init(int numberOfConnection, String serverlist, boolean copyTable){
		tenants = new VTenant[numberOfConnection];
		for(int i=0; i<numberOfConnection; i++){
			tenants[i] = new VTenant(i, serverlist, copyTable);
		}
	}
	
	public static void main(String[] args){
		//new Tenant(1, )
	}
	
	public int id;
	public String serverlist;;
	public Client voltdbConn;
	public Sequence sequence;
	public boolean isLoaded = false;
	public VTenant(int id, String serverlist, boolean copyTable){
		this.id = id;
		this.serverlist = serverlist;
		sequence = new Sequence();
		sequence.initSequence(100, 0);
	}
	
	public void setSequence(double writePercent){
		int w = (int) (100*writePercent);
		int r = 100 - w;
		this.sequence.initSequence(r, w);
	}
	
	public void run(){
		voltdbConn = DBManager.connectVoltdb(serverlist);
		if(voltdbConn == null) {
			System.out.println("VoltDB connection problem...");
			return;
		}
		System.out.println("thread "+id+": VoltDB connected...");
		
		new VDriver(id);
		
		try {
			voltdbConn.close();
		} catch ( InterruptedException e) {
			e.printStackTrace();
		}
	}
	
}