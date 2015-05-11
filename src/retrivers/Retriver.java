package retrivers;

import java.util.ArrayList;
import java.util.List;

/**
 * 	new Retriver(tenantId, volumnId);...
 * 	Retriver.startRetrive();
 * @author guojunshi
 *
 */
public class Retriver {
	
	public static void main(String[] args) throws InterruptedException{
		new Retriver(1,1);
		new Retriver(2,2);
		long start = System.nanoTime();
		Retriver.startRetrive();
		long end = System.nanoTime();
		System.out.println("time: "+(end-start)/1000000000.0+" s");
	}
	
	public static String voltdbServer = "10.171.5.28";
	public static String dbURL = "jdbc:mysql://10.171.5.28/tpcc3000";
	public static String dbUsername = "remote";
	public static String dbPassword = "remote";
	public static void setDBInfo(String server, String dbusername, String dbpassword){
		Retriver.voltdbServer = server;
		Retriver.dbURL = "jdbc:mysql://"+server+"/tpcc3000";
		Retriver.dbUsername = dbusername;
		Retriver.dbPassword = dbpassword;
	}
	
	public static int concurrency = 10;
	public static void startRetrive() throws InterruptedException{
		RetriveThread[] threads = new RetriveThread[concurrency];
		for(int i = 0; i < concurrency; i++){
			threads[i] = new RetriveThread();
			threads[i].start();
		}
		for(int i = 0; i < concurrency; i++){
			threads[i].join();
		}
	}

	public static List<Retriver> todo = new ArrayList<Retriver>();
	public static synchronized Retriver next(){
		if(todo.size() == 0) return null;
		Retriver tmp = todo.get(0);
		todo.remove(0);
		return tmp;
	}
	
	public int tenantId, volumnId;
	public Retriver(int tenantId, int volumnId){
		this.tenantId = tenantId;
		this.volumnId = volumnId;
		todo.add(this);
	}

}
