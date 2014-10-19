package pooling;

public class PController {
	public static int connNumber = 10;
	public static PConnection[] connections;
	public static String dbURL = "jdbc:mysql://10.20.2.211:3306/tpcc3000";
	public static String dbUsername = "remote";
	public static String dbPassword = "remote";
	public static String voltdbServer = "10.20.2.211";
	public static boolean isAlive = true;
	public static synchronized boolean checkState(boolean togle){
		if(togle == true){
			isAlive = !isAlive;
		}
		return isAlive;
	}
	
	public static void init(){
		connNumber = 10;
		connections = new PConnection[connNumber];
		for(int i = 0; i < connNumber; i++){
			connections[i] = new PConnection(i, dbURL, dbUsername, dbPassword, voltdbServer);
			connections[i].start();
		}
	}
	
	public static void stop(){
		checkState(true);
	}
	
	public synchronized static void addToList(int tenantId, int sqlId, Object[] para, int[] paraType, int paraNumber, int PKNumber){
		PRequest t = new PRequest(tenantId, sqlId, para, paraType, paraNumber, PKNumber, System.nanoTime());
		PRequest.gsRequest(false, t);
	}
	
}
