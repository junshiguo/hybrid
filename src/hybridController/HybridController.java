package hybridController;

import hybridConfig.HConfig;

public class HybridController {
	public static int port = 8899;
	public static SocketTask[] sendTask;
	public static SocketTask[] receiveTask;
	public static boolean[] inPosition;
	public static boolean[] endTest;
	public static int typeNumber = 18;
	public static int totalTime = 60;
	public static boolean ENDTEST = false;
	public static boolean[] usingVoltdb;
	public static boolean[] partiallyUsingVoltdb;
	public static int[] lateTenant;
	public static int[] lateQuery;
	
	public static void main(String[] args){
		HConfig.init(1000);
		
	}
	
	public static void init(){
		ENDTEST = false;
		sendTask = new SocketTask[typeNumber];
		receiveTask = new SocketTask[typeNumber];
		inPosition = new boolean[typeNumber];
		endTest = new boolean[typeNumber];
		for(int i = 0; i < typeNumber; i++){
			inPosition[i] = false;
			endTest[i] = false;
		}
		usingVoltdb = new boolean[HConfig.totalTenant];
		partiallyUsingVoltdb = new boolean[HConfig.totalTenant];
		for(int i = 0; i < HConfig.totalTenant; i++){
			usingVoltdb[i] = false;
			partiallyUsingVoltdb[i] = false;
		}
		lateTenant = new int[totalTime];
		lateQuery = new int[totalTime];
		for(int i = 0; i < totalTime; i++){
			lateTenant[i] = lateQuery[i] = 0;
		}
	}
	
}
