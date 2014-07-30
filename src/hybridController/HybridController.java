package hybridController;

import hybridConfig.HConfig;

public class HybridController {
	public static int port = 8899;
	public static SocketTask[] sendTask;
	public static SocketTask[] receiveTask;
	public static boolean[] inPosition;
	public static boolean[] senderInPosition;
//	public static boolean[] endTest;
	public static int typeNumber = 18;
	public static int totalTime = 60; //minutes
//	public static boolean ENDTEST = false;
	public static boolean[] usingVoltdb;
	public static boolean[] partiallyUsingVoltdb;
	public static int[] lateTenant;
	public static int[] lateQuery;
	public static SocketServer socketServer;
	public static boolean informStart = false;
	
	public static void main(String[] args){
		HConfig.init(1000);
		init();
		socketServer = new SocketServer();
		socketServer.start();
		//check whether it is needed to move data between mysql and voltdb 
		//use sendTask[TYPE].sendInfo(tenantId, usingV, usingPV);
	}
	
	public static synchronized void launchTask(SocketTask st, int type, boolean isSender){
		if(isSender){
			sendTask[type] = st;
			senderInPosition[type] = true;
		}else{
			receiveTask[type] = st;
			inPosition[type] = true;
		}
		
//		if(informStart == true)	return;
		sendTask[type].sendInfo("all in position");
//		if(inPosition[type] == true && senderInPosition[type] == true){
//			sendTask[type].sendInfo("all in position");
//			informStart = true;
//		}
//		boolean flag = true;
//		for(int i = 0; i < typeNumber; i++){
//			if(inPosition[i] == false || senderInPosition[i] == false)
//				flag = false;
//		}
//		if(flag){
//			for(int i = 0; i < typeNumber; i++){
//				sendTask[i].sendInfo("all in position");
//			}
//		informStart = true;
//		}
	}
	
	public static void init(){
//		ENDTEST = false;
		sendTask = new SocketTask[typeNumber];
		receiveTask = new SocketTask[typeNumber];
		inPosition = new boolean[typeNumber];
		senderInPosition = new boolean[typeNumber];
//		endTest = new boolean[typeNumber];
		for(int i = 0; i < typeNumber; i++){
			inPosition[i] = false;
			senderInPosition[i] = false;
//			endTest[i] = false;
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
