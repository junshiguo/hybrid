package hybridController;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

import hybridConfig.HConfig;

public class HybridController extends Thread {
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
	public static boolean controllerWorking = false;
	public static HybridController controller;
	
	public static void main(String[] args){
		HConfig.init(1000);
		init();
		socketServer = new SocketServer();
		socketServer.start();
		controller = new HybridController();
		controller.start();
		//check whether it is needed to move data between mysql and voltdb 
		//use sendTask[TYPE].sendInfo(tenantId, usingV, usingPV);
	}
	
	public void run(){
		try{
			synchronized(this){
				this.wait();
			}
			BufferedReader burstReader = new BufferedReader(new FileReader("data/burst.txt"));
			while(true){
				String str = burstReader.readLine();
				if(str == null){
					break;
				}
				String[] offloadingIds = str.split(" ");
				int time = Integer.parseInt(offloadingIds[0].trim());
				System.out.println(time);
				HybridController.sleep(time*60*1000);
				for(int i = 1; i < offloadingIds.length - 1; i++){
					new DataMover("jdbc:mysql://127.0.0.1/tpcc10", "remote", "remote", "127.0.0.1", Integer.parseInt(offloadingIds[i].trim()), true).start();
				}
				HybridController.sleep(5*60*1000);
				for(int i = 1; i < offloadingIds.length - 1; i++){
					new DataMover("jdbc:mysql://127.0.0.1/tpcc10", "remote", "remote", "127.0.0.1", Integer.parseInt(offloadingIds[i].trim()), false).start();
				}
			}
		}catch (IOException | InterruptedException | NumberFormatException e) {
				e.printStackTrace();
		}
	}
	
	public static synchronized void launchTask(SocketTask st, int type, boolean isSender){
		if(isSender){
			sendTask[type] = st;
			senderInPosition[type] = true;
		}else{
			receiveTask[type] = st;
			inPosition[type] = true;
		}
		
		if(informStart == true)	return;
//		sendTask[type].sendInfo("all in position");
		
//		if(inPosition[1] == true && senderInPosition[1] == true && inPosition[6] == true && senderInPosition[6] == true){
//			sendTask[1].sendInfo("all in position");
//			sendTask[6].sendInfo("all in position");
//			informStart = true;
//		}
		boolean flag = true;
		for(int i = 0; i < typeNumber; i++){
			if(inPosition[i] == false || senderInPosition[i] == false)
				flag = false;
		}
		if(flag){
			for(int i = 0; i < typeNumber; i++){
				sendTask[i].sendInfo("all in position");
			}
			informStart = true;
			synchronized(HybridController.controller){
				controller.notify();
			}
		}
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
