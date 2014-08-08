package hybridController;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;

import hybridConfig.HConfig;

public class HybridController extends Thread {
	public static int port = 8899;
	public static SocketTask[] sendTask;
	public static SocketTask[] receiveTask;
	public static boolean[] inPosition;
	public static boolean[] senderInPosition;
//	public static boolean[] endTest;
	public static int typeNumber = 18;
	public static int totalTime = 30; //minutes
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
		int tenantNumber = 3000;
		if(args.length > 0){
			tenantNumber = Integer.parseInt(args[0]);
		}
		HConfig.init(tenantNumber);
		init();
		socketServer = new SocketServer();
		socketServer.start();
		controller = new HybridController();
		controller.start();
		//check whether it is needed to move data between mysql and voltdb 
		//use sendTask[TYPE].sendInfo(tenantId, usingV, usingPV);
	}
	
	public ArrayList<ArrayList<Integer>> tenantInVoltdb = new ArrayList<ArrayList<Integer>>();
	public void run(){
		try{
			BufferedReader burstReader = new BufferedReader(new FileReader("data/burst.txt"));
			String str;
			while((str = burstReader.readLine()) != null){
				String[] offloadingIds = str.split(" ");
				ArrayList<Integer> tmp = new ArrayList<Integer>();
				for(int i = 0; i < offloadingIds.length; i++){
					tmp.add(Integer.parseInt(offloadingIds[i].trim()));
				}
				tenantInVoltdb.add(tmp);
			}
			synchronized(this){
				this.wait();
			}
			for(int i = 0; i < tenantInVoltdb.size(); i++){
				ArrayList<Integer> row = tenantInVoltdb.get(i);
				
//				if(i == 0){
//					for(int j = 1; j < row.size(); j++){
//						new DataMover("jdbc:mysql://127.0.0.1/tpcc3000", "remote", "remote", "127.0.0.1", row.get(j), true).start();
//					}
//				}
//				HybridController.sleep(30*1000);
//				if(i != 1){
//					ArrayList<Integer> lastRow = tenantInVoltdb.get(i -1);
//					for(int j = 1; j < lastRow.size(); j++){
//						if(row.contains(lastRow.get(j)) == false){
//							new DataMover("jdbc:mysql://127.0.0.1/tpcc3000", "remote", "remote", "127.0.0.1", lastRow.get(j), false).start();
//						}
//					}
//				}
//				HybridController.sleep(4*60*1000);
//				if(i != (tenantInVoltdb.size() -1)){
//					ArrayList<Integer> nextRow = tenantInVoltdb.get(i + 1);
//					for(int j = 1; j < nextRow.size(); j++){
//						if(row.contains(nextRow.get(j)) == false){
//							new DataMover("jdbc:mysql://127.0.0.1/tpcc3000", "remote", "remote", "127.0.0.1", nextRow.get(j), true).start();
//						}
//					}
//				}
//				HybridController.sleep(30*1000);
//				if(i == tenantInVoltdb.size() -1){
//					for(int j = 1; j < row.size(); j++){
//						new DataMover("jdbc:mysql://127.0.0.1/tpcc3000", "remote", "remote", "127.0.0.1", row.get(j), false).start();
//					}
//				}
				
				if(i == 0){
					if(row.size() > 1){
						for(int j = 1; j < row.size(); j++){
							new DataMover("jdbc:mysql://127.0.0.1/tpcc3000", "remote", "remote", "127.0.0.1", row.get(j), true).start();
						}
					}
				}else{
					ArrayList<Integer> lastRow = tenantInVoltdb.get(i-1);
					for (int j = 1; j < row.size(); j++) {
						if (lastRow.contains(row.get(j)) == false)
							new DataMover("jdbc:mysql://127.0.0.1/tpcc3000", "remote", "remote", "127.0.0.1", row.get(j), true).start();
					}
					HybridController.sleep(30000);
					for (int j = 1; j < lastRow.size(); j++) {
						if (row.contains(lastRow.get(j)) == false)
							new DataMover("jdbc:mysql://127.0.0.1/tpcc3000", "remote", "remote", "127.0.0.1", lastRow.get(j), false).start();
					}
				}
				HybridController.sleep(5*60*1000 - 30000);
			}
			FileWriter fstream = null;
			try {
				fstream = new FileWriter("violation.txt", false);
				BufferedWriter out = new BufferedWriter(fstream);
				for(int i = 0; i < HybridController.totalTime; i++){
					out.write(i+" "+HybridController.lateTenant[i]+" "+HybridController.lateQuery[i]);
					out.newLine();
				}
				out.flush();
				out.close();
			} catch (IOException e1) {
				e1.printStackTrace();
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
