package hybridTest;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

import hybridConfig.HConfig;

public class Main extends Thread {
	public static int totalTenantNumber;
	public static int tenantNumber;
	public static HTenant[] tenants;
	public static int IDStart;
	public static int TYPE = 0;
	public static int QTMax;
	public static String dbURL = "jdbc:mysql://10.20.2.211:3306/tpcc3000";
	public static String dbUsername = "remote";
	public static String dbPassword = "remote";
	public static String voltdbServer = "10.20.2.211";
	public static String loadPath = "data/load.txt";

	public static boolean[] usingVoltdb; //set by setDBState
	public static boolean[] partiallyUsingVoltdb; //set by setDBState
	public static int[] throughputPerTenant; //planned throughput, set by setQT()
	public static int[] actualThroughputPerTenant; // updated in Main per minute
	public static ArrayList<Long> timePerQuery = new ArrayList<Long>();
	public static double concurrency = 0.1; //set by setConcurrency
	
	public static int port = 8899;
	public static String SocketServer = "10.20.2.211";
	public static SocketSender socketSender;
	public static StateReceiver socketReceiver;
	public static PerformanceMonitor performanceMonitor;
	public static int MAXRETRY = 2;
	
//	public static boolean sendRequest = false;
//	public static boolean startCount = false;
//	public static double writePercent = 0.0; //set in performanceMonitor
	public static boolean onlyMysql = true;
	public static long testTime = 1800000; //30 mins
	public static long intervalTime = 300000; //5 min, must be integer mins
	public static long intervalNumber = 3;
	public static long minPerInterval = 5;
	public static boolean isActive = true;
	public static boolean startTest = false;
	public static boolean socketWorking = true;
	
	public static Main mainThread;
	public static int setQTNow = 0;
	public static int doSQLNow = 0;
	public static int resetActualQT = 0;
	
	public static int checkTp = 60; //check throughput every 60 seconds
	
	public static void main(String[] args) throws InterruptedException{
		TYPE = 0;
		if(args.length > 0){
			TYPE = Integer.parseInt(args[0]);
		}
		totalTenantNumber = 1000;
		if(args.length > 1){
			totalTenantNumber = Integer.parseInt(args[1]);
		}
		onlyMysql = false;
		if(args.length > 2){
			if(Integer.parseInt(args[2]) == 1)	onlyMysql = true;
			else onlyMysql = false;
		}
		testTime = 1800000;
		intervalTime = 300000;
		HConfig.init(totalTenantNumber);
		init();
		
		for(int i = 0; i < tenantNumber; i++){
			tenants[i].start();
		}
		
		socketSender = new SocketSender();
		socketReceiver = new StateReceiver();
		socketSender.start();
		socketReceiver.start();
		Thread.sleep(20000);
		socketSender.sendInfo("in position");
		synchronized(socketSender){
			socketSender.notify();
		}
		while(true){
			if(checkStart(false)){
				break;
			}
		}
		
		System.out.println("******************hybrid test start******************");
		Main.setConcurrency(0.1);
		Main.isActive = true; 
		mainThread = new Main();
		mainThread.start();
		performanceMonitor = new PerformanceMonitor(tenantNumber);
		performanceMonitor.start();
		HTimer timer = new HTimer();
		timer.start();
		try {
			timer.join();
			mainThread.join();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		try {
			Main.socketReceiver.socket.close();
		} catch (IOException e) {
		}
		System.out.println("******************hybrid test complete******************");
		
	}
	
	public void run(){
		while(true){
			try {
				synchronized(this){
					this.wait();
				}
				if(Main.checkSetQT(0) > 0){
//					Main.setQT();
					Main.setQT(Main.loadPath);
					Main.checkSetQT(-1);
				}
				if(Main.checkResetActualQT(0) > 0){
					Main.resetActualTP();
					Main.checkResetActualQT(-1);
				}
				if(Main.checkDoSQL(0) > 0){
					for(int id = 0; id < tenantNumber; id++){
						if(Main.throughputPerTenant[id] > 0){
							tenants[id].checkDoSQLNow(1);
							synchronized(tenants[id]){
								tenants[id].notify();
							}
						}
					}
					Main.checkDoSQL(-1);
				}
				if(Main.isActive == false){
					for(int id = 0; id < tenantNumber; id++){
						synchronized(tenants[id]){
							tenants[id].notify();
						}
					}
					break;
				}
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		
	}
	
	public static synchronized boolean checkStart(boolean source){
		if(source){
			Main.startTest = true;
		}
		return Main.startTest;
	}
	
	public static synchronized int checkSetQT(int action){
		if(action == 1){
			Main.setQTNow ++;;
		}else if(action == -1){
			Main.setQTNow --;
		}
		return Main.setQTNow;
	}
	
	public static synchronized int checkDoSQL(int action){
		if(action ==1){
			Main.doSQLNow ++;
		}else if(action == -1){
			Main.doSQLNow --;
		}
		return Main.doSQLNow;
	}
	
	public static synchronized int checkResetActualQT(int action){
		if(action == 1){
			Main.resetActualQT++;
		}else if(action == -1){
			Main.resetActualQT--;
		}
		return Main.resetActualQT;
	}
	
	public static void init(){
		intervalNumber = testTime / intervalTime;
		minPerInterval = intervalTime / 60000;
		tenantNumber = (int) (totalTenantNumber*HConfig.PercentTenantSplits[TYPE]);
//		tenantNumber = 1;
		if(TYPE != 0){
			IDStart = (int) (totalTenantNumber*HConfig.PercentTenantSplitsSum[TYPE-1]);
		}else{
			IDStart = 0;
		}
		Main.QTMax = HConfig.QTMatrix[TYPE];
		tenants = new HTenant[tenantNumber];
		usingVoltdb = new boolean[tenantNumber];
		partiallyUsingVoltdb = new boolean[tenantNumber];
		throughputPerTenant = new int[tenantNumber];
		actualThroughputPerTenant = new int[tenantNumber];
		for(int i = 0; i < tenantNumber; i++){
			tenants[i] = new HTenant(i+IDStart, HConfig.DSMatrix[TYPE], HConfig.QTMatrix[TYPE], HConfig.WHMatrix[TYPE], Main.dbURL, Main.dbUsername, Main.dbPassword, Main.voltdbServer);
			usingVoltdb[i] = false;
			partiallyUsingVoltdb[i] = false;
			throughputPerTenant[i] = 0;
			actualThroughputPerTenant[i] = 0;
		}
	}

	public static void setConcurrency(double cc){
		concurrency = cc;
	}
	
	public static void setQT(){
		for(int i = 0; i < tenantNumber; i++){
			Main.throughputPerTenant[i] = HConfig.getQT(i+IDStart, false);
			Main.tenants[i].setQT(Main.throughputPerTenant[i]);
		}
	}
	
	public static boolean loadLoaded = false;
	public static BufferedReader loadReader;
	public static void setQT(String file){
		String str = null;
		String[] workload;
		try{
			if(loadLoaded == false){
				loadReader = new BufferedReader(new FileReader(file));
				loadLoaded = true;
			}
			str = loadReader.readLine();
			if(str == null) return;
			workload = str.split(" ");
			for(int i = 0; i < tenantNumber; i++){
				Main.throughputPerTenant[i] = Integer.parseInt(workload[i+Main.IDStart+1]);
//				Main.tenants[i].setQT(Main.throughputPerTenant[i]);
			}
		}catch(IOException e){
			
		}
	}
	
	public static void resetActualTP(){
		for(int i = 0; i < Main.tenantNumber; i++){
			Main.actualThroughputPerTenant[i] = 0;
			Main.tenants[i].setQT(Main.throughputPerTenant[i]);
		}
	}
	
	public static void setDBState(int tenantId, boolean usingV, boolean usingPV){
		int index = tenantId - Main.IDStart;
		Main.usingVoltdb[index] = usingV;
		Main.partiallyUsingVoltdb[index] = usingPV;
//		if(usingV == false && usingPV == false){
//			try {
//				Main.tenants[index].connection.voltdbConn.close();
//			} catch (InterruptedException e) {
//				System.out.println("error in closing voltdb connection...");
//			}
//		}
	}
	
	public static void setDBState(int tenantId, int usingV, int usingPV, int volumnId){
		int index = tenantId - Main.IDStart;
		if(usingV == 1)	Main.usingVoltdb[index] = true;
		else Main.usingVoltdb[index] = false;
		if(usingPV == 1)	Main.partiallyUsingVoltdb[index] = true;
		else Main.partiallyUsingVoltdb[index] = false;
//		if(usingV == 0 && usingPV == 0){
//			try {
//				Main.tenants[index].connection.voltdbConn.close();
//			} catch (InterruptedException e) {
//				System.out.println("error in closing voltdb connection...");
//			}
//		}
		Main.tenants[index].idInVoltdb = volumnId;
	}
	
}
