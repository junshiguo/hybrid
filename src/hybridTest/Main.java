package hybridTest;

import hybridConfig.HConfig;

import java.util.Random;

import utility.Support;

public class Main {
	public static int totalTenantNumber;
	public static int tenantNumber;
	public static HTenant[] tenants;
	public static int IDStart;
	public static int TYPE = 0;
	public static int QTMax;
	public static String dbURL = "jdbc:mysql://127.0.0.1:3306/tpcc10";
	public static String dbUsername = "remote";
	public static String dbPassword = "remote";
	public static String voltdbServer = "127.0.0.1";

	public static boolean[] usingVoltdb; //set by setDBState
	public static boolean[] partiallyUsingVoltdb; //set by setDBState
	public static int[] throughputPerTenant; //planned throughput, set by setQT()
	public static double concurrency = 0.1; //set by setConcurrency
	
	public static int port = 8899;
	public static String SocketServer = "127.0.0.1";
	public static SocketSender socketSender = new SocketSender();
	public static StateReceiver socketReceiver = new StateReceiver();
	public static int MAXRETRY = 2;
	
//	public static boolean sendRequest = false;
//	public static boolean startCount = false;
//	public static double writePercent = 0.0; //set in performanceMonitor
	public static boolean onlyMysql = true;
	public static long testTime = 900000; //15 mins
	public static long intervalTime = 300000; //5 min, must be integer mins
	public static long intervalNumber = 3;
	public static long minPerInterval = 5;
	public static boolean isActive = true;
	public static boolean startTest = false;
	
	public static int checkTp = 60; //check throughput every 60 seconds
	
	public static void main(String[] args){
		totalTenantNumber = 1000;
		TYPE = 1;
		onlyMysql = true;
		testTime = 900000;
		intervalTime = 300000;
		HConfig.init(1000);
		init();
		
		for(int i = 0; i < tenantNumber; i++){
			tenants[i].start();
		}
		socketSender = new SocketSender();
		socketReceiver = new StateReceiver();
		socketSender.run();
		socketReceiver.run();
		socketSender.sendInfo("in position");
		while(true){
			if(startTest == true){
				break;
			}
		}
		
		System.out.println("******************hybrid test start******************");
		Main.setConcurrency(0.1);
		Main.isActive = true; 
		for(int i=0; i<intervalNumber; i++){
			try {
				Main.setQT();
				
				for(int j = 0; j < minPerInterval; j++){
					for(int k = 0; k < 20; k++){ //send requests every 3 seconds, 20 times per minute
						for(int id = 0; id < tenantNumber; id++){
							tenants[id].doSQLNow++;
						}
						Thread.sleep(3000);
					}
					//check throughput, send data to PerformanceController: lateTenant, lateQuery&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
					socketSender.sendInfo((i*minPerInterval+j),throughputPerTenant.clone(), PerformanceMonitor.actualThroughputPerTenant.clone());
				}
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		Main.isActive = false;
		System.out.println("******************hybrid test complete******************");
		System.exit(1);
		
	}
	
	public static void init(){
		intervalNumber = testTime / intervalTime;
		minPerInterval = intervalTime / 60000;
		tenantNumber = (int) (totalTenantNumber*HConfig.PercentTenantSplits[TYPE]);
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
		for(int i = 0; i < tenantNumber; i++){
			tenants[i] = new HTenant(i+IDStart, HConfig.DSMatrix[TYPE], HConfig.QTMatrix[TYPE], HConfig.WHMatrix[TYPE], Main.dbURL, Main.dbUsername, Main.dbPassword, Main.voltdbServer);
			usingVoltdb[i] = false;
			partiallyUsingVoltdb[i] = false;
			throughputPerTenant[i] = 0;
		}
	}

	public static void setConcurrency(double cc){
		concurrency = cc;
	}
	
	public static void setQT(){
		int activeTenantNumber = (int) (tenantNumber * concurrency);
		int[] activeTenant = Support.Rands(IDStart, tenantNumber, activeTenantNumber);
		boolean[] flags = new boolean[tenantNumber];
		for(int i = 0; i < activeTenantNumber; i++){
			flags[activeTenant[i]] = true; 
		}
		for(int i = 0; i < tenantNumber; i++){
			if(flags[i]){
				Main.throughputPerTenant[i] = Main.QTMax;
			}else{
				Random ran = new Random();
				switch(Main.QTMax){
				case 20:
					Main.throughputPerTenant[i] = 0;
					break;
				case 60:
					int tt = ran.nextInt(3);
					if(tt == 0)	Main.throughputPerTenant[i] = 0;
					else if(tt == 1) Main.throughputPerTenant[i] = 20;
					else Main.throughputPerTenant[i] = 40;
					break;
				case 100:
					int ttt = ran.nextInt(4);
					if(ttt == 0)	Main.throughputPerTenant[i] = 20;
					else if(ttt == 1) Main.throughputPerTenant[i] = 40;
					else if(ttt == 2) Main.throughputPerTenant[i] = 60;
					else Main.throughputPerTenant[i] = 0;
					break;
				}
			}
			Main.tenants[i].setQT(Main.throughputPerTenant[i]);
		}
	}
	
	public static void setDBState(int tenantId, boolean usingV, boolean usingPV){
		int index = tenantId - Main.IDStart;
		Main.usingVoltdb[index] = usingV;
		Main.partiallyUsingVoltdb[index] = usingPV;
	}
	
	public static void setDBState(int tenantId, int usingV, int usingPV){
		int index = tenantId - Main.IDStart;
		if(usingV == 1)	Main.usingVoltdb[index] = true;
		else Main.usingVoltdb[index] = false;
		if(usingPV == 1)	Main.partiallyUsingVoltdb[index] = true;
		else Main.partiallyUsingVoltdb[index] = false;
	}
	
}
