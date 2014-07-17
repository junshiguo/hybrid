package hybridTest;

import java.util.Random;

import utility.Support;

public class Main {
	public static int totalTenantNumber;
	public static int tenantNumber;
	public static Tenant[] tenants;
	public static int IDStart;
	public static int TYPE = 0;
	public static int QTMax;
	public static String dbURL = "jdbc:mysql://10.20.2.111:3306/tpcc10";
	public static String dbUsername = "remote";
	public static String dbPassword = "remote";
	public static String voltdbServer = "10.20.2.111";

	public static int[] ValueQT = { 20, 60, 100 };  // per minute
	public static double[] ValueDS = {6.9, 38.7, 142.4 };  // MB
	public static double[] ValueWP = { 0.6, 0.2 };  // +-0.05
	public static double[] PercentQT = { 0.3, 0.5, 0.2 };
	public static double[] PercentDS = { 0.6, 0.3, 0.1 };
	public static double[] PercentWH = { 0.4, 0.6 };  // 0.4 for write heavy and 0.6 for read heavy
	public static double[]  PercentTenantSplits  = {
		0.06	//QT: 20, DS: 6.9, write heavy
		,0.09	//QT: 20, DS: 6.9, read heavy
		,0.1		//QT: 20, DS: 38.7, write heavy
		,0.15	//QT: 20, DS: 38.7, read heavy
		,0.04	//QT: 20, DS: 142.4, write heavy
		,0.06	//QT: 20, DS: 142.4, read heavy
		
		,0.036	//QT: 60, DS: 6.9, write heavy
		,0.054	//QT: 60, DS: 6.9, read heavy
		,0.06	//QT: 60, DS: 38.7, write heavy
		,0.09	//QT: 60, DS: 38.7, read heavy
		,0.024	//QT: 60, DS: 142.4, write heavy
		,0.036	//QT: 60, DS: 142.4, write heavy
		
		,0.024	//QT: 100, DS: 6.9, write heavy
		,0.036	//QT: 100, DS: 6.9, read heavy
		,0.04	//QT: 100, DS: 38.7, write heavy
		,0.06	//QT: 100, DS: 38.7, read heavy
		,0.016	//QT: 100, DS: 142.4, write heavy
		,0.024	//QT: 100, DS: 142.4, write heavy
	};
	public static double[] PercentTenantSplitsSum = {
		0.06, 0.15, 0.25, 0.4, 0.44, 0.5,
		0.536, 0.59, 0.65, 0.74, 0.764, 0.8,
		0.824, 0.86, 0.9, 0.96, 0.976, 1.0
		};
	public static int QTMatrix[] = {ValueQT[0],ValueQT[0],ValueQT[0],ValueQT[0],ValueQT[0],ValueQT[0],
		ValueQT[1],ValueQT[1],ValueQT[1],ValueQT[1],ValueQT[1],ValueQT[1],
		ValueQT[2],ValueQT[2],ValueQT[2],ValueQT[2],ValueQT[2],ValueQT[2],};
	public static double DSMatrix[] = {ValueDS[0],ValueDS[0],ValueDS[1],ValueDS[1],ValueDS[2],ValueDS[2],
		ValueDS[0],ValueDS[0],ValueDS[1],ValueDS[1],ValueDS[2],ValueDS[2],
		ValueDS[0],ValueDS[0],ValueDS[1],ValueDS[1],ValueDS[2],ValueDS[2]};
	public static boolean WHMatrix[] = {true,false,true,false,true,false,true,false,true,false,true,false,true,false,true,false,true,false};
	public static boolean[] usingVoltdb;
	public static boolean[] partiallyUsingVoltdb;
	public static int[] throughputPerTenant; //planned throughput
	public static double concurrency = 0.1;
	
//	public static boolean sendRequest = false;
	public static boolean startCount = false;
	public static double writePercent = 0.0; //set in performanceMonitor
	public static boolean onlyMysql = true;
	public static long testTime = 900000; //15 mins
	public static long intervalTime = 300000; //5 min, must be integer mins
	public static long intervalNumber = 3;
	public static long minPerInterval = 5;
	public static boolean isActive = true;
	
	public static void main(String[] args){
		totalTenantNumber = 1000;
		TYPE = 1;
		onlyMysql = true;
		testTime = 900000;
		intervalTime = 300000;
		int waitTime = 30000;
		init();
		
		for(int i = 0; i < tenantNumber; i++){
			tenants[i].start();
		}
		try {
			Thread.sleep(waitTime);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		
		System.out.println("******************hybrid test start******************");
		Main.setConcurrency(0.1);
		Main.isActive = true; //****************start test***************************//
		for(int i=0; i<intervalNumber; i++){
			Main.setQT();
			//send data to PerformanceController: new QT
			try {
//				Thread.sleep(intervalTime);
				for(int j = 0; j < minPerInterval; j++){
					Thread.sleep(60000);
					//check throughput, send data to PerformanceController: lateTenant, lateQuery
				}
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		Main.isActive = false;//******************end test**************************//
		System.out.println("******************hybrid test complete******************");
		System.exit(1);
		
	}
	
	public static void init(){
		intervalNumber = testTime / intervalTime;
		minPerInterval = intervalTime / 60000;
		tenantNumber = (int) (totalTenantNumber*PercentTenantSplits[TYPE]);
		if(TYPE != 0){
			IDStart = (int) (totalTenantNumber*PercentTenantSplitsSum[TYPE-1]);
		}else{
			IDStart = 0;
		}
		Main.QTMax = QTMatrix[TYPE];
		tenants = new Tenant[tenantNumber];
		usingVoltdb = new boolean[tenantNumber];
		partiallyUsingVoltdb = new boolean[tenantNumber];
		throughputPerTenant = new int[tenantNumber];
		for(int i = 0; i < tenantNumber; i++){
			tenants[i] = new Tenant(i+IDStart, DSMatrix[i], QTMatrix[i], WHMatrix[i], Main.dbURL, Main.dbUsername, Main.dbPassword, Main.voltdbServer);
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
					int tt = ran.nextInt(2);
					if(tt == 1)	Main.throughputPerTenant[i] = 20;
					else Main.throughputPerTenant[i] = 40;
					break;
				case 100:
					int ttt = ran.nextInt(3);
					if(ttt == 0)	Main.throughputPerTenant[i] = 20;
					else if(ttt == 1) Main.throughputPerTenant[i] = 40;
					else Main.throughputPerTenant[i] = 60;
					break;
				}
			}
		}
	}
	
}
