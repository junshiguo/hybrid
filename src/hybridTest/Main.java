package hybridTest;

public class Main {
	public static int totalTenantNumber;
	public static int tenantNumber;
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
	public static double[] PercentQT = { 0.5, 0.3, 0.2 };
	public static double[] PercentDS = { 0.3, 0.5, 0.2 };
	public static double[] PercentWH = { 0.4, 0.6 };  // 0.4 for write heavy and 0.6 for read heavy
	public static Tenant[] tenants;
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
	public static boolean[] usingVoltdb;
	public static int[] throughputPerTenant; //planned throughput
	public static int[] latePerTenant; //late requests
	public static double concurrency = 0.1;
	
//	public static boolean sendRequest = false;
	public static boolean startCount = false;
	public static boolean isVoltdbUsed = false;
	public static double writePercent = 0.0; //set in performanceMonitor
	public static boolean dataRetriving = false;
	public static boolean retriverWorking = false;
	public static boolean onlyMysql = true;
	public static long testTime = 600000; //10 mins
	public static boolean isActive = true;
	
	public static void main(String[] args){
		totalTenantNumber = 1000;
		TYPE = 1;
		onlyMysql = true;
		testTime = 600000;
		isVoltdbUsed = false;
		int waitTime = 30000;
		init();
		
		
	}
	
	public static void init(){
		tenantNumber = (int) (totalTenantNumber*PercentTenantSplits[TYPE]);
		if(TYPE != 0){
			IDStart = (int) (totalTenantNumber*PercentTenantSplitsSum[TYPE-1]);
		}else{
			IDStart = 0;
		}
		usingVoltdb = new boolean[tenantNumber];
		throughputPerTenant = new int[tenantNumber];
		latePerTenant = new int[tenantNumber];
		for(int i = 0; i < tenantNumber; i++){
			usingVoltdb[i] = false;
			throughputPerTenant[i] = 0;
			latePerTenant[i] = 0;
		}
		switch(TYPE){
		case 0: case 1: case 2: case 3: case 4: case 5:
			QTMax = 20; 
			break;
		case 6: case 7: case 8: case 9: case 10: case 11:
			QTMax = 60;
			break;
		case 12: case 13: case 14: case 15: case 16: case 17:
			QTMax = 100;
			break;
		}
	}

	public static void setConcurrency(double cc){
		concurrency = cc;
	}
	
	public static void setQT(){
		
	}
	
}
