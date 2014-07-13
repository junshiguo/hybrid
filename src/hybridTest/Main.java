package hybridTest;

public class Main {
	public static int tenantNumber;
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
	
	public static boolean startCount = false;
	public static boolean isVoltdbUsed = false;
	public static double writePercent = 0.0; //set in performanceMonitor
	public static boolean dataRetriving = false;
	public static boolean retriverWorking = false;
	public static boolean onlyMysql = true;
	public static long testTime = 600000; //10 mins
	public static boolean isActive = true;
	
	public static void main(String[] args){
		tenantNumber = 50;
		onlyMysql = true;
		testTime = 600000;
		isVoltdbUsed = false;
		int waitTime = 30000;
	}

}
