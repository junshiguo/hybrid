package hybridConfig;

public class BurstDetector {
	public static String fileName = "load.txt";
	public static int maxOffloadingTenant = 200;
	public static double maxOffloadingData = 1024; //MB
	public static long mysqlThroughput = 10000;
	public static long voltdbThroughtput = 400000;
	
	public static int[] findBurst(int tenantNumber, int totalTimeInterval){
		return null;
	}
	
	public static int getMTP(double writePercent){
		return 0;
	}
	
	public static int getVTP(double writePercent){
		return 0;
	}

}
