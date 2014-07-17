package hybridTest;

public class PerformanceMonitor extends Thread {
	public static int[] actualThroughputPerTenant;
	public static int[] latePerTenant; //late requests
	public static int totalLateQuery = 0;
	public static int totalLateTenant = 0;
	public static long writeQuery = 0;
	public static long readQuery = 0;
	public static double writePercent; 
	public static long checkInterval = 5000; // 5s
	public static long time = 0;
	
	public PerformanceMonitor(int tenantNumber){
		totalLateQuery = 0;
		totalLateTenant = 0;
		writeQuery = 0;
		readQuery = 0;
		checkInterval = 5000;
		time = 0;
		actualThroughputPerTenant = new int[tenantNumber];
		latePerTenant = new int[tenantNumber];
		for(int i=0; i<tenantNumber; i++){
			actualThroughputPerTenant[i] = 0;
			latePerTenant[i] = 0;
		}
	}
	
	public void run() {
		time = 0;
		while(Main.isActive == false){
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		while(Main.isActive){
			try {
				writeQuery = 0;
				readQuery = 0;
				Thread.sleep(checkInterval);
				long readNumber = readQuery;
				long writeNumber = writeQuery;
				writePercent = (writeNumber*1.0)/(readNumber+writeNumber);
				//send data to PerformanceController using socket: throughput, writePercent
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}
	
}
