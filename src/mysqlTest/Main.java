package mysqlTest;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;


/**
 * Main
 */
public class Main {
	public static double writePercent = 0.1;
	public static int numberOfThread;
	public static int[] success;
	public static int[] failure;
	public static int[] retry;
	public static int[] exception;
	public static int[] nullExecute;
//	public static long[][] time;  //[numberOfThread][(int) Math.ceil(activeTime/timeInterval)]
	public static int[][] success2;  //[numberOfThread][(int) Math.ceil(activeTime/timeInterval)]
//	public static long[][] averageTime; //time/success2
	public static long timeInterval = 10000; //10s
	public static long activeTime = 300000; //5min
	public static long intervalNumber;
	public static long currentInterval = 0;
	public static int MaxTry = 1;
	
	public static String dbURL;
	public static String dbUserName;
	public static String dbPassword;

	public static void main(String[] args){
		numberOfThread = 50;
		writePercent = 0.8;
		timeInterval = 60000; //
		activeTime = 60000; //
		//*******************init para from args*****************//
//		if(args.length < 4){
//			System.out.println("args: writePercent, numberOfThread, timeInterval, activeTime. Please try again...");
//			System.exit(0);
//		}
		if(args.length > 0){
			writePercent = Double.parseDouble(args[0]);
		}
		if(args.length > 1){
			numberOfThread = Integer.parseInt(args[1]);
		}
		if(args.length > 2){
			timeInterval = Long.parseLong(args[2])*1000;
		}
		if(args.length > 3){
			activeTime = Long.parseLong(args[3])*1000;
		}		
		boolean copyTable = false;
		if(args.length > 4 && args[4] != null){
			int b = Integer.parseInt(args[4]);
			if(b == 0) copyTable = false;
			else copyTable = true;
		}			
		
		initDBPara("jdbc:mysql://10.20.2.44:3306/tpcc10", "remote", "remote");
		Tenant.init(numberOfThread,writePercent, Main.dbURL, Main.dbUserName, Main.dbPassword, copyTable);
		initResult();
		
		Driver.IsActive = true;
		for(int i=0; i<numberOfThread; i++){
			Tenant.tenants[i].start();
		}
		try {
			Thread.sleep(10000);
		} catch (InterruptedException e1) {
			e1.printStackTrace();
		}
		
		intervalNumber = activeTime/timeInterval;
		for(int i=0; i<intervalNumber; i++){
			try {
				currentInterval = i;
				Thread.sleep(timeInterval);
				System.out.println("Interval "+i+" finished! (Total: "+intervalNumber+" intervals...)");
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		
		Driver.IsActive = false;
		
//		long average = 0, maxAverage=0, minAverage=0;
//		long[] averages = new long[numberOfThread];
//		for(int i=0; i< numberOfThread; i++){
//			averages[i] = 0;
//			for(int j=0; j<(activeTime/timeInterval); j++){
//				averageTime[i][j] =( (success2[i][j]!=0)?time[i][j] / success2[i][j]:0 );
//				average += averageTime[i][j];
//				averages[i] += averageTime[i][j];
//				System.out.println("thread "+i+" interval "+j+", average time: "+averageTime[i][j]/1000000.0);
//			}
//			averages[i] /= (intervalNumber);
//			if(i == 0){
//				maxAverage = minAverage = averages[i];
//			}else{
//				if(averages[i] > maxAverage)	maxAverage = averages[i];
//				if(averages[i] < minAverage)	minAverage = averages[i];
//			}
//		}
//		average = average/(numberOfThread*activeTime/timeInterval);
//		System.out.println();
//		
//		for(int i=0; i<numberOfThread; i++){
//			System.out.println("Thread "+i+" average time: "+averages[i]/1000000.0);
//		}
//		System.out.println(average/1000000.0);
	//		int[] throughput = new int[(int) intervalNumber];
	//		for(int i=0; i<intervalNumber; i++){
	//			throughput[i] = 0;
	//			for(int j=0; j<numberOfThread; j++){
	//				throughput[i] += success2[j][i];
	//			}
	//			throughput[i] = throughput[i]*60000/(int)timeInterval;
	//		}
		int throughput = 0;
		for(int i=0; i<numberOfThread; i++){
			for(int j=0; j<intervalNumber; j++){
				throughput += success2[i][j];
			}
		}
		throughput /= intervalNumber;
		throughput *= (6000/(int)timeInterval);
		
		try {
			FileWriter fstream = new FileWriter("test"+numberOfThread+".txt", true);
			BufferedWriter out = new BufferedWriter(fstream);
//			out.write(numberOfThread+" "+writePercent+" "+average/1000000.0+" "+maxAverage/1000000.0+" "+minAverage/1000000.0);
			out.write(numberOfThread+" "+writePercent+" "+ throughput);
			out.newLine();
			out.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		for(int i=0; i<numberOfThread; i++){
			try {
				Tenant.tenants[i].join();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}
	
	public static void initDBPara(String url, String user, String pwd){
		dbURL = url;
		dbUserName = user;
		dbPassword = pwd;
	}
	
	public static void initResult(){
		success = new int[numberOfThread];
		failure = new int[numberOfThread];
		retry = new int[numberOfThread];
		exception = new int[numberOfThread];
		nullExecute = new int[numberOfThread];
//		time = new long[numberOfThread][(int) Math.ceil(activeTime/timeInterval)];
		success2 = new int[numberOfThread][(int) Math.ceil(activeTime/timeInterval)];
//		averageTime = new long[numberOfThread][(int) Math.ceil(activeTime/timeInterval)];
		for(int i=0;i<numberOfThread;i++){
			success[i] = 0;
			failure[i] = 0;
			retry[i] = 0;
			exception[i] = 0;
			nullExecute[i] = 0;
			for(int j=0; j<(int) Math.ceil(activeTime/timeInterval); j++){
//				time[i][j] = 0;
				success2[i][j] = 0;
			}
		}
	}
	
	public static double getWritePercent() {
		return writePercent;
	}

	public static void setWritePercent(double writePercent) {
		Main.writePercent = writePercent;
	}

}
