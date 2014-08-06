package mysqlTest;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.SQLException;


/**
 * Main
 */
public class MMain {
	public static int numberOfThread;
	public static int totalTenant = 3000;
	public static long timeInterval = 60000; //60s
	public static long currentInterval = 0;
	public static int MaxTry = 3;
	public static boolean startCount = false;
	
	public static String dbURL;
	public static String dbUserName;
	public static String dbPassword;
	public static int intervalNumber = 101;
	public static long queryThisInterval = 0;
	public static long retryThisInterval = 0;
	public static int tenantPerThread = 30;

	public static void main(String[] args){
		String server = "10.20.2.211";
		totalTenant = 2000;
		numberOfThread = 100;
		timeInterval = 60000; //1 min
		intervalNumber = 2;
		double base = 0.34;
		double step = 0.02 ;
		boolean copyTable = false;
//		MCopyData.CopyTables(numberOfThread);
		//*******************init para from args*****************//
		if(args.length > 0){
			totalTenant = Integer.parseInt(args[0]);
		}
		if(args.length > 1){
			numberOfThread = Integer.parseInt(args[1]);
		}
		if(args.length > 2){
			intervalNumber = Integer.parseInt(args[2]);
		}
		if(args.length > 3){
			base = Double.parseDouble(args[3]);
		}
		if(args.length > 4){
			step = Double.parseDouble(args[4]);
		}
		if(args.length > 5 && args[5] != null){
			int b = Integer.parseInt(args[5]);
			if(b == 0) copyTable = false;
			else copyTable = true;
		}			
		tenantPerThread = totalTenant / numberOfThread;
		initDBPara("jdbc:mysql://"+server+"/tpcc3000", "remote", "remote");
		Tenant.init(numberOfThread, MMain.dbURL, MMain.dbUserName, MMain.dbPassword, copyTable);
		
		Driver.IsActive = true;
		for(int i=0; i<numberOfThread; i++){
			Tenant.tenants[i].start();
		}
		System.out.println("************init finished*****************");
		try { //wait all thread connect to mysql and prepare statements and warm up
			Thread.sleep(15000);
		} catch (InterruptedException e1) {
			e1.printStackTrace();
		}
		//*********************start MCopyData***********************//
		startCount = true;
		FileWriter fstream = null;
		BufferedWriter out = null;
		try {
			fstream = new FileWriter("test"+numberOfThread+".txt", true);
			out = new BufferedWriter(fstream);
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		System.out.println("***************mysql test start now***************");
		for(int i=0; i<intervalNumber; i++){
			try {
				for(int j=0; j<numberOfThread; j++){
					Tenant.tenants[j].setSequence(i*step+base);
				}
//				tmpList = new ArrayList<Long>();
				queryThisInterval = 0;
				retryThisInterval = 0;
				currentInterval = i;
				Thread.sleep(timeInterval);
				System.out.println("Interval "+i+" finished! (Total: "+intervalNumber+" intervals...)");
				long throughput = queryThisInterval * 60000/ timeInterval;
//				throughput /= intervalNumber;
				out.write(""+(i*step+base)+" "+throughput+" "+retryThisInterval*60000/timeInterval);
				out.newLine();out.flush();
			} catch (InterruptedException | IOException e) {
				e.printStackTrace();
			}
		}
		startCount = false;
		Driver.IsActive = false;
		try {
			out.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		try {
			Thread.sleep(3000);
			for(int i = 0; i < numberOfThread; i++){
				Tenant.tenants[i].conn.close();
			}
		} catch (InterruptedException | SQLException e1) {
			e1.printStackTrace();
		}
//		System.exit(0);
	}
	
	public static void initDBPara(String url, String user, String pwd){
		dbURL = url;
		dbUserName = user;
		dbPassword = pwd;
	}

}
