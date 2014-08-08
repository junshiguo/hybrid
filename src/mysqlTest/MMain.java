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
		String dbname = "tpccM1500";
		totalTenant = 3000;
		numberOfThread = 100;
		timeInterval = 60000; //1 min
		intervalNumber = 2;
		double base = 0.34;
		double step = 0.02 ;
		boolean copyTable = false;
//		MCopyData.CopyTables(numberOfThread);
		//*******************init para from args*****************//
		if(args.length > 0){
			server = args[0].trim();
		}
		if(args.length > 1){
			dbname = args[1].trim();
			if(dbname.equals("tpccM1500"))	totalTenant = 1500;
			else if(dbname.equals("tpccM3000")) totalTenant = 3000;
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
		tenantPerThread = totalTenant / numberOfThread;
		initDBPara("jdbc:mysql://"+server+"/"+dbname, "remote", "remote");
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
				long throughput = queryThisInterval * 60000/ timeInterval;
//				throughput /= intervalNumber;
				out.write(""+(i*step+base)+" "+throughput+" "+retryThisInterval*60000/timeInterval);
				out.newLine();out.flush();
				System.out.println("Interval "+i+" finished! Throughput: "+throughput+". Write percent: "+(i*step+base)+".  (Total: "+intervalNumber+" intervals...)");
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
