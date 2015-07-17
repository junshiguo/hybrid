package mysqlTest;

import hybridConfig.HConfig;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.SQLException;

//import newhybrid.HQueryResult;


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
	
	public static int ReturnData = 100; // default return 100% data. 
	public static boolean OnlySelect = false;
	public static boolean DSTest = false;
	public static int Longer = 1;
	public static long READ = 0;
	public static long WRITE = 0;
	public static int tenantPerThread1 = 500/Longer;
	public static int tenantPerThread2 = 500/(100-Longer); 

	public static void main(String[] args){
		HConfig.init(3000);
		String server = "10.20.2.28";
		String dbname = "tpcc3000";
		totalTenant = 50;
		numberOfThread = 50;
		timeInterval = 1*60*1000; //1 min
		intervalNumber = 5;
		double base = 0.2;
		double step = 0.0 ;
		boolean copyTable = false;
//		MCopyData.CopyTables(numberOfThread);
		//*******************init para from args*****************//
		if(args.length > 0){
			server = args[0].trim();
		}
		if(args.length > 1){
			dbname = args[1].trim();
		}
		if(args.length > 2){
			intervalNumber = Integer.parseInt(args[2]);
		}
		if(args.length > 3){
			base = Double.parseDouble(args[3]);
		}
		if(args.length > 4){
//			step = Double.parseDouble(args[4]);
			ReturnData = Integer.parseInt(args[4]);
			if(ReturnData != 25 && ReturnData != 50 && ReturnData != 75 && ReturnData != 100){
				ReturnData = 100;
				System.out.println("Wrong value for return data percentage. Using default 100% instead...");
			}
			OnlySelect = true;
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
			if(DSTest == true){
				fstream = new FileWriter("DSTest"+numberOfThread+"."+Longer+"txt", true);
			}else if(OnlySelect == false){
				fstream = new FileWriter("test"+numberOfThread+".txt", true);
			}else{
				fstream = new FileWriter("STest"+numberOfThread+"."+ReturnData+".txt", true);
			}
			out = new BufferedWriter(fstream);
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		System.out.println("***************mysql test start now***************");
		for(int i=0; i<intervalNumber; i++){
			try {
				for(int j=0; j<numberOfThread; j++){
					Tenant.tenants[j].setSequence(i*step+base);
					Tenant.tenants[j].queryNumber(-1);
				}
//				tmpList = new ArrayList<Long>();
				queryThisInterval = 0;
				retryThisInterval = 0;
				currentInterval = i;
				READ = 0;
				WRITE = 0;
				Thread.sleep(timeInterval);
				long throughput = queryThisInterval * 60000/ timeInterval;
				long tp2 = 0;
				for(int j = 0; j < numberOfThread; j++){
					tp2 += Tenant.tenants[j].queryNumber(0);
				}
				tp2 = tp2 * 60000/ timeInterval;
				out.write(""+(i)+" "+tp2+" "+retryThisInterval*60000/timeInterval+" "+(i*step+base));
				out.newLine();out.flush();
				System.out.println("Interval "+(i+1)+" finished! Throughput: "+throughput+". Write percent: "+(i*step+base)+".  (Total: "+intervalNumber+" intervals...)");
				System.out.println("Interval "+(i+1)+" finished! Throughput: "+tp2);
				System.out.println("READ: "+READ+". WRITE: "+WRITE+". write percent: "+(WRITE*1.0/(READ+WRITE)));
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
		try {
			Thread.sleep(3000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		System.exit(0);
	}
	
	public static void initDBPara(String url, String user, String pwd){
		dbURL = url;
		dbUserName = user;
		dbPassword = pwd;
	}

}
