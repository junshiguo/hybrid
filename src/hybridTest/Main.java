package hybridTest;

import hybridConfig.HConfig;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.net.Socket;
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
	public static String voltdbServer = "10.20.2.211";

	public static boolean[] usingVoltdb; //set by setDBState
	public static boolean[] partiallyUsingVoltdb; //set by setDBState
	public static int[] throughputPerTenant; //planned throughput, set by setQT()
	public static double concurrency = 0.1; //set by setConcurrency
	
	public static int port = 8899;
	public static Socket socket;
	public static BufferedReader reader;
	public static Writer writer;
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
	
	public static void main(String[] args){
		totalTenantNumber = 1000;
		TYPE = 1;
		onlyMysql = true;
		testTime = 900000;
		intervalTime = 300000;
		init();
		
		for(int i = 0; i < tenantNumber; i++){
			tenants[i].start();
		}
//		try {
//			Thread.sleep(waitTime);
//		} catch (InterruptedException e) {
//			e.printStackTrace();
//		}
		try {
			socket = new Socket("10.171.5.28", port);
			reader = new BufferedReader(new InputStreamReader(socket.getInputStream(),"UTF-8"));
			writer = new OutputStreamWriter(socket.getOutputStream(),"UTF-8");
			writer.write(Main.TYPE+"&0&in position\n");
			writer.flush();
			reader.readLine(); // hybrid controller send sth to start test
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		
		System.out.println("******************hybrid test start******************");
		Main.setConcurrency(0.1);
		Main.isActive = true; 
		for(int i=0; i<intervalNumber; i++){
			try {
				Main.setQT();
				//send data to controller: new qt &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
				String str = "";
				for(int qt: Main.throughputPerTenant){
					str = str + qt + " ";
				}
				writer.write(Main.TYPE+"&1&"+str+"\n");
				writer.flush();
				for(int j = 0; j < minPerInterval; j++){
					for(int k = 0; k < 20; k++){ //send requests every 3 seconds, 20 times per minute
						for(int id = 0; id < tenantNumber; id++){
							tenants[id].doSQLNow++;
						}
						Thread.sleep(3000);
					}
					//check throughput, send data to PerformanceController: lateTenant, lateQuery&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
					
				}
			} catch (InterruptedException | IOException e) {
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
		tenants = new Tenant[tenantNumber];
		usingVoltdb = new boolean[tenantNumber];
		partiallyUsingVoltdb = new boolean[tenantNumber];
		throughputPerTenant = new int[tenantNumber];
		for(int i = 0; i < tenantNumber; i++){
			tenants[i] = new Tenant(i+IDStart, HConfig.DSMatrix[i], HConfig.QTMatrix[i], HConfig.WHMatrix[i], Main.dbURL, Main.dbUsername, Main.dbPassword, Main.voltdbServer);
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
	
}
