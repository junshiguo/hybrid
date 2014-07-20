package voltdbTest;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

/**
 * Main
 */
public class VMain {
	public static int numberOfThread;
	public static long timeInterval = 60000; //60s
	public static long currentInterval = 0;
	public static int MaxTry = 1;
	public static boolean startCount = false;
	
	public static String serverlist = "127.0.0.1";
	public static int intervalNumber = 101;
	public static long queryThisInterval = 0;

	public static void main(String[] args){
		serverlist = "10.20.2.211";
		numberOfThread = 500;
		timeInterval = 60000; 
		intervalNumber = 2;
		double base = 0.34;
		double step = 0.02;
		boolean copyTable = false;
//		test.CopyTables(numberOfThread);
		
		//*******************init para from args*****************//
		if(args.length > 0){
			numberOfThread = Integer.parseInt(args[0]);
		}
		if(args.length > 1){
			timeInterval = Long.parseLong(args[1])*1000;
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
		
		initDBPara(serverlist);
		Tenant.init(numberOfThread, VMain.serverlist, copyTable);
		
		Driver.IsActive = true;
		for(int i=0; i<numberOfThread; i++){
			Tenant.tenants[i].start();
		}
		System.out.println("***************warm up***************");
		try { //wait all thread connect to mysql and prepare statements and warm up
			Thread.sleep(15000);
		} catch (InterruptedException e1) {
			e1.printStackTrace();
		}
		System.out.println("***************voltdb test start now***************");
		startCount = true;
		FileWriter fstream = null;
		BufferedWriter out = null;
		try {
			fstream = new FileWriter("VDTest"+numberOfThread+".txt", true);
			out = new BufferedWriter(fstream);
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		for(int i=0; i<intervalNumber; i++){
			try {
				for(int j=0; j<numberOfThread; j++){
					Tenant.tenants[j].setSequence(i*step+base);
				}
				queryThisInterval = 0;
				currentInterval = i;
				Thread.sleep(timeInterval);
				System.out.println("Interval "+i+" finished! (Total: "+intervalNumber+" intervals...)");
				long throughput = queryThisInterval * 60000/ timeInterval;
				out.write(""+(i*step+base)+" "+throughput);
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
//		for(int i=0; i<numberOfThread; i++){
//			try {
//				Tenant.tenants[i].join(30000);
//			} catch (InterruptedException e) {
//				e.printStackTrace();
//			}
//		}
//		System.exit(0);
	}
	
	public static void initDBPara(String sl){
		serverlist = sl;;
	}

}
