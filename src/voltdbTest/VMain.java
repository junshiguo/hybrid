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
		serverlist = "127.0.0.1";
		numberOfThread = 100;
		timeInterval = 60000; 
		intervalNumber = 5;
		double base = 0.90;
		double step = 0.02;
		boolean copyTable = false;
//		test.CopyTables(numberOfThread);
		
		//*******************init para from args*****************//
		if(args.length > 0){
			serverlist = args[0].trim();
		}
		if(args.length > 1){
			numberOfThread = Integer.parseInt(args[1]);
		}
		if(args.length > 2){
			timeInterval = Long.parseLong(args[2])*1000;
		}
		if(args.length > 3){
			intervalNumber = Integer.parseInt(args[3]);
		}
		if(args.length > 4){
			base = Double.parseDouble(args[4]);
		}
		if(args.length > 5){
			step = Double.parseDouble(args[5]);
		}
		if(args.length > 6 && args[6] != null){
			int b = Integer.parseInt(args[6]);
			if(b == 0) copyTable = false;
			else copyTable = true;
		}			
		
		initDBPara(serverlist);
		VTenant.init(numberOfThread, VMain.serverlist, copyTable);
		
		VDriver.IsActive = true;
		for(int i=0; i<numberOfThread; i++){
			VTenant.tenants[i].start();
		}
		System.out.println("***************warm up***************");
		try { //wait all thread connect to mysql and prepare statements and warm up
			Thread.sleep(10000);
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
		double wptmp = base;
		for(int i=0; i<intervalNumber; i++){
			try {
				for(int j=0; j<numberOfThread; j++){
//					VTenant.tenants[j].setSequence(i*step+base);
					VTenant.tenants[j].setSequence(wptmp);
				}
				queryThisInterval = 0;
				currentInterval = i;
				Thread.sleep(timeInterval);
				System.out.println("Interval "+i+" finished! (Total: "+intervalNumber+" intervals...)");
				long throughput = queryThisInterval * 60000/ timeInterval;
				out.write(""+(wptmp)+" "+throughput);
				out.newLine();out.flush();
				wptmp += step;
			} catch (InterruptedException | IOException e) {
				e.printStackTrace();
			}
		}
		startCount = false;
		VDriver.IsActive = false;
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
		System.exit(0);
	}
	
	public static void initDBPara(String sl){
		serverlist = sl;;
	}

}
