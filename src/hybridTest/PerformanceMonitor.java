package hybridTest;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;

import utility.Support;

public class PerformanceMonitor extends Thread {
	public static ArrayList<Long> timePerQuery;
	public static int[] actualThroughputPerTenant; // updated in Main per minute
	public static long writeQuery = 0;
	public static long readQuery = 0;
	public static double writePercent; 
	public static long checkInterval = 5000; // 5s
	public static long time = 0;
	
	public PerformanceMonitor(int tenantNumber){
		writeQuery = 0;
		readQuery = 0;
		checkInterval = 5000;
		time = 0;
		actualThroughputPerTenant = new int[tenantNumber];
		for(int i=0; i<tenantNumber; i++){
			actualThroughputPerTenant[i] = 0;
		}
	}
	
	public void run() {
		FileWriter fstream = null;
		try {
			fstream = new FileWriter("HTest"+Main.TYPE+".txt", true);
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		BufferedWriter out = new BufferedWriter(fstream);
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
				timePerQuery = new ArrayList<Long>();
				writeQuery = 0;
				readQuery = 0;
				Thread.sleep(checkInterval);
				long readNumber = readQuery;
				long writeNumber = writeQuery;
				writePercent = (writeNumber*1.0)/(readNumber+writeNumber);
				//send data to PerformanceController using socket: throughput, writePercent&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
				Main.socketSender.sendInfo(time/1000, (readNumber+writeNumber), writePercent);
				ArrayList<Long> tmpList = new ArrayList<Long>(timePerQuery);
				Iterator<Long> iter = tmpList.iterator();
				ArrayList<Long> toRemove = new ArrayList<Long>();
				while(iter.hasNext()){
					Long nn = iter.next();
					if(nn == null){
						toRemove.add(nn);
					}
				}
				tmpList.removeAll(toRemove);
				Collections.sort(tmpList, new Comparator<Long>(){
					@Override
					public int compare(Long e0, Long e1) {
						return e0.compareTo(e1);
					}
				});
				int totalQuery = tmpList.size();
				out.write(""+time/1000+" "+Support.getAverage(tmpList)/1000000.0); //col 1,2
				out.write(" "+totalQuery);  //3
				for(int ii = 0; ii < totalQuery; ii++){
					out.write(" "+tmpList.get(ii)/1000000.0);
				}
//				out.write(""+time/1000+" "+Support.getAverage(tmpList)/1000000.0); //col 1,2
//				out.write(" "+tmpList.get((int)(totalQuery*0.8))/1000000.0); //col 3
//				out.write(" "+tmpList.get((int)(totalQuery*0.85))/1000000.0); //col 4
//				out.write(" "+tmpList.get((int)(totalQuery*0.9))/1000000.0);  //col 5
//				out.write(" "+tmpList.get((int)(totalQuery*0.95))/1000000.0);  //col 6
//				out.write(" "+tmpList.get((int)(totalQuery*0.99))/1000000.0);  //col 7
//				out.write(" "+totalQuery);  //col 8
				out.newLine();
				out.flush();		
				time += checkInterval;
			} catch (InterruptedException | IOException e) {
				e.printStackTrace();
			}
		}
	}
	
}
