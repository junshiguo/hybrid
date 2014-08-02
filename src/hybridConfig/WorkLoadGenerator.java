package hybridConfig;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;

import utility.Support;

public class WorkLoadGenerator {
	public static double activeRatio = 0.2;
	public static double exchangeRatio = 0.2;
	public static int totalTenant = 1000;
	public static int timePerInterval = 5; //min
	public static int totalInterval = 6; // 1 h
	public static int HRan = 180;
	public static int MRan = 60;
	public static int LRan = 10;
	public static int dRan = 5;
	public static boolean[][] activePattern = new boolean[totalTenant][totalInterval];
	public static boolean[] isBursty = new boolean[totalInterval];
	
	public static int activeNumber;
	public static int[] activeTenant;
	public static int[] inactiveTenant;
	
	public static void main(String[] args) throws IOException{
		totalTenant = 1000;
		activeNumber = (int) (activeRatio * totalTenant);
		activeTenant = new int[activeNumber];
		inactiveTenant = new int[totalTenant - activeNumber];
		HConfig.init(totalTenant);
		generateLoad();
	}
	
	public static void generateLoad() throws IOException{
		setBursty();
		setActivePattern();
		FileWriter fstream = null;
		fstream = new FileWriter("load.txt", false);
		BufferedWriter out = new BufferedWriter(fstream);
		
		for(int intervalId = 0; intervalId < totalInterval; intervalId++){
			int[] load = new int[totalTenant];
			Random ran = new Random(System.nanoTime());
			for (int tenantId = 0; tenantId < totalTenant; tenantId++) {
				int QT = HConfig.getQT(tenantId, false);
				boolean isActive = activePattern[tenantId][intervalId];
				if (isActive) {
					load[tenantId] = ran.nextInt(QT) + 1;
					if(isBursty[intervalId]){
						load[tenantId] = (load[tenantId]+HRan > QT)?QT:(load[tenantId]+HRan);
					}else{
						boolean tmp = ran.nextBoolean();
						if (tmp) {
							load[tenantId] = (load[tenantId] + MRan > QT) ? QT: load[tenantId] + MRan;
						} else {
							load[tenantId] = (load[tenantId] + LRan > QT) ? QT: load[tenantId] + LRan;
						}
					}
				} else {
					load[tenantId] = 0;
				}
			}
//			for (int time = 0; time < timePerInterval; time++) {
//				out.write("" + (intervalId * timePerInterval + time));
//				int dd = ran.nextInt() % dRan;
			out.write(""+(intervalId*timePerInterval));
				int dd = 0;
				int totalLoad = 0;
				for (int tenantId = 0; tenantId < totalTenant; tenantId++) {
					int QT = HConfig.getQT(tenantId, false);
					boolean isActive = activePattern[tenantId][intervalId];
					int tmpload = 0;
					if (isActive) {
						tmpload = load[tenantId] + dd;
						tmpload = (tmpload > QT) ? QT : tmpload;
					}
					out.write(" " + tmpload);
					totalLoad += tmpload;
				}
				out.write(" " + totalLoad);
				out.newLine();out.flush();
			}
//		}
		out.close();
	}
	
	public static void setBursty(){
		isBursty = new boolean[totalInterval];
		for(int i = 0; i < totalInterval; i++){
			isBursty[i] = false;
		}
		isBursty[2] = isBursty[3] = true;
	}
	
	public static void setActivePattern(){
		activePattern = new boolean[totalTenant][totalInterval];
		for(int i=0;i<totalTenant;i++){
			for(int j=0;j<totalInterval;j++){
				activePattern[i][j] = false;
			}
		}
		
		activeTenant = Support.Rands(0, totalTenant, activeNumber);
		boolean[] state = new boolean[totalTenant];
		for(int i = 0; i < totalTenant; i++){
			state[i] = false;
		}
		for(int i = 0; i < activeNumber; i++){
			state[activeTenant[i]] = true;
		}
		int index = 0;
		for(int i = 0; i < totalTenant; i++){
			if(state[i] == false){
				inactiveTenant[index] = i;
				index++;
			}
		}
		for(int intervalId = 0; intervalId < totalInterval; intervalId++){
			exchange();
			for(int i = 0; i < activeNumber; i++){
				activePattern[activeTenant[i]][intervalId] = true;
			}
		}
	}
	
	public static void exchange(){
		int exchangeNumber = (int) (exchangeRatio*activeNumber);
		int[] exa = Support.Rands(0, activeNumber, exchangeNumber);
		int[] exin = Support.Rands(0, inactiveTenant.length, exchangeNumber);
		int tmp;
		for(int i = 0; i < exchangeNumber; i++){
			tmp = activeTenant[exa[i]];
			activeTenant[exa[i]] = inactiveTenant[exin[i]];
			inactiveTenant[exin[i]] = tmp;
		}
	}

}
