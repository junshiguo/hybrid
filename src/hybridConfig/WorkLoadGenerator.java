package hybridConfig;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;

import utility.Support;

public class WorkLoadGenerator {
	public static double activeRatio = 0.2;
	public static double exchangeRatio = 0.1;
	public static double[] activePercentPerQT = {0.3, 0.3, 0.4};
	public static int[] tenantsPerQT;
	public static int totalTenant = 2000;
	public static int timePerInterval = 10; //min
	public static int totalInterval = 48; // 8 h
	public static int HRan = 80;
	public static int MRan = 40;
	public static int LRan = 10;
	public static boolean[][] activePattern = new boolean[totalTenant][totalInterval];
	public static boolean[] isBursty = new boolean[totalInterval];
	
	public static int activeNumber = (int) (activeRatio * totalTenant);
//	public static int[][] load = new int[activeNumber][timePerInterval];
	public static int[] activeTenant = new int[activeNumber];
	public static int[] inactiveTenant = new int[totalTenant-activeNumber];
	
	public static void main(String[] args) throws IOException{
		totalTenant = 1000;
		HConfig.init(totalTenant);
		tenantsPerQT = new int[3];
		for(int i = 0; i < 3;i ++){
			tenantsPerQT[i] = (int) (totalTenant*HConfig.PercentQT[i]);
		}
//		generateLoad();
		generateLoad2();
	}
	
	public static void generateLoad() throws IOException{
		setBursty();
		setActivePattern();
		FileWriter fstream = null;
		fstream = new FileWriter("load.txt", false);
		BufferedWriter out = new BufferedWriter(fstream);
		
		Random ran = new Random();
		for(int tenantId = 0; tenantId < totalTenant; tenantId++){
			int QT = HConfig.getQT(tenantId, false);
			out.write(tenantId+" "+QT);
			for(int intervalId = 0; intervalId < totalInterval; intervalId++){
				boolean isActive = activePattern[tenantId][intervalId];
				for(int time = 0; time < timePerInterval; time++){
					if(isActive){
						int load = ran.nextInt(QT)+1;
						if(isBursty[intervalId]){
							int tmp = load+HRan;
							load = (tmp > QT)?QT:tmp;
						}else{
							boolean tmp = ran.nextBoolean();
							if(tmp){
								load = (load+MRan>QT)?QT:load+MRan;
							}else{
								load = (load+LRan>QT)?QT:load+LRan;
							}
						}
						out.write(" "+load);
					}else{
						out.write(" 0");
					}
				}
			}
			out.newLine();out.flush();
		}
		out.close();
	}
	
	public static void generateLoad2() throws IOException{
		setBursty();
		setActivePattern();
		FileWriter fstream = null;
		fstream = new FileWriter("load2.txt", false);
		BufferedWriter out = new BufferedWriter(fstream);
		
		Random ran = new Random();
		int load = 0;
		for(int intervalId = 0; intervalId < totalInterval; intervalId++){
			boolean flag = true;
			for(int time = 0; time < timePerInterval; time++){
				int totalLoad = 0;
				int tmptime = intervalId*timePerInterval+time;
				out.write(tmptime+"");
				for(int tenantId = 0; tenantId < totalTenant; tenantId++){
					int QT = HConfig.getQT(tenantId, false);
					boolean isActive = activePattern[tenantId][intervalId];
					if(isActive){
						if(flag){
							load = ran.nextInt(QT)+1;
							if(isBursty[intervalId]){
								int tmp = load+HRan;
								load = (tmp > QT)?QT:tmp;
							}else{
								boolean tmp = ran.nextBoolean();
								if(tmp){
									load = (load+MRan>QT)?QT:load+MRan;
								}else{
									load = (load+LRan>QT)?QT:load+LRan;
								}
							}
							flag = false;
						}
						out.write(" "+load);
						totalLoad += load;
					}else{
						out.write(" 0");
					}
				}
				out.write(" "+totalLoad);
				out.newLine();out.flush();
			}
		}
		out.close();
	}
	
	public static void setBursty(){
		isBursty = new boolean[totalInterval];
		for(int i = 0; i < totalInterval; i++){
			isBursty[i] = false;
		}
		
	}
	
	public static void setActivePattern(){
		activePattern = new boolean[totalTenant][totalInterval];
		for(int i=0;i<totalTenant;i++){
			for(int j=0;j<totalInterval;j++){
				activePattern[i][j] = false;
			}
		}
		
		int exchangeNumber = (int) (exchangeRatio*activeNumber);
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
			exchange(0, HConfig.totalTenant, exchangeNumber, activeTenant);
			for(int i = 0; i < activeNumber; i++){
				activePattern[activeTenant[i]][intervalId] = true;
			}
		}
	}
	
	public static void exchange(int startId, int total, int exN, int[] ori){
		int[] exa = Support.Rands(0, activeNumber, exN);
		int[] exin = Support.Rands(0, inactiveTenant.length, exN);
		int tmp;
		for(int i = 0; i < exN; i++){
			tmp = activeTenant[exa[i]];
			activeTenant[exa[i]] = inactiveTenant[exin[i]];
			inactiveTenant[exin[i]] = tmp;
		}
	}

}
