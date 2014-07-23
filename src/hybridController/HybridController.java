package hybridController;

import hybridConfig.HConfig;

import java.util.ArrayList;

public class HybridController {
	public static int port = 8899;
	public static int portWrite = 8890;
	public static ArrayList<SocketTask> task;
	public static boolean[] inPosition;
	public static boolean[] endTest;
	public static boolean ENDTEST = false;
	public static boolean[] usingVoltdb;
	public static boolean[] partiallyUsingVoltdb;
	
	public static void main(String[] args){
		HConfig.init(1000);
		
	}
	
	public static void init(){
		ENDTEST = false;
		task = new ArrayList<SocketTask>();
		inPosition = new boolean[18];
		endTest = new boolean[18];
		for(int i = 0; i < 18; i++){
			inPosition[i] = false;
			endTest[i] = false;
		}
		usingVoltdb = new boolean[HConfig.totalTenant];
		partiallyUsingVoltdb = new boolean[HConfig.totalTenant];
		for(int i = 0; i < HConfig.totalTenant; i++){
			usingVoltdb[i] = false;
			partiallyUsingVoltdb[i] = false;
		}
	}
	
}
