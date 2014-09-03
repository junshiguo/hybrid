package hybridController;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;

import hybridConfig.HConfig;

public class HybridController extends Thread {
	public static int port = 8899;
	public static SocketTask[] sendTask;
	public static SocketTask[] receiveTask;
	public static boolean[] inPosition;
	public static boolean[] senderInPosition;
//	public static boolean[] endTest;
	public static int typeNumber = 18;
	public static int totalTime = 30; //minutes
//	public static boolean ENDTEST = false;
	public static boolean[] usingVoltdb;
	public static boolean[] partiallyUsingVoltdb;
	public static int[] lateTenant;
	public static int[] lateQuery;
	public static SocketServer socketServer;
	public static boolean informStart = false;
	public static boolean controllerWorking = false;
	public static HybridController controller;
	public static String server = "127.0.0.1";
	
	public static void main(String[] args){
//		server = "10.20.2.211";
		int tenantNumber = 3000;
		if(args.length > 0){
			tenantNumber = Integer.parseInt(args[0]);
		}
		HConfig.init(tenantNumber);
		init();
		socketServer = new SocketServer();
		socketServer.start();
		controller = new HybridController();
		controller.start();
		//check whether it is needed to move data between mysql and voltdb 
		//use sendTask[TYPE].sendInfo(tenantId, usingV, usingPV);
	}
	
	public ArrayList<ArrayList<Integer>> tenantInVoltdb = new ArrayList<ArrayList<Integer>>();
	public static int retriveConcurrence = 10;
	public static int loadConcurrence = 2;
	public void run(){
		try{
			synchronized(this){
				this.wait();
			}
			HybridController.sleep((5*60 + 10)*1000);
			HybridController.writeViolation();
		}catch(InterruptedException | NumberFormatException e){
			
		}
	}
	public void run2(){
		try{
			BufferedReader burstReader = new BufferedReader(new FileReader("data/burst.txt"));
			String str;
			while((str = burstReader.readLine()) != null){
				String[] offloadingIds = str.split(" ");
				ArrayList<Integer> tmp = new ArrayList<Integer>();
				for(int i = 0; i < offloadingIds.length; i++){
					tmp.add(Integer.parseInt(offloadingIds[i].trim()));
				}
				tenantInVoltdb.add(tmp);
			}
			burstReader.close();
			ArrayList<Integer> toLoad = new ArrayList<Integer>();
			ArrayList<Integer> row = tenantInVoltdb.get(2);
			ArrayList<Integer> nextRow = tenantInVoltdb.get(3);
			for(int i = 1; i < row.size(); i++){
				toLoad.add(row.get(i));
			}
			for(int i = 1; i < nextRow.size(); i++){
				if(toLoad.contains(nextRow.get(i)) == false){
					toLoad.add(nextRow.get(i));
				}
			}
			LoaderThread.setToLoad(toLoad);
			LoaderThread[] loader = new LoaderThread[HybridController.loadConcurrence];
			for(int i = 0; i < HybridController.loadConcurrence; i++){
				loader[i] = new LoaderThread();
			}
			RetriveThread.setToRetrive(toLoad);
			RetriveThread[] retriver = new RetriveThread[HybridController.retriveConcurrence];
			for(int i = 0; i < HybridController.retriveConcurrence; i++){
				retriver[i] = new RetriveThread();
			}
			synchronized(this){
				this.wait();
			}
			HybridController.sleep(5*60*1000);
			for(int i = 0; i < HybridController.loadConcurrence; i++){
				loader[i].start();
			}
			HybridController.sleep((15*60+30)*1000);
			for(int i = 0; i < HybridController.retriveConcurrence; i++){
				retriver[i].start();
			}
			HybridController.sleep(9*60*1000);
			
			FileWriter fstream = null;
			try {
				fstream = new FileWriter("violation.txt", false);
				BufferedWriter out = new BufferedWriter(fstream);
				for(int i = 0; i < HybridController.totalTime; i++){
					out.write(i+" "+HybridController.lateTenant[i]+" "+HybridController.lateQuery[i]);
					out.newLine();
				}
				out.flush();
				out.close();
			} catch (IOException e1) {
				e1.printStackTrace();
			}
		}catch (IOException | InterruptedException | NumberFormatException e) {
				e.printStackTrace();
		}
	}
	
	public static ArrayList<Integer> getToLoad() throws NumberFormatException, IOException{
		ArrayList<ArrayList<Integer>> tenantInVoltdb = new ArrayList<ArrayList<Integer>>();
		BufferedReader burstReader = new BufferedReader(new FileReader("data/burst.txt"));
		String str;
		while((str = burstReader.readLine()) != null){
			String[] offloadingIds = str.split(" ");
			ArrayList<Integer> tmp = new ArrayList<Integer>();
			for(int i = 0; i < offloadingIds.length; i++){
				tmp.add(Integer.parseInt(offloadingIds[i].trim()));
			}
			tenantInVoltdb.add(tmp);
		}
		burstReader.close();
		ArrayList<Integer> toLoad = new ArrayList<Integer>();
		ArrayList<Integer> row = tenantInVoltdb.get(0);
		for(int i = 1; i < row.size(); i++){
			toLoad.add(row.get(i));
		}
		for(int rowId = 1; rowId < 5; rowId++){
			ArrayList<Integer> nextRow = tenantInVoltdb.get(rowId);
			for(int i = 1; i < nextRow.size(); i++){
				if(toLoad.contains(nextRow.get(i)) == false){
					toLoad.add(nextRow.get(i));
				}
			}
		}
		return toLoad;
	}
	public static void offloadData(){
		try{
			ArrayList<Integer> toLoad = getToLoad();
			System.out.println(toLoad.size());
			LoaderThread.setToLoad(toLoad);
			LoaderThread[] loader = new LoaderThread[HybridController.loadConcurrence];
			for(int i = 0; i < HybridController.loadConcurrence; i++){
				loader[i] = new LoaderThread();
			}
			for(int i = 0; i < HybridController.loadConcurrence; i++){
				loader[i].start();
			}
		}catch (IOException | NumberFormatException e) {
				e.printStackTrace();
		}
	}
	public static void writeViolation(){
		FileWriter fstream = null;
		try {
			fstream = new FileWriter("violation.txt", false);
			BufferedWriter out = new BufferedWriter(fstream);
			for(int i = 0; i < HybridController.totalTime; i++){
				out.write(i+" "+HybridController.lateTenant[i]+" "+HybridController.lateQuery[i]);
				out.newLine();
			}
			out.flush();
			out.close();
		} catch (IOException e1) {
			e1.printStackTrace();
		}
	}
	
	public static synchronized void launchTask(SocketTask st, int type, boolean isSender){
		if(isSender){
			sendTask[type] = st;
			senderInPosition[type] = true;
		}else{
			receiveTask[type] = st;
			inPosition[type] = true;
		}
		
		if(informStart == true)	return;
		boolean flag = true;
		for(int i = 0; i < typeNumber; i++){
			if(inPosition[i] == false || senderInPosition[i] == false)
				flag = false;
		}
		if(flag){
			ArrayList<Integer> toLoad;
			try {
				toLoad = getToLoad();
				System.out.println(toLoad.size());
				for(int i = 0; i < toLoad.size(); i++){
					int tenantId = toLoad.get(i);
					int volumnId = i%50;
					HybridController.sendTask[HConfig.getType(tenantId)].sendInfo(tenantId, 1, 0, volumnId);
					Thread.sleep(300);
				}
			} catch (NumberFormatException | IOException | InterruptedException e) {
				e.printStackTrace();
			}
			for(int i = 0; i < typeNumber; i++){
				sendTask[i].sendInfo("all in position");
			}
			informStart = true;
			synchronized(HybridController.controller){
				controller.notify();
			}
		}
	}
	
	public static void init(){
//		ENDTEST = false;
		sendTask = new SocketTask[typeNumber];
		receiveTask = new SocketTask[typeNumber];
		inPosition = new boolean[typeNumber];
		senderInPosition = new boolean[typeNumber];
//		endTest = new boolean[typeNumber];
		for(int i = 0; i < typeNumber; i++){
			inPosition[i] = false;
			senderInPosition[i] = false;
//			endTest[i] = false;
		}
		usingVoltdb = new boolean[HConfig.totalTenant];
		partiallyUsingVoltdb = new boolean[HConfig.totalTenant];
		for(int i = 0; i < HConfig.totalTenant; i++){
			usingVoltdb[i] = false;
			partiallyUsingVoltdb[i] = false;
		}
		lateTenant = new int[totalTime];
		lateQuery = new int[totalTime];
		for(int i = 0; i < totalTime; i++){
			lateTenant[i] = lateQuery[i] = 0;
		}
	}
	
}
