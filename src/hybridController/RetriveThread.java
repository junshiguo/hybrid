package hybridController;

import java.util.ArrayList;

public class RetriveThread extends Thread {
	public static String server = "127.0.0.1";
	public static ArrayList<Integer> toRetrive;
	public static void setToRetrive(ArrayList<Integer> to){
		toRetrive = to;
	}
	public static synchronized int nextToRetrive(){
		int ret = -1;
		if(toRetrive.isEmpty() == false){
			ret = toRetrive.get(0);
			toRetrive.remove(0);
		}
		return ret;
	}
	
	public void run(){
		int next;
		while((next = RetriveThread.nextToRetrive()) != -1){
			DataMover m = new DataMover("jdbc:mysql://"+server+"/tpcc3000", "remote", "remote", server, next, false);
			m.start();
			try {
				m.join();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}
}
