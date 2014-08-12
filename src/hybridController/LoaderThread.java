package hybridController;

import java.util.ArrayList;

public class LoaderThread extends Thread {
	public static String server = "127.0.0.1";
	public static ArrayList<Integer> toLoad;
	
	public static void setToLoad(ArrayList<Integer> to){
		toLoad = new ArrayList<Integer>(to);
	}
	public static synchronized int nextToLoad(){
		int ret = -1;
		if(toLoad.isEmpty() == false){
			ret = toLoad.get(0);
			toLoad.remove(0);
		}
		return ret;
	}
	
	public void run(){
		int next;
		while((next = LoaderThread.nextToLoad()) != -1){
			DataMover m = new DataMover("jdbc:mysql://"+server+"/tpcc3000", "remote", "remote", server, next, true);
			m.start();
			try {
				m.join();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}
	
	public ArrayList<DataMover> mover = new ArrayList<DataMover>();
	public void oldrun(){
		System.out.println("********start offloading********");
		long start = System.nanoTime();
		for(int i = 0; i < toLoad.size(); i++){
			DataMover m = new DataMover("jdbc:mysql://"+server+"/tpcc3000", "remote", "remote", server, toLoad.get(i), true);
			m.start();
			mover.add(m);
			DataMover m2 = new DataMover("jdbc:mysql://"+server+"/tpcc3000", "remote", "remote", server, toLoad.get(i+1), true);
			m2.start();
			mover.add(m2);
			DataMover m3 = new DataMover("jdbc:mysql://"+server+"/tpcc3000", "remote", "remote", server, toLoad.get(i+2), true);
			m3.start();
			mover.add(m3);
			i++;
			try {
				m.join();m2.join();m3.join();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
//				try {
//					LoaderThread.sleep(3500);
//				} catch (InterruptedException e) {
//					e.printStackTrace();
//				}
		}
//		for(int i = 0; i < mover.size(); i++){
//			try {
//				mover.get(i).join();
//			} catch (InterruptedException e) {
//				e.printStackTrace();
//			}
//		}
		System.out.println("********end offloading********");
		long end = System.nanoTime();
		System.out.println("total time: "+(end-start)/1000000000.0);
	}

}
