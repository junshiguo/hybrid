package hybridTest;

public class HTimer extends Thread {
	
	public void run(){
		int count = -1;
		try {
			HTimer.sleep(3000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		for(int intervalId = 0; intervalId < Main.intervalNumber; intervalId++){
			for(int min = 0; min < Main.minPerInterval; min++){
				for(int i = 0; i < 20; i++){
					Main.doSQLNow++;
					if(i == 0){
						Main.resetActualQT ++;
					}
					if(count < intervalId){
						Main.setQTNow++;
						count++;
					}
					synchronized(Main.mainThread){
						Main.mainThread.notify();
					}
					try {
						HTimer.sleep(3000);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
				Main.socketSender.sendInfo((intervalId*Main.minPerInterval+min),Main.throughputPerTenant.clone(), Main.actualThroughputPerTenant.clone());
				synchronized(Main.socketSender){
					Main.socketSender.notify();
				}
			}
		}
		Main.isActive = false;
		Main.socketWorking = false;
		
		try {
			HTimer.sleep(10000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		synchronized(Main.mainThread){
			Main.mainThread.notify();
		}
		synchronized(Main.socketSender){
			Main.socketSender.notify();
		}
	}

}
