package hybridTest;

public class HTimer extends Thread {
	
	public void run(){
		int count = -1;
		try {
			HTimer.sleep(3000);
//			Main.checkDoSQL(1);
//			Main.checkResetActualQT(+1);
//			Main.checkSetQT(1);
//			synchronized(Main.mainThread){
//				Main.mainThread.notify();
//			}
//			for(int i = 0; i < 3; i++){
//				Main.actualThroughputPerTenant = new int[Main.tenantNumber];
//				HTimer.sleep(60000);
//				int[] tmp = Main.actualThroughputPerTenant.clone();
//				int tp = 0;
//				for(int j = 0; j < tmp.length; j++)
//					tp += tmp[j];
//				System.out.println(i+" "+Main.TYPE+" "+tp+"  ");
//			}
			for(int intervalId = 0; intervalId < Main.intervalNumber; intervalId++){
				for(int min = 0; min < Main.minPerInterval; min++){
					for(int i = 0; i < 20; i++){
						Main.checkDoSQL(1);
						if(i == 0){
							Main.checkResetActualQT(+1);
						}
						if(count < intervalId){
							Main.checkSetQT(1);
							count++;
						}
						synchronized(Main.mainThread){
							Main.mainThread.notify();
						}
						HTimer.sleep(2900);
					}
					HTimer.sleep(2000);
					Main.socketSender.sendInfo((intervalId*Main.minPerInterval+min),Main.throughputPerTenant.clone(), Main.actualThroughputPerTenant.clone());
					synchronized(Main.socketSender){
						Main.socketSender.notify();
					}
				}
			}
			Main.isActive = false;
			Main.socketWorking = false;
			
			HTimer.sleep(10000);
			synchronized(Main.mainThread){
				Main.mainThread.notify();
			}
			synchronized(Main.socketSender){
				Main.socketSender.notify();
			}
		}catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

}
