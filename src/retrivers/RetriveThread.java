package retrivers;

public class RetriveThread extends Thread {
	
	public void run(){
		Retriver next;
		while((next = Retriver.next()) != null){
			Voltdb2Mysql m = new Voltdb2Mysql(next.tenantId, next.volumnId);
			m.start();
			try {
				m.join();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}
}
