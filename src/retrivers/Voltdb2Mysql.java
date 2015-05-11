package retrivers;

public class Voltdb2Mysql extends Thread{
	
	public int tenantId, volumnId;
	
	public Voltdb2Mysql(int tenantId, int volumnId){
		this.tenantId = tenantId;
		this.volumnId = volumnId;
	}
	
	public void run(){
		CustomerRetriver cr = new CustomerRetriver(Retriver.dbURL, Retriver.dbUsername, Retriver.dbPassword, Retriver.voltdbServer, tenantId, volumnId);
		cr.start();
		DistrictRetriver dr = new DistrictRetriver(Retriver.dbURL, Retriver.dbUsername, Retriver.dbPassword, Retriver.voltdbServer, tenantId, volumnId);
		dr.start();
		HistoryRetriver hr = new HistoryRetriver(Retriver.dbURL, Retriver.dbUsername, Retriver.dbPassword, Retriver.voltdbServer, tenantId, volumnId);
		hr.start();
		NewOrdersRetriver nor = new NewOrdersRetriver(Retriver.dbURL, Retriver.dbUsername, Retriver.dbPassword, Retriver.voltdbServer, tenantId, volumnId);
		nor.start();
		OrderLineRetriver olr = new OrderLineRetriver(Retriver.dbURL, Retriver.dbUsername, Retriver.dbPassword, Retriver.voltdbServer, tenantId, volumnId);
		olr.start();
		OrdersRetriver or = new OrdersRetriver(Retriver.dbURL, Retriver.dbUsername, Retriver.dbPassword, Retriver.voltdbServer, tenantId, volumnId);
		or.start();
		StockRetriver sr = new StockRetriver(Retriver.dbURL, Retriver.dbUsername, Retriver.dbPassword, Retriver.voltdbServer, tenantId, volumnId);
		sr.start();
		WarehouseRetriver wr = new WarehouseRetriver(Retriver.dbURL, Retriver.dbUsername, Retriver.dbPassword, Retriver.voltdbServer, tenantId, volumnId);
		wr.start();
		ItemRetriver ir = new ItemRetriver(Retriver.dbURL, Retriver.dbUsername, Retriver.dbPassword, Retriver.voltdbServer, tenantId, volumnId);
		ir.start();
		try {
			cr.join();
			dr.join();
			hr.join();
			nor.join();
			olr.join();
			or.join();
			sr.join();
			wr.join();
			ir.join();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
	
}
