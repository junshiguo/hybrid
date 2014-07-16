package retrivers;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import org.voltdb.VoltTable;
import org.voltdb.VoltTableRow;
import org.voltdb.VoltType;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcCallException;

import utility.DBManager;

public class NewOrdersRetriver extends Thread {
	public String url, username, password;
	public String voltdbServer;
	public int startId;
	public int tenantNumber;
	
	public Connection conn = null;
	public Statement stmt = null;
	Client voltdbConn = null;
	
	public NewOrdersRetriver(String url, String username, String password, String voltdbServer, int startId, int tenantNumber){
		this.url = url;
		this.username = username;
		this.password = password;
		this.voltdbServer = voltdbServer;
		this.startId = startId;
		this.tenantNumber = tenantNumber;
	}
	
	@Override
	public void run(){
		try {
			conn = DBManager.connectDB(url, username, password);
			stmt = conn.createStatement();
		} catch (Exception e1) {
			System.out.println("error in creating or preparing mysql statement for retriving data...");
		}
		voltdbConn = DBManager.connectVoltdb(voltdbServer);
		if(voltdbConn == null){
			System.out.println("error connecting to voltdb while retriving data...");
		}
		//******************************************************************************//
		ClientResponse response = null;
		VoltTable result = null;
		boolean dataExist = false;
		for(int id = 0; id < tenantNumber; id++){
			int tenantId = id + startId;
			try{
				String sql = "INSERT INTO new_orders"+tenantId+" (no_o_id, no_d_id, no_w_id) VALUES ";;
				response = voltdbConn.callProcedure("PRSelectAllNewOrders", tenantId, 1, 0);
				if(response.getStatus() == ClientResponse.SUCCESS && response.getResults()[0].getRowCount() != 0){
					dataExist = true;
					result = response.getResults()[0];
					for(int i = 0; i<result.getRowCount(); i++){
						VoltTableRow row = result.fetchRow(i);
						sql = sql + "("+row.get("no_o_id", VoltType.INTEGER)+","+row.get("no_d_id", VoltType.INTEGER)+
								","+row.get("no_w_id", VoltType.INTEGER)+")";
						sql += " , ";
					}
					if(dataExist){
						sql = sql.substring(0, sql.length()-2);
						stmt.execute(sql);
						dataExist = false;
					}
				}
				voltdbConn.callProcedure("PRDeleteAllNewOrders", tenantId);
			}catch(IOException | ProcCallException | SQLException e){
				e.printStackTrace();
			}
		}
		System.out.println("\nTABLE new_orders: "+startId+" ~ "+(startId+tenantNumber-1)+" truncated...");
		//******************************************************************************//
		try {
			conn.close();
			voltdbConn.close();
		} catch (SQLException | InterruptedException e) {
			e.printStackTrace();
		}
				
		
	}

}
