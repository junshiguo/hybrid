package retrivers;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;

import org.voltdb.VoltTable;
import org.voltdb.VoltTableRow;
import org.voltdb.VoltType;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcCallException;

import utility.DBManager;

public class StockRetriver extends Thread {
	public String url, username, password;
	public String voltdbServer;
	public int startId;
	public int tenantNumber;
	
	public Connection conn = null;
	public Statement stmt = null;
	Client voltdbConn = null;
	PreparedStatement[] statements;
	
	public StockRetriver(String url, String username, String password, String voltdbServer, int startId, int tenantNumber){
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
			statements = new PreparedStatement[tenantNumber];
			for(int id=0;id<tenantNumber;id++){
				statements[id] = conn.prepareStatement("UPDATE stock"+(id+startId)+" SET s_quantity = ? WHERE s_i_id = ? AND s_w_id = ?");
			}
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
		for(int id = 0; id < tenantNumber; id++){
			int tenantId = id + startId;
			try{
				response = voltdbConn.callProcedure("PRSelectAllStock", tenantId, 0, 1);
				if(response.getStatus() == ClientResponse.SUCCESS && response.getResults()[0].getRowCount() != 0){
					result = response.getResults()[0];
					for(int i=0; i<result.getRowCount(); i++){
						VoltTableRow row = result.fetchRow(i);
						statements[id].setInt(1, (int) row.get("s_quantity", VoltType.INTEGER));
						statements[id].setInt(2, (int) row.get("s_i_id", VoltType.INTEGER));
						statements[id].setInt(3, (int) row.get("s_w_id", VoltType.INTEGER));
						statements[id].addBatch();
					}
					statements[id].executeBatch();
				}
				voltdbConn.callProcedure("PRDeleteAllStock", tenantId);
			}catch(IOException | ProcCallException | SQLException e){
				e.printStackTrace();
			}
		}
//			voltdbConn.callProcedure("PRTruncateAll", RetriveMonitor.STOCK);
		System.out.println("\nTABLE stock: "+startId+" ~ "+(startId+tenantNumber-1)+" truncated...");
		//******************************************************************************//
		try {
			conn.close();
			voltdbConn.close();
		} catch (SQLException | InterruptedException e) {
			e.printStackTrace();
		}
				
	}

}
