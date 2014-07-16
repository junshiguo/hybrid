package retrivers;

import java.io.IOException;
import java.math.BigDecimal;
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

public class WarehouseRetriver extends Thread {
	public String url, username, password;
	public String voltdbServer;
	public int startId;
	public int tenantNumber;
	
	public Connection conn = null;
	public Statement stmt = null;
	Client voltdbConn = null;
	PreparedStatement[] statements;
	
	public WarehouseRetriver(String url, String username, String password, String voltdbServer, int startId, int tenantNumber){
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
				statements[id] = conn.prepareStatement("UPDATE warehouse"+(id+startId)+" SET w_ytd = ? WHERE w_id = ?");
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
				response = voltdbConn.callProcedure("PRSelectAllWarehouse", tenantId, 0, 1);
				if(response.getStatus() == ClientResponse.SUCCESS && response.getResults()[0].getRowCount() != 0){
					result = response.getResults()[0];
					for(int i=0; i<result.getRowCount(); i++){
						VoltTableRow row = result.fetchRow(i);
						statements[id].setBigDecimal(1, row.getDecimalAsBigDecimal("w_ytd").setScale(12, BigDecimal.ROUND_HALF_DOWN));
						statements[id].setInt(2, (int) row.get("w_id", VoltType.INTEGER));
						statements[id].addBatch();
					}
					statements[id].executeBatch();
				}
				voltdbConn.callProcedure("PRDeleteAllWarehouse", tenantId);
			}catch(IOException | ProcCallException | SQLException e){
				System.out.println(statements[id].toString());
				e.printStackTrace();
			}
		}
		System.out.println("\nTABLE warehouse: "+startId+" ~ "+(startId+tenantNumber-1)+" truncated...");
		//******************************************************************************//
		try {
			conn.close();
			voltdbConn.close();
		} catch (SQLException | InterruptedException e) {
			e.printStackTrace();
		}	
		
	}

}
