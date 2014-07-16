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

public class DistrictRetriver extends Thread {
	public String url, username, password;
	public String voltdbServer;
	public int startId;
	public int tenantNumber;
	
	public Connection conn = null;
	public Statement stmt = null;
	Client voltdbConn = null;
	PreparedStatement[] statements;
	
	public DistrictRetriver(String url, String username, String password, String voltdbServer, int startId, int tenantNumber){
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
				statements[id] = conn.prepareStatement("UPDATE district"+(id+startId)+" SET d_next_o_id = ? , d_ytd = ? WHERE d_id = ? AND d_w_id = ?");
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
				response = voltdbConn.callProcedure("PRSelectAllDistrict", tenantId, 0, 1);
				if(response.getStatus() == ClientResponse.SUCCESS && response.getResults()[0].getRowCount() != 0){
					result = response.getResults()[0];
					for(int i=0; i<result.getRowCount(); i++){
						VoltTableRow row = result.fetchRow(i);
						statements[id].setInt(1, (int) row.get("d_next_o_id", VoltType.INTEGER));
						statements[id].setBigDecimal(2, row.getDecimalAsBigDecimal("d_ytd").setScale(12, BigDecimal.ROUND_HALF_DOWN));
						statements[id].setInt(3, (int) row.get("d_id", VoltType.INTEGER));
						statements[id].setInt(4, (int) row.get("d_w_id", VoltType.INTEGER));
						statements[id].addBatch();
					}
					statements[id].executeBatch();
				}
				voltdbConn.callProcedure("PRDeleteAllDistrict", tenantId);
			}catch(IOException | ProcCallException | SQLException e){
				e.printStackTrace();
			}
		}
		System.out.println("\nTABLE district: "+startId+" ~ "+(startId+tenantNumber-1)+" truncated...");
		//******************************************************************************//
		try {
			conn.close();
			voltdbConn.close();
		} catch (SQLException | InterruptedException e) {
			e.printStackTrace();
		}	
				
	}

}
