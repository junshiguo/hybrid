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

/**
 * retrive data for tenants whose id is in range [startId, startId+tenantNumber)
 * @author jsguo
 *
 */
public class CustomerRetriver extends Thread {
	public String url, username, password;
	public String voltdbServer;
	public int startId;
	public int tenantNumber;
	
	public Connection conn = null;
	public Statement stmt = null;
	Client voltdbConn = null;
	PreparedStatement[] statements;
	
	public CustomerRetriver(String url, String username, String password, String voltdbServer, int startId, int tenantNumber){
		this.url = url;
		this.username = username;
		this.password = password;
		this.voltdbServer = voltdbServer;
		this.startId = startId;
		this.tenantNumber = tenantNumber;
	}

	@Override
	public void run() {
		try {
			conn = DBManager.connectDB(url, username, password);
			stmt = conn.createStatement();
			statements = new PreparedStatement[tenantNumber];
			for(int id=0;id<tenantNumber;id++){
				statements[id] = conn.prepareStatement("UPDATE customer"+(id+startId)+" SET c_balance = ? , c_data = ? , c_delivery_cnt = ? WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?");
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
		for(int index = 0; index < tenantNumber; index++){
			int tenantId = index + startId;
			try{
				response = voltdbConn.callProcedure("PRSelectAllCustomer", tenantId, 0, 1);
				if(response.getStatus() == ClientResponse.SUCCESS && response.getResults()[0].getRowCount() != 0){
					result = response.getResults()[0];
					for(int i=0; i<result.getRowCount(); i++){
						VoltTableRow row = result.fetchRow(i);
						statements[index].setBigDecimal(1, row.getDecimalAsBigDecimal("c_balance").setScale(12, BigDecimal.ROUND_HALF_DOWN));
						statements[index].setString(2, row.getString("c_data"));
						statements[index].setInt(3, (int) row.get("c_delivery_cnt", VoltType.INTEGER));
						statements[index].setInt(4, (int) row.get("c_w_id", VoltType.INTEGER));
						statements[index].setInt(5, (int) row.get("c_d_id", VoltType.INTEGER));
						statements[index].setInt(6, (int) row.get("c_id", VoltType.INTEGER));
						statements[index].addBatch();
					}
					statements[index].executeBatch();
				}
				voltdbConn.callProcedure("PRDeleteAllCustomer", tenantId);
			}catch(IOException | ProcCallException | SQLException e){
				e.printStackTrace();
			}
		}
		System.out.println("\nTABLE customer: "+startId+" truncated...");
		//******************************************************************************//
		try {
			conn.close();
			voltdbConn.close();
		} catch (SQLException | InterruptedException e) {
			e.printStackTrace();
		}	
		
	}

}
