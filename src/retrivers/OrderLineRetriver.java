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

public class OrderLineRetriver extends Thread {
	public String url, username, password;
	public String voltdbServer;
	public int startId;
	public int tenantNumber;
	
	public Connection conn = null;
	public Statement stmt = null;
	Client voltdbConn = null;
	PreparedStatement[] statements;
	
	public OrderLineRetriver(String url, String username, String password, String voltdbServer, int startId, int tenantNumber){
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
				statements[id] = conn.prepareStatement("UPDATE order_line"+(id+startId)+" SET ol_delivery_d = ? WHERE ol_o_id = ? AND ol_d_id = ? AND ol_w_id = ?");
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
		boolean dataExist = false;
		boolean dataExist1 = false;
		for(int i=0; i<tenantNumber; i++){
			int tenantId = i + startId;
			try {
				String sql = "INSERT INTO order_line"+tenantId+" (ol_o_id, ol_d_id, ol_w_id, ol_number, ol_i_id, ol_supply_w_id, ol_delivery_d,"
						+ "ol_quantity, ol_amount, ol_dist_info) VALUES ";
				String sql1 = "INSERT INTO order_line"+tenantId+" (ol_o_id, ol_d_id, ol_w_id, ol_number, ol_i_id, ol_supply_w_id,"
						+ "ol_quantity, ol_amount, ol_dist_info) VALUES ";
				response = voltdbConn.callProcedure("PRSelectAllOrderLine", tenantId, 1, 0);
				if(response.getStatus() == ClientResponse.SUCCESS && response.getResults()[0].getRowCount() != 0){
					result = response.getResults()[0];
					for(int j=0; j<result.getRowCount(); j++){
						VoltTableRow row = result.fetchRow(j);
						if(row.getTimestampAsSqlTimestamp("ol_delivery_d") != null){
							dataExist = true;
							String time = row.getTimestampAsSqlTimestamp("ol_delivery_d").toString();
							int index = time.lastIndexOf(".");
							if(index != -1)
								time = time.substring(0, index);
							sql = sql +"("+row.get("ol_o_id", VoltType.INTEGER)+","+row.get("ol_d_id", VoltType.INTEGER)+","+row.get("ol_w_id",VoltType.INTEGER)+
									","+row.get("ol_number", VoltType.INTEGER)+","+row.get("ol_i_id", VoltType.INTEGER)+","+row.get("ol_supply_w_id", VoltType.INTEGER)+
									",'"+time+"',"+row.get("ol_quantity", VoltType.INTEGER)+","+row.getDecimalAsBigDecimal("ol_amount").setScale(6, BigDecimal.ROUND_HALF_DOWN).doubleValue()+
									",'"+row.getString("ol_dist_info")+"')";
								
							sql += " , ";
						}else{
							dataExist1 = true;
							sql1 = sql1 +"("+row.get("ol_o_id", VoltType.INTEGER)+","+row.get("ol_d_id", VoltType.INTEGER)+","+row.get("ol_w_id",VoltType.INTEGER)+
									","+row.get("ol_number", VoltType.INTEGER)+","+row.get("ol_i_id", VoltType.INTEGER)+","+row.get("ol_supply_w_id", VoltType.INTEGER)+
									","+row.get("ol_quantity", VoltType.INTEGER)+","+row.getDecimalAsBigDecimal("ol_amount").setScale(6, BigDecimal.ROUND_HALF_DOWN).doubleValue()+
									",'"+row.getString("ol_dist_info")+"')";
							sql1 += " , ";
						}
					}
				}
				response = voltdbConn.callProcedure("PRSelectAllOrderLine", tenantId, 1, 1);
				if(response.getStatus() == ClientResponse.SUCCESS && response.getResults()[0].getRowCount() != 0){
					result = response.getResults()[0];
					for(int j=0; j<result.getRowCount(); j++){
						VoltTableRow row = result.fetchRow(j);
						if(row.getTimestampAsSqlTimestamp("ol_delivery_d") != null){
							dataExist = true;
							String time = row.getTimestampAsSqlTimestamp("ol_delivery_d").toString();
							int index = time.lastIndexOf(".");
							if(index != -1) time = time.substring(0, index);
							sql = sql +"("+row.get("ol_o_id", VoltType.INTEGER)+","+row.get("ol_d_id", VoltType.INTEGER)+","+row.get("ol_w_id",VoltType.INTEGER)+
									","+row.get("ol_number", VoltType.INTEGER)+","+row.get("ol_i_id", VoltType.INTEGER)+","+row.get("ol_supply_w_id", VoltType.INTEGER)+
									",'"+time+"',"+row.get("ol_quantity", VoltType.INTEGER)+","+row.getDecimalAsBigDecimal("ol_amount").setScale(6, BigDecimal.ROUND_HALF_DOWN).doubleValue()+
									",'"+row.getString("ol_dist_info")+"')";
								
							sql += " , ";
						}else{
							dataExist1 = true;
							sql1 = sql1 +"("+row.get("ol_o_id", VoltType.INTEGER)+","+row.get("ol_d_id", VoltType.INTEGER)+","+row.get("ol_w_id",VoltType.INTEGER)+
									","+row.get("ol_number", VoltType.INTEGER)+","+row.get("ol_i_id", VoltType.INTEGER)+","+row.get("ol_supply_w_id", VoltType.INTEGER)+
									","+row.get("ol_quantity", VoltType.INTEGER)+","+row.getDecimalAsBigDecimal("ol_amount").setScale(6, BigDecimal.ROUND_HALF_DOWN).doubleValue()+
									",'"+row.getString("ol_dist_info")+"')";
							sql1 += " , ";
						}
					}
				}
				if(dataExist){
					sql = sql.substring(0, sql.length()-2);
					stmt.execute(sql);
					dataExist = false;
				}
				if(dataExist1){
					sql1 = sql1.substring(0, sql1.length()-2);
					stmt.execute(sql1);
					dataExist1 = false;
				}
				
				//UPDATE order_line"+id+" SET ol_delivery_d = ? WHERE ol_o_id = ? AND ol_d_id = ? AND ol_w_id = ?
				response = voltdbConn.callProcedure("PRSelectAllOrderLine", tenantId, 0, 1);
				if(response.getStatus() == ClientResponse.SUCCESS && response.getResults()[0].getRowCount() != 0){
					result = response.getResults()[0];
					for(int in=0;in<result.getRowCount();in++){
						VoltTableRow row = result.fetchRow(in);
						statements[i].setTimestamp(1, row.getTimestampAsSqlTimestamp("ol_delivery_d"));
						statements[i].setInt(2, (int) row.get("ol_o_id", VoltType.INTEGER));
						statements[i].setInt(3, (int) row.get("ol_d_id", VoltType.INTEGER));
						statements[i].setInt(4, (int) row.get("ol_w_id", VoltType.INTEGER));
						statements[i].addBatch();
					}
					statements[i].executeBatch();
				}	
				voltdbConn.callProcedure("PRDeleteAllOrderLine", tenantId);
			} catch (IOException | ProcCallException | SQLException e) {
				e.printStackTrace();
			}
		}
		System.out.println("\nTABLE order_line: "+startId+" ~ "+(startId+tenantNumber-1)+" truncated...");
		//******************************************************************************//
		try {
			conn.close();
			voltdbConn.close();
		} catch (SQLException | InterruptedException e) {
			e.printStackTrace();
		}
				
	}

}
