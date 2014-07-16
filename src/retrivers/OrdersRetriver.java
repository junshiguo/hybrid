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

public class OrdersRetriver extends Thread {
	public String url, username, password;
	public String voltdbServer;
	public int startId;
	public int tenantNumber;
	
	public Connection conn = null;
	public Statement stmt = null;
	Client voltdbConn = null;
	PreparedStatement[] statements;
	
	public OrdersRetriver(String url, String username, String password, String voltdbServer, int startId, int tenantNumber){
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
				statements[id] = conn.prepareStatement("UPDATE orders"+(id+startId)+" SET o_carrier_id = ? WHERE o_id = ? AND o_d_id = ? AND o_w_id = ?");
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
				String sql = "INSERT INTO orders"+(i+startId)+" (o_id, o_d_id, o_w_id, o_c_id, o_entry_d, o_carrier_id, o_ol_cnt, o_all_local) VALUES ";;
				String sql1 = "INSERT INTO orders"+(i+startId)+" (o_id, o_d_id, o_w_id, o_c_id, o_entry_d, o_ol_cnt, o_all_local) VALUES ";
				response = voltdbConn.callProcedure("PRSelectAllOrders", tenantId, 1, 0);
				if(response.getStatus() == ClientResponse.SUCCESS && response.getResults()[0].getRowCount() != 0){
					result = response.getResults()[0];
					for(int j=0; j<result.getRowCount(); j++){
						VoltTableRow row = result.fetchRow(j);
						String time = row.getTimestampAsSqlTimestamp("o_entry_d").toString();
						int index = time.lastIndexOf(".");
						if(index != -1) time = time.substring(0, index);
						if(row.get("o_carrier_id", VoltType.INTEGER) == null){
							dataExist = true;
							sql = sql +"("+row.get("o_id", VoltType.INTEGER)+","+row.get("o_d_id", VoltType.INTEGER)+","+row.get("o_w_id",VoltType.INTEGER)+
									","+row.get("o_c_id", VoltType.INTEGER)+",'"+time+"',"+row.get("o_carrier_id", VoltType.INTEGER)+
									","+row.get("o_ol_cnt", VoltType.INTEGER)+","+row.get("o_all_local", VoltType.INTEGER)+")";
							sql += " , ";	
						}else{
							dataExist1 = true;
							sql1 = sql1 +"("+row.get("o_id", VoltType.INTEGER)+","+row.get("o_d_id", VoltType.INTEGER)+","+row.get("o_w_id",VoltType.INTEGER)+
									","+row.get("o_c_id", VoltType.INTEGER)+",'"+time+
									"',"+row.get("o_ol_cnt", VoltType.INTEGER)+","+row.get("o_all_local", VoltType.INTEGER)+")";
							sql1 += " , ";
						}
					}
				}
				response = voltdbConn.callProcedure("PRSelectAllOrders", tenantId, 1, 1);
				if(response.getStatus() == ClientResponse.SUCCESS && response.getResults()[0].getRowCount() != 0){
					result = response.getResults()[0];
					for(int j=0; j<result.getRowCount(); j++){
						VoltTableRow row = result.fetchRow(j);
						String time = row.getTimestampAsSqlTimestamp("o_entry_d").toString();
						int index = time.lastIndexOf(".");
						if(index != -1) time = time.substring(0, index);
						if(row.get("o_carrier_id", VoltType.INTEGER) == null){
							dataExist = true;
							sql = sql +"("+row.get("o_id", VoltType.INTEGER)+","+row.get("o_d_id", VoltType.INTEGER)+","+row.get("o_w_id",VoltType.INTEGER)+
									","+row.get("o_c_id", VoltType.INTEGER)+",'"+time+"',"+row.get("o_carrier_id", VoltType.INTEGER)+
									","+row.get("o_ol_cnt", VoltType.INTEGER)+","+row.get("o_all_local", VoltType.INTEGER)+")";
							sql += " , ";	
						}else{
							dataExist1 = true;
							sql1 = sql1 +"("+row.get("o_id", VoltType.INTEGER)+","+row.get("o_d_id", VoltType.INTEGER)+","+row.get("o_w_id",VoltType.INTEGER)+
									","+row.get("o_c_id", VoltType.INTEGER)+",'"+time+
									"',"+row.get("o_ol_cnt", VoltType.INTEGER)+","+row.get("o_all_local", VoltType.INTEGER)+")";
							sql1 += " , ";
						}
					}
				}
				if(dataExist){
					int length = sql.length();
					sql = sql.substring(0, length-2);
					stmt.execute(sql);
					dataExist = false;
				}
				if(dataExist1){
					int length = sql1.length();
					sql1 = sql1.substring(0,length-2);
					stmt.execute(sql1);
					dataExist1= false;
				}
				
				//UPDATE orders"+id+" SET o_carrier_id = ? WHERE o_id = ? AND o_d_id = ? AND o_w_id = ?
				response = voltdbConn.callProcedure("PRSelectAllOrders", tenantId, 0, 1);
				if(response.getStatus() == ClientResponse.SUCCESS && response.getResults()[0].getRowCount() != 0){
					result = response.getResults()[0];
					for(int in=0;in<result.getRowCount();in++){
						VoltTableRow row = result.fetchRow(in);
						statements[i].setInt(1, (int) row.get("o_carrier_id", VoltType.INTEGER));
						statements[i].setInt(2, (int) row.get("o_id", VoltType.INTEGER));
						statements[i].setInt(3, (int) row.get("o_d_id", VoltType.INTEGER));
						statements[i].setInt(4, (int) row.get("o_w_id", VoltType.INTEGER));
						statements[i].addBatch();
					}
					statements[i].executeBatch();
				}
				voltdbConn.callProcedure("PRDeleteAllOrders", tenantId);
			} catch (IOException | ProcCallException | SQLException e) {
				e.printStackTrace();
			}
		}
		System.out.println("\nTABLE orders: "+startId+" ~ "+(startId+tenantNumber-1)+" truncated...");
		//******************************************************************************//
		try {
			conn.close();
			voltdbConn.close();
		} catch (SQLException | InterruptedException e) {
			e.printStackTrace();
		}	
				
	}

}
