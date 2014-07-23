package hybridTest;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Vector;

import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcCallException;

import utility.DBManager;

public class HConnection extends Thread {
	public int connId;
	public int tenantId;
	public String mysqlURL;
	public String mysqlUsername;
	public String mysqlPassword;
	public String voltdbServer;
	public Connection conn;
	public Client voltdbConn;
	public PreparedStatement[] statements;
	public int doSQLNow = 0;  //set in Tenant. very important
	public Vector<WaitList> waitList;
	
	public class WaitList{
		public int sqlId;
		public Object[] para;
		public int[] paraType;
		public int paraNumber;
		public WaitList(int sqlId, Object[] para, int[] paraType, int paraNumber){
			this.sqlId = sqlId;
			this.para = para;
			this.paraType = paraType;
			this.paraNumber = paraNumber;
		}
	}

	public HConnection(int connId, int id, String url, String username, String password, String serverlist){
		this.connId = connId;
		this.tenantId = id;
		this.mysqlURL = url;
		this.mysqlUsername = username;
		this.mysqlPassword = password;
		this.voltdbServer = serverlist;
		statements = new PreparedStatement[44];
		waitList = new Vector<WaitList>();
	}

	public void run(){
		conn = DBManager.connectDB(mysqlURL, mysqlUsername, mysqlPassword);
		if(conn == null){
			System.out.println("Tenant "+tenantId+" connecting mysql failed...");
		}
//		voltdbConn = DBManager.connectVoltdb(voltdbServer);
//		if(voltdbConn == null){
//			System.out.println("Tenant "+tenantId+" connectiong voltdb failed...");
//		}
		try {
			sqlPrepare();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		while(true){
			if(doSQLNow > 0 && waitList.isEmpty() == false){
				WaitList tmp = waitList.firstElement();
				waitList.remove(0);
				doSQL(tmp.sqlId, tmp.para, tmp.paraType, tmp.paraNumber);
				this.doSQLNow --;
			}
		}
	}
	
	public boolean doSQL(int sqlId, Object[] para, int[] paraType, int paraNumber){
		boolean success = false;
		long start = System.nanoTime();
		if(Main.onlyMysql == true){ //only mysql, for mysql test
			success = doSQLInMysql(sqlId, paraNumber, para, paraType);
		}else if(this.isPartiallUsingVoltdb() == true){ // this tenant partially uses voltdb
			success = doSQLInVoltdb(sqlId, paraNumber, para, true, true);
			if((!success && sqlId<21) || sqlId==15 || sqlId==18 || sqlId==19 || sqlId==34)
				success = doSQLInMysql(sqlId, paraNumber, para, paraType);
		}else if(this.isUsingVoltdb() == true){ // this tenant has all his data in voltdb
			success = doSQLInVoltdb(sqlId, paraNumber, para, false, false);
		}else{ // this tenant has all his data in mysql
			success = doSQLInMysql(sqlId, paraNumber, para, paraType);
		}
		if(success && sqlId > 20){
			try {
				this.conn.commit();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		long end = System.nanoTime();
		if(success == true){
			PerformanceMonitor.timePerQuery.add(end - start);
			PerformanceMonitor.actualThroughputPerTenant[this.tenantId]++;
			if(sqlId <= 20)	PerformanceMonitor.readQuery++;
			else PerformanceMonitor.writeQuery++;
		}
		return success;
	}
	
	public boolean doSQLInMysql(int sqlId, int paraNumber, Object[] para,
			int[] paraType) {
		int i = 0;
		try {
			for (i = 0; i < paraNumber; i++) {
				switch (paraType[i]) {
				case 0:
					statements[sqlId].setInt(i+1, (int) para[i]);
					break;
				case 1:
					statements[sqlId].setString(i+1, (String)para[i]);
					break;
				case 2:
					statements[sqlId].setFloat(i+1, (float)para[i]);
					break;
				case 3:
					statements[sqlId].setDouble(i+1, (double)para[i]);
					break;
					default:
				}
			}
			statements[sqlId].execute();
			return true;
			//System.out.println(threadId +" sta "+sqlId+" : "+ Tenant.statements[threadId][sqlId].toString());
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println("thread id: "+this.tenantId +"; sql id: "+sqlId+"; i= "+i+" : exception??");
			return false;
		}
	}
	
	public boolean doSQLInVoltdb(int sqlId, int paraNumber, Object[] para, boolean checkUpdate, boolean careResult) {
		if(this.voltdbConn == null){
			this.voltdbConn = DBManager.connectVoltdb(this.voltdbServer);
		}
		//*******************************set parameters for volt procedures****************************************//
		try {
			ClientResponse response = null;
			if(sqlId > 24 && sqlId < 34 && checkUpdate == true){//update
				//******************check if data is in voltdb, if not, insert**********************//
				insertForUpdate(sqlId, paraNumber, para);
			}
			//**********************communal part****************************//
			switch(paraNumber){
			case 1:
				response = voltdbConn.callProcedure("Procedure"+sqlId, this.tenantId, para[0]);
				break;
			case 2:
				response = voltdbConn.callProcedure("Procedure"+sqlId, this.tenantId, para[0], para[1]);
				break;
			case 3:
				response = voltdbConn.callProcedure("Procedure"+sqlId, this.tenantId, para[0], para[1], para[2]);
				break;
			case 4:
				response = voltdbConn.callProcedure("Procedure"+sqlId, this.tenantId, para[0], para[1], para[2], para[3]);
				break;
			case 5:
				response = voltdbConn.callProcedure("Procedure"+sqlId, this.tenantId, para[0], para[1], para[2], para[3], para[4]);
				break;
			case 6:
				response = voltdbConn.callProcedure("Procedure"+sqlId, this.tenantId, para[0], para[1], para[2], para[3], para[4], para[5]);
				break;
			case 7:
				response = voltdbConn.callProcedure("Procedure"+sqlId, this.tenantId, para[0], para[1], para[2], para[3], para[4], para[5], para[6]);
				break;
			case 8:
				response = voltdbConn.callProcedure("Procedure"+sqlId, this.tenantId, para[0], para[1], para[2], para[3], para[4], para[5], para[6], para[7]);
				break;
			case 9:
				response = voltdbConn.callProcedure("Procedure"+sqlId, this.tenantId, para[0], para[1], para[2], para[3], para[4], para[5], para[6], para[7], para[8]);
				break;
			case 10:
				response = voltdbConn.callProcedure("Procedure"+sqlId, this.tenantId, para[0], para[1], para[2], para[3], para[4], para[5], para[6], para[7], para[8], para[9]);
				break;
			}
			
			if(response.getStatus() != ClientResponse.SUCCESS){
				System.out.println("response failed");
				return false;
			}
			if(careResult == false){
				return true;
			}
			long rets = response.getResults()[0].asScalarLong();
			if(rets == 0)
				return false;
			else return true;
		} catch (IOException | ProcCallException e) {
			e.printStackTrace();
			return false;
		}
	}
	
	public void insertForUpdate(int sqlId, int paraNumber, Object[] para){
		ClientResponse response = null;
		ResultSet rs = null;
		try{
			switch(sqlId){
			case 25:
				response = voltdbConn.callProcedure("ProcedureSelectDistrict", this.tenantId, para[0], para[1]);
				if (response.getStatus() != ClientResponse.SUCCESS) {
					System.out.println("select for update response failed");
					return;
				}
				if(response.getResults()[0].asScalarLong() == 0){ //insert needed
					statements[35].setInt(1, (int) para[0]);
					statements[35].setInt(2, (int) para[1]);
					rs = statements[35].executeQuery();
					while(rs.next()){
						voltdbConn.callProcedure("ProcedureInsertDistrict", this.tenantId, rs.getInt("d_id"), rs.getInt("d_w_id"), 
								rs.getString("d_name"), rs.getString("d_street_1"), rs.getString("d_street_2"), rs.getString("d_city"), 
								rs.getString("d_state"), rs.getString("d_zip"),	rs.getDouble("d_tax"), rs.getDouble("d_ytd"), 
								rs.getInt("d_next_o_id"), 0, 1);
					}
				}
				break;
			case 26:
				response = voltdbConn.callProcedure("ProcedureSelectStock", this.tenantId, para[1], para[2]);
				if (response.getStatus() != ClientResponse.SUCCESS) {
					System.out.println("select for update response failed");
					return;
				}
				if(response.getResults()[0].asScalarLong() == 0){
					statements[36].setInt(1, (int) para[1]);
					statements[36].setInt(2, (int) para[2]);
					rs = statements[36].executeQuery();
					while(rs.next()){
						voltdbConn.callProcedure("ProcedureInsertStock", this.tenantId, rs.getInt("s_i_id"), rs.getInt("s_w_id"),
								rs.getInt("s_quantity"), rs.getString("s_dist_01"), rs.getString("s_dist_02"), rs.getString("s_dist_03"), 
								rs.getString("s_dist_04"), rs.getString("s_dist_05"), rs.getString("s_dist_06"), rs.getString("s_dist_07"), 
								rs.getString("s_dist_08"), rs.getString("s_dist_09"), rs.getString("s_dist_10"), rs.getDouble("s_ytd"), 
								rs.getInt("s_order_cnt"), rs.getInt("s_remote_cnt"), rs.getString("s_data"), 0, 1);
					}
				}
				break;
			case 27:
				response = voltdbConn.callProcedure("ProcedureSelectWarehouse", this.tenantId, para[1]);
				if (response.getStatus() != ClientResponse.SUCCESS) {
					System.out.println("select for update response failed");
					return;
				}
				if(response.getResults()[0].asScalarLong() == 0){
					statements[37].setInt(1, (int) para[1]);
					rs = statements[37].executeQuery();
					while(rs.next()){
						voltdbConn.callProcedure("ProcedureInsertWarehouse", this.tenantId, rs.getInt("w_id"), 
								rs.getString("w_name"), rs.getString("w_street_1"), rs.getString("w_street_2"), rs.getString("w_city"), 
								rs.getString("w_state"), rs.getString("w_zip"), rs.getDouble("w_tax"), rs.getDouble("w_ytd"), 
								0, 1);
					}
				}
				break;
			case 28:
				response = voltdbConn.callProcedure("ProcedureSelectDistrict", this.tenantId, para[2], para[1]);
				if (response.getStatus() != ClientResponse.SUCCESS) {
					System.out.println("select for update response failed");
					return;
				}
				if(response.getResults()[0].asScalarLong() == 0){
					statements[38].setInt(1, (int) para[1]);
					statements[38].setInt(2, (int) para[2]);
					rs = statements[38].executeQuery();
					while(rs.next()){
						voltdbConn.callProcedure("ProcedureInsertDistrict", this.tenantId, rs.getInt("d_id"), rs.getInt("d_w_id"), 
								rs.getString("d_name"), rs.getString("d_street_1"), rs.getString("d_street_2"), rs.getString("d_city"), 
								rs.getString("d_state"), rs.getString("d_zip"),	rs.getDouble("d_tax"), rs.getDouble("d_ytd"), 
								rs.getInt("d_next_o_id"), 0, 1);
					}
				}
				break;
			case 29:
				response = voltdbConn.callProcedure("ProcedureSelectCustomer", this.tenantId, para[2], para[3], para[4]);
				if (response.getStatus() != ClientResponse.SUCCESS) {
					System.out.println("select for update response failed");
					return;
				}
				if(response.getResults()[0].asScalarLong() == 0){
					statements[39].setInt(1, (int) para[2]);
					statements[39].setInt(2, (int) para[3]);
					statements[39].setInt(3, (int) para[4]);
					rs = statements[39].executeQuery();
					while(rs.next()){
						voltdbConn.callProcedure("ProcedureInsertCustomer", this.tenantId, rs.getInt("c_id"), rs.getInt("c_d_id"), 
								rs.getInt("c_w_id"), rs.getString("c_first"), rs.getString("c_middle"), rs.getString("c_last"),
								rs.getString("c_street_1"), rs.getString("c_street_2"), rs.getString("c_city"),rs.getString("c_state"),
								rs.getString("c_zip"), rs.getString("c_phone"), rs.getTimestamp("c_since"), 
								rs.getString("c_credit"), rs.getInt("c_credit_lim"), rs.getDouble("c_discount"), rs.getDouble("c_balance"), 
								rs.getDouble("c_ytd_payment"), rs.getInt("c_payment_cnt"), rs.getInt("c_delivery_cnt"), rs.getString("c_data"),
								0, 1);
					}
				}
				break;
			case 30:
				response = voltdbConn.callProcedure("ProcedureSelectCustomer", this.tenantId, para[1], para[2], para[3]);
				if (response.getStatus() != ClientResponse.SUCCESS) {
					System.out.println("select for update response failed");
					return;
				}
				if(response.getResults()[0].asScalarLong() == 0){
					statements[40].setInt(1, (int) para[1]);
					statements[40].setInt(2, (int) para[2]);
					statements[40].setInt(3, (int) para[3]);
					rs = statements[40].executeQuery();
					while(rs.next()){
						voltdbConn.callProcedure("ProcedureInsertCustomer", this.tenantId, rs.getInt("c_id"), rs.getInt("c_d_id"), 
								rs.getInt("c_w_id"), rs.getString("c_first"), rs.getString("c_middle"), rs.getString("c_last"),
								rs.getString("c_street_1"), rs.getString("c_street_2"), rs.getString("c_city"),rs.getString("c_state"),
								rs.getString("c_zip"), rs.getString("c_phone"), rs.getTimestamp("c_since"), 
								rs.getString("c_credit"), rs.getInt("c_credit_lim"), rs.getDouble("c_discount"), rs.getDouble("c_balance"), 
								rs.getDouble("c_ytd_payment"), rs.getInt("c_payment_cnt"), rs.getInt("c_delivery_cnt"), rs.getString("c_data"),
								0, 1);
					}
				}
				break;
			case 31:
				response = voltdbConn.callProcedure("ProcedureSelectOrders", this.tenantId, para[1], para[2], para[3]);
				if (response.getStatus() != ClientResponse.SUCCESS) {
					System.out.println("select for update response failed");
					return;
				}
				if(response.getResults()[0].asScalarLong() == 0){
					statements[41].setInt(1, (int) para[1]);
					statements[41].setInt(2, (int) para[2]);
					statements[41].setInt(3, (int) para[3]);
					rs = statements[41].executeQuery();
					while(rs.next()){
						voltdbConn.callProcedure("ProcedureInsertOrders", this.tenantId, rs.getInt("o_id"), rs.getInt("o_d_id"),
								rs.getInt("o_w_id"), rs.getInt("o_c_id"),
								rs.getTimestamp("o_entry_d"), 
								rs.getInt("o_carrier_id"), rs.getInt("o_ol_cnt"), rs.getInt("o_all_local"), 0, 1);
					}
				}
				break;
			case 32:
				response = voltdbConn.callProcedure("ProcedureSelectOrderLine", this.tenantId, para[1], para[2], para[3]);
				if (response.getStatus() != ClientResponse.SUCCESS) {
					System.out.println("select for update response failed");
					return;
				}
				if(response.getResults()[0].asScalarLong() == 0){
					statements[42].setInt(1, (int) para[1]);
					statements[42].setInt(2, (int) para[2]);
					statements[42].setInt(3, (int) para[3]);
					rs = statements[42].executeQuery();
					while(rs.next()){
						voltdbConn.callProcedure("ProcedureInsertOrderLine", this.tenantId, rs.getInt("ol_o_id"), rs.getInt("ol_d_id"), 
								rs.getInt("ol_w_id"), rs.getInt("ol_number"), rs.getInt("ol_i_id"), rs.getInt("ol_supply_w_id"),
								rs.getTimestamp("ol_delivery_d"), 
								rs.getInt("ol_quantity"), rs.getDouble("ol_amount"), rs.getString("ol_dist_info"), 0, 1);
					}
				}
				break;
			case 33:
				response = voltdbConn.callProcedure("ProcedureSelectCustomer", this.tenantId, para[3], para[2], para[1]);
				if (response.getStatus() != ClientResponse.SUCCESS) {
					System.out.println("select for update response failed");
					return;
				}
				if(response.getResults()[0].asScalarLong() == 0){
					statements[43].setInt(1, (int) para[1]);
					statements[43].setInt(2, (int) para[2]);
					statements[43].setInt(3, (int) para[3]);
					rs = statements[43].executeQuery();
					while(rs.next()){
						voltdbConn.callProcedure("ProcedureInsertCustomer", this.tenantId, rs.getInt("c_id"), rs.getInt("c_d_id"), 
								rs.getInt("c_w_id"), rs.getString("c_first"), rs.getString("c_middle"), rs.getString("c_last"),
								rs.getString("c_street_1"), rs.getString("c_street_2"), rs.getString("c_city"),rs.getString("c_state"),
								rs.getString("c_zip"), rs.getString("c_phone"), rs.getTimestamp("c_since"), 
								rs.getString("c_credit"), rs.getInt("c_credit_lim"), rs.getDouble("c_discount"), rs.getDouble("c_balance"), 
								rs.getDouble("c_ytd_payment"), rs.getInt("c_payment_cnt"), rs.getInt("c_delivery_cnt"), rs.getString("c_data"),
								0, 1);
					}
				}
				break;				
			}
		}catch(IOException | ProcCallException | SQLException e){
			e.printStackTrace();
		}
		
	}
	
	public boolean isUsingVoltdb(){
		return Main.usingVoltdb[tenantId - Main.IDStart];
	}
	
	public boolean isPartiallUsingVoltdb(){
		return Main.partiallyUsingVoltdb[tenantId - Main.IDStart];
	}
	
	public void setPara(int sqlId, Object[] para, int[] paraType, int paraNumber){
		waitList.add(new WaitList(sqlId, para, paraType, paraNumber));
	}
	
	public void sqlPrepare() throws SQLException{
		int id = tenantId;
		statements[0] = conn.prepareStatement("SELETE * FROM customer"+id+" WHERE c_id = ? AND c_w_id = ? AND c_d_id = ?");
		statements[1] = conn.prepareStatement("SELETE * FROM district"+id+" WHERE d_w_id = ? AND d_id = ?");
		statements[2] = conn.prepareStatement("SELETE * FROM item"+id+" WHERE i_id = ?");
		statements[3] = conn.prepareStatement("SELETE * FROM new_orders"+id+" WHERE no_w_id = ? AND no_d_id = ? AND no_o_id = ?");
		statements[4] = conn.prepareStatement("SELETE * FROM order_line"+id+" WHERE ol_w_id = ? AND ol_d_id = ? AND ol_o_id = ? AND ol_number = ?");
		statements[5] = conn.prepareStatement("SELETE * FROM orders"+id+" WHERE o_w_id = ? AND o_d_id = ? AND o_id = ?");
		statements[6] = conn.prepareStatement("SELETE * FROM stock"+id+" WHERE s_w_id = ? AND s_i_id = ?");
		statements[7] = conn.prepareStatement("SELETE * FROM warehouse"+id+" WHERE w_id = ?");
		statements[8] = conn.prepareStatement("SELETE * FROM history"+id+" WHERE h_c_id = ? AND h_c_d_id = ? AND h_c_w_id = ?");
		
		statements[9] = conn.prepareStatement("UPDATE customer"+id+" SET c_id = ?, c_d_id = ?,c_w_id = ?, c_first = ?, c_middle = ?, c_last = ?, c_street_1 = ?, c_street_2 = ?,c_city = ?,"
				+ "c_state = ?,c_zip = ?, c_phone = ?,c_since = ?, c_credit = ?, c_credit_lim = ?, c_discount = ?, c_balance = ?, c_ytd_payment = ?,c_payment_cnt = ?, c_delivery_cnt = ?, c_data = ? "
				+ "WHERE c_id = ? AND c_w_id = ? AND c_d_id = ?");
		statements[10] = conn.prepareStatement("UPDATE district"+id+" SET d_id = ?, d_w_id = ?, d_name = ?, d_street_1 = ?, d_street_2 = ?, d_city = ?, d_state = ?, d_zip = ?, d_tax = ?, d_ytd = ?, d_next_o_id = ? WHERE d_w_id = ? AND d_id = ?");
		statements[11] = conn.prepareStatement("UPDATE item"+id+" SET i_id = ?, i_im_id = ?, i_name = ?, i_price = ?, i_data = ? WHERE i_id = ?");
		statements[12] = conn.prepareStatement("UPDATE new_orders"+id+" SET no_o_id = ?,no_d_id = ?,no_w_id = ? WHERE no_w_id = ? AND no_d_id = ? AND no_o_id = ?");
		statements[13] = conn.prepareStatement("UPDATE order_line"+id+" SET ol_o_id = ?, ol_d_id = ?,ol_w_id = ?,ol_number = ?,ol_i_id = ?, ol_supply_w_id = ?,ol_delivery_d = ?, ol_quantity = ?, ol_amount = ?, ol_dist_info = ? WHERE ol_w_id = ? AND ol_d_id = ? AND ol_o_id = ? AND ol_number = ?");
		statements[14] = conn.prepareStatement("UPDATE orders"+id+" SET o_id = ?, o_d_id = ?, o_w_id = ?,o_c_id = ?,o_entry_d = ?,o_carrier_id = ?,o_ol_cnt = ?, o_all_local = ? WHERE o_w_id = ? AND o_d_id = ? AND o_id = ?");
		statements[15] = conn.prepareStatement("UPDATE stock"+id+" SET s_i_id = ?, s_w_id = ?, s_quantity = ?, s_dist_01 = ?, s_dist_02 = ?,s_dist_03 = ?,s_dist_04 = ?, s_dist_05 = ?, s_dist_06 = ?, s_dist_07 = ?, s_dist_08 = ?, s_dist_09 = ?, s_dist_10 = ?, s_ytd = ?, s_order_cnt = ?, s_remote_cnt = ?,s_data = ? WHERE s_w_id = ? AND s_i_id = ?");
		statements[16] = conn.prepareStatement("UPDATE warehouse"+id+" SET w_id = ?,	w_name = ?,w_street_1 = ?,w_street_2 = ?,w_city = ?,w_state = ?,w_zip = ?,w_tax = ?,	w_ytd = ? WHERE w_id = ?");
		statements[17] = conn.prepareStatement("UPDATE history"+id+" SET h_c_id = ?, h_c_d_id = ?, h_c_w_id = ?,h_d_id = ?,h_w_id = ?,h_date v,h_amount = ?,h_data = ? WHERE h_c_id = ? AND h_c_d_id = ? AND h_c_w_id = ?");
		
		statements[18] = conn.prepareStatement("DELETE FROM customer"+id+" WHERE c_id = ? AND c_w_id = ? AND c_d_id = ?");
		statements[19] = conn.prepareStatement("DELETE FROM district"+id+" WHERE d_w_id = ? AND d_id = ?");
		statements[20] = conn.prepareStatement("DELETE FROM item"+id+" WHERE i_id = ?");
		statements[21] = conn.prepareStatement("DELETE FROM new_orders"+id+" WHERE no_w_id = ? AND no_d_id = ? AND no_o_id = ?");
		statements[22] = conn.prepareStatement("DELETE FROM order_line"+id+" WHERE ol_w_id = ? AND ol_d_id = ? AND ol_o_id = ? AND ol_number = ?");
		statements[23] = conn.prepareStatement("DELETE FROM orders"+id+" WHERE o_w_id = ? AND o_d_id = ? AND o_id = ?");
		statements[24] = conn.prepareStatement("DELETE FROM stock"+id+" WHERE s_w_id = ? AND s_i_id = ?");
		statements[25] = conn.prepareStatement("DELETE FROM warehouse"+id+" WHERE w_id = ?");
		statements[26] = conn.prepareStatement("DELETE FROM history"+id+" WHERE h_c_id = ? AND h_c_d_id = ? AND h_c_w_id = ?");
		
		statements[9] = conn.prepareStatement("INSERT INTO customer"+id+" VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"); //21
		statements[10] = conn.prepareStatement("INSERT INTO district"+id+" VALUES (?,?,?,?,？,?,?,?,?,?,?)"); //11
		statements[11] = conn.prepareStatement("INSERT INTO item"+id+" VALUES (?,?,?,?,?)"); //5
		statements[12] = conn.prepareStatement("INSERT INTO new_orders"+id+" VALUES (?,?,?)"); //3
		statements[13] = conn.prepareStatement("INSERT INTO order_line"+id+" VALUES (?,?,?,?,?,?,?,?,?,?"); //10
		statements[14] = conn.prepareStatement("INSERT INTO orders"+id+" VALUES (?,?,?,?,?,?,?,?)"); //8
		statements[15] = conn.prepareStatement("INSERT INTO stock"+id+" VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"); //17
		statements[16] = conn.prepareStatement("INSERT INTO warehouse"+id+" VALUES (?,?,?,?,?,?,?,?,?)"); //9
		statements[17] = conn.prepareStatement("INSERT INTO history"+id+" VALUES (?,?,?,?,?,?,?,?)"); //8
	}

}
