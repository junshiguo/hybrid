package hybridTest;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Vector;

import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.NoConnectionsException;
import org.voltdb.client.ProcCallException;

import utility.DBManager;

public class HConnection extends Thread {
	public static String[] tables = {"CUSTOMER", "DISTRICT", "ITEM", "NEW_ORDERS", "ORDER_LINE", "ORDERS", "STOCK", "WAREHOUSE", "HISTORY"};
	public static String[] querys = {"select", "update", "delete", "insert"};
	
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
		public int PKNumber;
		public WaitList(int sqlId, Object[] para, int[] paraType, int paraNumber, int PKNumber){
			this.sqlId = sqlId;
			this.para = para;
			this.paraType = paraType;
			this.paraNumber = paraNumber;
			this.PKNumber = PKNumber;
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
	
	public boolean connectDB(){
		conn = DBManager.connectDB(mysqlURL, mysqlUsername, mysqlPassword);
		if(conn == null){
			System.out.println("Tenant "+tenantId+" connecting mysql failed...");
			return false;
		}
		try {
			sqlPrepare();
			conn.setAutoCommit(false);
		} catch (SQLException e) {
			e.printStackTrace();
			return false;
		}
		return true;
	}

	public void run(){
		conn = DBManager.connectDB(mysqlURL, mysqlUsername, mysqlPassword);
		if(conn == null){
			System.out.println("Tenant "+tenantId+" connecting mysql failed...");
		}
		try {
			sqlPrepare();
			conn.setAutoCommit(false);
		} catch (SQLException e) {
			e.printStackTrace();
		}
		while(true){
			synchronized(this){
				try {
					this.wait();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
			if(doSQLNow > 0 && waitList.isEmpty() == false){
				WaitList tmp = waitList.firstElement();
				waitList.remove(0);
				doSQL(tmp.sqlId, tmp.para, tmp.paraType, tmp.paraNumber, tmp.PKNumber);
				this.doSQLNow --;
			}
		}
	}
	
	Object[] paratmp = new Object[30];
	int[] paratmpType = new int[30];
	int paratmpNumber = 0;
	
	public boolean doSQL(int sqlId, Object[] para, int[] paraType, int paraNumber, int PKNumber){
		boolean success = false;
		
		long start = System.nanoTime();
		int tableId = sqlId % 9;
		int queryId = sqlId / 9;
		int[] state = new int[2];
		
		if(Main.onlyMysql == true || (this.isUsingVoltdb() == false && this.isPartiallUsingVoltdb() == false)){ //only mysql, for mysql test
//			if(queryId == 3){//insert, for primary key constraint
//				paratmpNumber = setActualPara(0, true, para, paraType, paraNumber, PKNumber, 0, 0);
//				success = doSQLInMysql(tableId, 0, paratmpNumber, paratmp, paratmpType, true);
//				if(success == true){
//					paratmpNumber = setActualPara(1, true, para, paraType, paraNumber, PKNumber, 0, 0);
//					success = doSQLInMysql(tableId, 1, paratmpNumber, paratmp, paratmpType, false);
//					return success;
//				}
//			}
			paratmpNumber = setActualPara(queryId, true, para, paraType, paraNumber, PKNumber, 0, 0);
			success = doSQLInMysql(tableId, queryId, paratmpNumber, paratmp, paratmpType, false);
		}else if(this.isPartiallUsingVoltdb() == true){ // this tenant partially uses voltdb
			if(queryId == 0 || queryId == 1 || queryId == 3){
				paratmpNumber = setActualPara(0, false, para, paraType, paraNumber, PKNumber, 0, 0);
				success = doSQLInVoltdb(tableId, 0, paratmpNumber, paratmp, true, state);
				if(success){
					if(queryId == 1 || queryId == 3){
						paratmpNumber = setActualPara(1, false, para, paraType, paraNumber, PKNumber, state[0], 1);
						success = doSQLInVoltdb(tableId, 1, paratmpNumber, paratmp, false, state);
					}
				}else{
					paratmpNumber = setActualPara(0, true, para, paraType, paraNumber, PKNumber, 0, 0);
					if(queryId == 0){
						success = doSQLInMysql(tableId, 0, paratmpNumber, paratmp, paraType, false);
					}else{
						success = doSQLInMysql(tableId, 0, paratmpNumber, paratmp, paraType, true);
						if(success){
							state[0] = 0; state[1] = 1;
							success = doSQLInVoltdb(tableId, 3, paratmpNumber, paratmp, false, state);
						}else if(queryId == 3){
							state[0] = 1; state[1] = 0;
							success = doSQLInVoltdb(tableId, 3, paratmpNumber, paratmp, false, state);
						}
					}
				}
			}else{// delete
				paratmpNumber = setActualPara(queryId, false, para, paraType, paraNumber, PKNumber, 0, 0);
				success = doSQLInVoltdb(tableId, queryId, paratmpNumber, paratmp, false, state);
				paratmpNumber = setActualPara(queryId, true, para, paraType, paraNumber, PKNumber, 0, 0);
				success = doSQLInMysql(tableId, queryId, paratmpNumber, paratmp, paratmpType, false);
			}
//			success = doSQLInVoltdb(tableId, queryId, paratmpNumber, paratmp, true, state);
		}else if(this.isUsingVoltdb() == true && this.isPartiallUsingVoltdb() == false){ // this tenant has all his data in voltdb
			if(queryId == 3 || queryId == 1){ //insert or update
				paratmpNumber = setActualPara(0, false, para, paraType, paraNumber, PKNumber, 0, 0);
				success = doSQLInVoltdb(tableId, 0, paratmpNumber, paratmp, true, state);
				if(success){ //change to update
					paratmpNumber = setActualPara(1, false, para, paraType, paraNumber, PKNumber, state[0], 1);
					success = doSQLInVoltdb(tableId, 1, paratmpNumber, paratmp, false, state);
				}else{ 
					if(queryId == 3){// still insert
						paratmpNumber = setActualPara(3, false, para, paraType, paraNumber, PKNumber, 1, 0);
						success = doSQLInVoltdb(tableId, 3, paratmpNumber, paratmp, false, state);
					}
				}
			}else{//select or delete
				paratmpNumber = setActualPara(queryId, false, para, paraType, paraNumber, PKNumber, 0, 0);
				success = doSQLInVoltdb(tableId, queryId, paratmpNumber, paratmp, false, state);
				if(queryId == 2){ //delete in mysql
					paratmpNumber = setActualPara(queryId, true, para, paraType, paraNumber, PKNumber, 0, 0);
					success = doSQLInMysql(tableId, queryId, paratmpNumber, paratmp, paratmpType, false);
				}
			}
		}
		if(success && sqlId > 8){
			try {
				this.conn.commit();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		long end = System.nanoTime();
		if(success == true && Main.isActive == true){
			Main.timePerQuery.add(new Long(end - start));
			Main.actualThroughputPerTenant[this.tenantId - Main.IDStart]++;
			if(sqlId <= 8)	PerformanceMonitor.readQuery++;
			else PerformanceMonitor.writeQuery++;
		}
		return success;
	}
	
	public int setActualPara(int queryId, boolean isMysql, Object[] para, int[] paraType, int paraNumber, int PKNumber, int is_insert, int is_update){
		if(isMysql){
			if(queryId == 0 || queryId == 2){
				for(int pk = 0; pk < PKNumber; pk ++){
					paratmp[pk] = para[paraNumber - PKNumber + pk];
					paratmpType[pk] = paraType[paraNumber - PKNumber + pk];
				}
				return PKNumber;
			}else if(queryId == 1){
				for(int pi = 0; pi < paraNumber; pi++){
					paratmp[pi] = para[pi];
					paratmpType[pi] = paraType[pi];
				}
				return paraNumber;
			}else{
				for(int pi = 0; pi < paraNumber - PKNumber; pi++){
					paratmp[pi] = para[pi];
					paratmpType[pi] = paraType[pi];
				}
				return paraNumber - PKNumber;
			}
		}else{ //voltdb
			if(queryId == 0 || queryId == 2){
				for(int pk = 0; pk < PKNumber; pk ++){
					paratmp[pk] = para[paraNumber - PKNumber + pk];
				}
				paratmp[PKNumber] = this.tenantId;
				return PKNumber + 1;
			}else if(queryId == 1){
				for(int pi = 0; pi < paraNumber - PKNumber; pi++){
					paratmp[pi] = para[pi];
				}
				paratmp[paraNumber-PKNumber] = this.tenantId;
				paratmp[paraNumber-PKNumber+1] = is_insert;
				paratmp[paraNumber-PKNumber+2] = is_update;
				for(int pk = 0; pk < PKNumber; pk ++){
					paratmp[paraNumber+pk - PKNumber + 3] = para[paraNumber - PKNumber + pk];
				}
				paratmp[paraNumber+3] = this.tenantId;
				return paraNumber + 4;
			}else{
				for(int pi = 0; pi < paraNumber - PKNumber; pi++){
					paratmp[pi] = para[pi];
				}
				paratmp[paraNumber - PKNumber] = this.tenantId;
				paratmp[paraNumber - PKNumber + 1] = is_insert;
				paratmp[paraNumber - PKNumber + 2] = is_update;
				return paraNumber - PKNumber + 3;
			}
		}
	}
	
	public boolean doSQLInMysql(int tableId, int queryId, int paraNumber, Object[] para, int[] paraType, boolean careResult) {
		int i = 0;
		int sqlId = tableId + queryId * 9;
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
//			System.out.println(statements[sqlId].toString());
			statements[sqlId].execute();
			if(careResult == true && queryId == 0){
				ResultSet rs = statements[sqlId].getResultSet();
				if(rs.next()) return true;
				else return false;
			}
			return true;
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println("thread id: "+this.tenantId +"; sql id: "+sqlId+"; table id: "+tableId+"; query id: "+queryId+"; i= "+i+" : exception??");
			System.out.println(para[i]);
			return false;
		}
	}
	
	public boolean doSQLInVoltdb(int tableId, int queryId, int paraNumber, Object[] para, boolean careResult, int[] state) {
		if(this.voltdbConn == null){
			this.voltdbConn = DBManager.connectVoltdb(this.voltdbServer);
		}
		try {
			ClientResponse response = null;
			response = callProc(this.tenantId, paraNumber, tableId, queryId, para);
			if(response.getStatus() != ClientResponse.SUCCESS){
				System.out.println("response failed");
				return false;
			}
			if(careResult && queryId == 0){
				VoltTable result = response.getResults()[0];
				if(result.getRowCount() == 0){
					return false;
				}else{
					state[0] = (int) result.get("is_insert", VoltType.INTEGER);
					state[1] = (int) result.get("is_update", VoltType.INTEGER);
					return true;
				}
			}
			return true;
		} catch (IOException | ProcCallException e) {
			e.printStackTrace();
			return false;
		}
	}
	
	public ClientResponse callProc(int threadId, int paraNumber, int tableId, int queryId, Object[] para) throws NoConnectionsException, IOException, ProcCallException{
		ClientResponse response = null;
		switch(paraNumber){
		case 1:
			response = this.voltdbConn.callProcedure(tables[tableId]+threadId+"."+querys[queryId], para[0]);
			break;
		case 2:
			response = this.voltdbConn.callProcedure(tables[tableId]+threadId+"."+querys[queryId], para[0], para[1]);
			break;
		case 3:
			response = this.voltdbConn.callProcedure(tables[tableId]+threadId+"."+querys[queryId], para[0], para[1], para[2]);
			break;
		case 4:
			response = this.voltdbConn.callProcedure(tables[tableId]+threadId+"."+querys[queryId], para[0], para[1], para[2], para[3]);
			break;
		case 5:
			response = this.voltdbConn.callProcedure(tables[tableId]+threadId+"."+querys[queryId], para[0], para[1], para[2], para[3], para[4]);
			break;
		case 6:
			response = this.voltdbConn.callProcedure(tables[tableId]+threadId+"."+querys[queryId], para[0], para[1], para[2], para[3], para[4], para[5]);
			break;
		case 7:
			response = this.voltdbConn.callProcedure(tables[tableId]+threadId+"."+querys[queryId], para[0], para[1], para[2], para[3], para[4], para[5], para[6]);
			break;
		case 8:
			response = this.voltdbConn.callProcedure(tables[tableId]+threadId+"."+querys[queryId], para[0], para[1], para[2], para[3], para[4], para[5], para[6], para[7]);
			break;
		case 9:
			response = this.voltdbConn.callProcedure(tables[tableId]+threadId+"."+querys[queryId], para[0], para[1], para[2], para[3], para[4], para[5], para[6], para[7], para[8]);
			break;
		case 10:
			response = this.voltdbConn.callProcedure(tables[tableId]+threadId+"."+querys[queryId], para[0], para[1], para[2], para[3], para[4], para[5], para[6], para[7], para[8], para[9]);
			break;
		case 11:
			response = this.voltdbConn.callProcedure(tables[tableId]+threadId+"."+querys[queryId], para[0], para[1], para[2], para[3], para[4], para[5], para[6], para[7], para[8], para[9], para[10]);
			break;
		case 12:
			response = this.voltdbConn.callProcedure(tables[tableId]+threadId+"."+querys[queryId], para[0], para[1], para[2], para[3], para[4], para[5], para[6], para[7], para[8], para[9], para[10], para[11]);
			break;
		case 13:
			response = this.voltdbConn.callProcedure(tables[tableId]+threadId+"."+querys[queryId], para[0], para[1], para[2], para[3], para[4], para[5], para[6], para[7], para[8], para[9], para[10], para[11], para[12]);
			break;
		case 14:
			response = this.voltdbConn.callProcedure(tables[tableId]+threadId+"."+querys[queryId], para[0], para[1], para[2], para[3], para[4], para[5], para[6], para[7], para[8], para[9], para[10], para[11], para[12], para[13]);
			break;
		case 15:
			response = this.voltdbConn.callProcedure(tables[tableId]+threadId+"."+querys[queryId], para[0], para[1], para[2], para[3], para[4], para[5], para[6], para[7], para[8], para[9], para[10], para[11], para[12], para[13], para[14]);
			break;
		case 16:
			response = this.voltdbConn.callProcedure(tables[tableId]+threadId+"."+querys[queryId], para[0], para[1], para[2], para[3], para[4], para[5], para[6], para[7], para[8], para[9], para[10], para[11], para[12], para[13], para[14], para[15]);
			break;
		case 19:
			response = this.voltdbConn.callProcedure(tables[tableId]+threadId+"."+querys[queryId], para[0], para[1], para[2], para[3], para[4], para[5], para[6], para[7], para[8], para[9], para[10], para[11], para[12], para[13], para[14], para[15], para[16], para[17], para[18]);
			break;
		case 21:
			response = this.voltdbConn.callProcedure(tables[tableId]+threadId+"."+querys[queryId], para[0], para[1], para[2], para[3], para[4], para[5], para[6], para[7], para[8], para[9], para[10], para[11], para[12], para[13], para[14], para[15], para[16], para[17], para[18], para[19], para[20]);
			break;
		case 23:
			response = this.voltdbConn.callProcedure(tables[tableId]+threadId+"."+querys[queryId], para[0], para[1], para[2], para[3], para[4], para[5], para[6], para[7], para[8], para[9], para[10], para[11], para[12], para[13], para[14], para[15], para[16], para[17], para[18], para[19], para[20], para[21], para[22]);
			break;
		case 26:
			response = this.voltdbConn.callProcedure(tables[tableId]+threadId+"."+querys[queryId], para[0], para[1], para[2], para[3], para[4], para[5], para[6], para[7], para[8], para[9], para[10], para[11], para[12], para[13], para[14], para[15], para[16], para[17], para[18], para[19], para[20], para[21], para[22], para[23], para[24], para[25]);
			break;
			default:
		}
		return response;
	}

	public boolean isUsingVoltdb(){
		return Main.usingVoltdb[tenantId - Main.IDStart];
	}
	
	public boolean isPartiallUsingVoltdb(){
		return Main.partiallyUsingVoltdb[tenantId - Main.IDStart];
	}
	
	public void setPara(int sqlId, Object[] para, int[] paraType, int paraNumber, int PKNumber){
		waitList.add(new WaitList(sqlId, para, paraType, paraNumber, PKNumber));
	}
	
	public void sqlPrepare() throws SQLException{
		int id = tenantId;
		statements[0] = conn.prepareStatement("SELECT * FROM customer"+id+" WHERE c_id = ? AND c_w_id = ? AND c_d_id = ?");
		statements[1] = conn.prepareStatement("SELECT * FROM district"+id+" WHERE d_w_id = ? AND d_id = ?");
		statements[2] = conn.prepareStatement("SELECT * FROM item"+id+" WHERE i_id = ?");
		statements[3] = conn.prepareStatement("SELECT * FROM new_orders"+id+" WHERE no_w_id = ? AND no_d_id = ? AND no_o_id = ?");
		statements[4] = conn.prepareStatement("SELECT * FROM order_line"+id+" WHERE ol_w_id = ? AND ol_d_id = ? AND ol_o_id = ? AND ol_number = ?");
		statements[5] = conn.prepareStatement("SELECT * FROM orders"+id+" WHERE o_w_id = ? AND o_d_id = ? AND o_id = ?");
		statements[6] = conn.prepareStatement("SELECT * FROM stock"+id+" WHERE s_w_id = ? AND s_i_id = ?");
		statements[7] = conn.prepareStatement("SELECT * FROM warehouse"+id+" WHERE w_id = ?");
		statements[8] = conn.prepareStatement("SELECT * FROM history"+id+" WHERE h_c_id = ? AND h_c_d_id = ? AND h_c_w_id = ?");
		
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
		statements[17] = conn.prepareStatement("UPDATE history"+id+" SET h_c_id = ?, h_c_d_id = ?, h_c_w_id = ?,h_d_id = ?,h_w_id = ?,h_date = ?,h_amount = ?,h_data = ? WHERE h_c_id = ? AND h_c_d_id = ? AND h_c_w_id = ?");
		
		statements[18] = conn.prepareStatement("DELETE FROM customer"+id+" WHERE c_id = ? AND c_w_id = ? AND c_d_id = ?");
		statements[19] = conn.prepareStatement("DELETE FROM district"+id+" WHERE d_w_id = ? AND d_id = ?");
		statements[20] = conn.prepareStatement("DELETE FROM item"+id+" WHERE i_id = ?");
		statements[21] = conn.prepareStatement("DELETE FROM new_orders"+id+" WHERE no_w_id = ? AND no_d_id = ? AND no_o_id = ?");
		statements[22] = conn.prepareStatement("DELETE FROM order_line"+id+" WHERE ol_w_id = ? AND ol_d_id = ? AND ol_o_id = ? AND ol_number = ?");
		statements[23] = conn.prepareStatement("DELETE FROM orders"+id+" WHERE o_w_id = ? AND o_d_id = ? AND o_id = ?");
		statements[24] = conn.prepareStatement("DELETE FROM stock"+id+" WHERE s_w_id = ? AND s_i_id = ?");
		statements[25] = conn.prepareStatement("DELETE FROM warehouse"+id+" WHERE w_id = ?");
		statements[26] = conn.prepareStatement("DELETE FROM history"+id+" WHERE h_c_id = ? AND h_c_d_id = ? AND h_c_w_id = ?");
		
		statements[27] = conn.prepareStatement("INSERT INTO customer"+id+" VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"); //21
		statements[28] = conn.prepareStatement("INSERT INTO district"+id+" VALUES (?,?,?,?,?,?,?,?,?,?,?)"); //11
		statements[29] = conn.prepareStatement("INSERT INTO item"+id+" VALUES (?,?,?,?,?)"); //5
		statements[30] = conn.prepareStatement("INSERT INTO new_orders"+id+" VALUES (?,?,?)"); //3
		statements[31] = conn.prepareStatement("INSERT INTO order_line"+id+" VALUES (?,?,?,?,?,?,?,?,?,?)"); //10
		statements[32] = conn.prepareStatement("INSERT INTO orders"+id+" VALUES (?,?,?,?,?,?,?,?)"); //8
		statements[33] = conn.prepareStatement("INSERT INTO stock"+id+" VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"); //17
		statements[34] = conn.prepareStatement("INSERT INTO warehouse"+id+" VALUES (?,?,?,?,?,?,?,?,?)"); //9
		statements[35] = conn.prepareStatement("INSERT INTO history"+id+" VALUES (?,?,?,?,?,?,?,?)"); //8
	}

}
