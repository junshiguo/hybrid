package pooling;

import hybridTest.Main;
import hybridTest.PerformanceMonitor;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;

import utility.DBManager;

public class PConnection extends Thread {
	public static String[] tables = {"CUSTOMER", "DISTRICT", "ITEM", "NEW_ORDERS", "ORDER_LINE", "ORDERS", "STOCK", "WAREHOUSE", "HISTORY"};
	public static String[] querys = {"select", "update", "delete", "insert"};
	
	public int connId;
	public PRequest request;
	public String mysqlURL;
	public String mysqlUsername;
	public String mysqlPassword;
	public String voltdbServer;
	public Connection conn;
	public Statement stmt;
	public Client voltdbConn;

	public PConnection(int connId, String url, String username, String password, String serverlist){
		this.connId = connId;
		this.mysqlURL = url;
		this.mysqlUsername = username;
		this.mysqlPassword = password;
		this.voltdbServer = serverlist;
	}
	
	public boolean connectDB(){
		conn = DBManager.connectDB(mysqlURL, mysqlUsername, mysqlPassword);
		if(conn == null){
			System.out.println("connecting mysql failed...");
			return false;
		}
		try {
			stmt = conn.createStatement();
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
			System.out.println("connecting mysql failed...");
		}
		try {
			stmt = conn.createStatement();
			conn.setAutoCommit(false);
		} catch (SQLException e) {
			e.printStackTrace();
		}
		while(true){
//			synchronized(this){
//				try {
//					this.wait();
//				} catch (InterruptedException e) {
//					e.printStackTrace();
//				}
//			}
			if(PController.checkState(false) == false){
				break;
			}
			if((this.request = PRequest.gsRequest(true, null)) != null){
				doSQL();
			}else{
				try {
					Thread.sleep(100);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}
	}
	
	Object[] paratmp = new Object[30];
	int[] paratmpType = new int[30];
	int paratmpNumber = 0;
	
	public boolean doSQL(){
		boolean success = true;
		
		int tableId = request.sqlId % 9;
		int queryId = request.sqlId / 9;
		int[] state = new int[2];

		if((this.isUsingVoltdb() == false && this.isPartiallUsingVoltdb() == false)){ //only mysql, for mysql test
			if(queryId == 3){
				paratmpNumber = setActualPara(0, true,  0, 0);
				success = doSQLInMysql(tableId, 0, paratmpNumber, paratmp, paratmpType, true);
				if(success){
					paratmpNumber = setActualPara(1, true,  0, 0);
					success = doSQLInMysql(tableId, 1, paratmpNumber, paratmp, paratmpType, false);
				}else{
					paratmpNumber = setActualPara(queryId, true, 0, 0);
					success = doSQLInMysql(tableId, queryId, paratmpNumber, paratmp, paratmpType, false);
				}
			}else{
				paratmpNumber = setActualPara(queryId, true,  0, 0);
				success = doSQLInMysql(tableId, queryId, paratmpNumber, paratmp, paratmpType, false);
			}
		}else if(this.isPartiallUsingVoltdb() == true){ // this tenant partially uses voltdb
			if(queryId == 0 || queryId == 1 || queryId == 3){
				paratmpNumber = setActualPara(0, false,  0, 0);
				success = doSQLInVoltdb(tableId, 0, paratmpNumber, paratmp, true, state);
				if(success){
					if(queryId == 1 || queryId == 3){
						paratmpNumber = setActualPara(1, false, state[0], 1);
						success = doSQLInVoltdb(tableId, 1, paratmpNumber, paratmp, false, state);
					}
				}else{
					paratmpNumber = setActualPara(0, true,  0, 0);
					if(queryId == 0){
						success = doSQLInMysql(tableId, 0, paratmpNumber, paratmp, paratmpType, false);
					}else if (queryId == 3){
						success = doSQLInMysql(tableId, 0, paratmpNumber, paratmp, paratmpType, true);
						if(success == false){
							paratmpNumber = setActualPara(3, false, 1, 0);
							success = doSQLInVoltdb(tableId, 3, paratmpNumber, paratmp, false, state);
						}
					}else if(queryId == 1){
							paratmpNumber = setActualPara(3, false, 0, 1);
							success = doSQLInVoltdb(tableId, 3, paratmpNumber, paratmp, false, state);
					}
				}
			}else{// delete
				paratmpNumber = setActualPara(queryId, false,  0, 0);
				success = doSQLInVoltdb(tableId, queryId, paratmpNumber, paratmp, false, state);
				paratmpNumber = setActualPara(queryId, true, 0, 0);
				success = doSQLInMysql(tableId, queryId, paratmpNumber, paratmp, paratmpType, false);
			}
		}else if(this.isUsingVoltdb() == true && this.isPartiallUsingVoltdb() == false){ // this tenant has all his data in voltdb
			if(queryId == 3 || queryId == 1){ //insert or update
				paratmpNumber = setActualPara(0, false, 0, 0);
				success = doSQLInVoltdb(tableId, 0, paratmpNumber, paratmp, true, state);
				if(success){ 
					if(queryId == 1){
						paratmpNumber = setActualPara(1, false, state[0], 1);
						success = doSQLInVoltdb(tableId, 1, paratmpNumber, paratmp, false, state);
					}
				}else{ 
					if(queryId == 1){
						success = true;
					}
					if(queryId == 3){// still insert
						paratmpNumber = setActualPara(3, false, 1, 0);
						success = doSQLInVoltdb(tableId, 3, paratmpNumber, paratmp, false, state);
					}
				}
			}else{//select or delete
				paratmpNumber = setActualPara(queryId, false, 0, 0);
				success = doSQLInVoltdb(tableId, queryId, paratmpNumber, paratmp, false, state);
				if(queryId == 2){ //delete in mysql
					paratmpNumber = setActualPara(queryId, true, 0, 0);
					success = doSQLInMysql(tableId, queryId, paratmpNumber, paratmp, paratmpType, false);
				}
			}
		}
		if(success && request.sqlId > 8){
			try {
				this.conn.commit();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		long end = System.nanoTime();
		if(success == true){
			Main.timePerQuery.add(new Long(end - request.timeStart));
			Main.actualThroughputPerTenant[request.tenantId - Main.IDStart]++;
			if(request.sqlId <= 8)	PerformanceMonitor.readQuery++;
			else PerformanceMonitor.writeQuery++;
		}
		if(success == false){
			System.out.print("*");
		}
		return success;
	}
	
	public int setActualPara(int queryId, boolean isMysql, int is_insert, int is_update){
		Object[] para = request.para;
		int[] paraType = request.paraType;
		int paraNumber = request.paraNumber;
		int PKNumber = request.PKNumber;
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
				paratmp[PKNumber] = request.tenantId;
				return PKNumber + 1;
			}else if(queryId == 1){
				for(int pi = 0; pi < paraNumber - PKNumber; pi++){
					paratmp[pi] = para[pi];
				}
				paratmp[paraNumber-PKNumber] = request.tenantId;
				paratmp[paraNumber-PKNumber+1] = is_insert;
				paratmp[paraNumber-PKNumber+2] = is_update;
				for(int pk = 0; pk < PKNumber; pk ++){
					paratmp[paraNumber+pk - PKNumber + 3] = para[paraNumber - PKNumber + pk];
				}
				paratmp[paraNumber+3] = request.tenantId;
				return paraNumber + 4;
			}else{
				for(int pi = 0; pi < paraNumber - PKNumber; pi++){
					paratmp[pi] = para[pi];
				}
				paratmp[paraNumber - PKNumber] = request.tenantId;
				paratmp[paraNumber - PKNumber + 1] = is_insert;
				paratmp[paraNumber - PKNumber + 2] = is_update;
				return paraNumber - PKNumber + 3;
			}
		}
	}
	
	public boolean doSQLInMysql(int tableId, int queryId, int paraNumber, Object[] para, int[] paraType, boolean careResult) {
		int sqlId = tableId + queryId * 9;
		int id = request.tenantId;
		try {
			String sql = "";
			switch(sqlId){
			case 0:
				sql = "SELECT * FROM customer"+id+" WHERE c_id = "+para[0]+" AND c_w_id = "+para[1]+" AND c_d_id = "+para[2];
				break;
			case 1:
				sql = "SELECT * FROM district"+id+" WHERE d_w_id = "+para[0]+" AND d_id = "+para[1];
				break;
			case 2:
				sql = "SELECT * FROM item"+id+" WHERE i_id = "+para[0];
				break;
			case 3:
				sql = "SELECT * FROM new_orders"+id+" WHERE no_w_id = "+para[0]+" AND no_d_id = "+para[1]+" AND no_o_id = "+para[2];
				break;
			case 4:
				sql = "SELECT * FROM order_line"+id+" WHERE ol_w_id = "+para[0]+" AND ol_d_id = "+para[1]+" AND ol_o_id = "+para[2]+" AND ol_number = "+para[3];
				break;
			case 5:
				sql = "SELECT * FROM orders"+id+" WHERE o_w_id = "+para[0]+" AND o_d_id = "+para[1]+" AND o_id = "+para[2]+"";
				break;
			case 6:
				sql = "SELECT * FROM stock"+id+" WHERE s_w_id = "+para[0]+" AND s_i_id = "+para[1]+"";
				break;
			case 7:
				sql = "SELECT * FROM warehouse"+id+" WHERE w_id = "+para[0]+"";
				break;
			case 8:
				sql = "SELECT * FROM history"+id+" WHERE h_c_id = "+para[0]+" AND h_c_d_id = "+para[1]+" AND h_c_w_id = "+para[2]+"";
				break;
			case 9:
				sql = "UPDATE customer"+id+" SET c_id = "+para[0]+", c_d_id = "+para[1]+",c_w_id = "+para[2]+", c_first = "+para[3]+", c_middle = "+para[4]+", c_last = "+para[5]+", c_street_1 = "+para[6]+", c_street_2 = "+para[7]+",c_city = "+para[8]+","
						+ "c_state = "+para[9]+",c_zip = "+para[10]+", c_phone = "+para[11]+",c_since = "+para[12]+", c_credit = "+para[13]+", c_credit_lim = "+para[14]+", c_discount = "+para[15]+", c_balance = "+para[16]+", c_ytd_payment = "+para[17]+",c_payment_cnt = "+para[18]+", c_delivery_cnt = "+para[19]+", c_data = "+para[20]+" "
						+ "WHERE c_id = "+para[21]+" AND c_w_id = "+para[22]+" AND c_d_id = "+para[23]+"";
				break;
			case 10:
				sql = "UPDATE district"+id+" SET d_id = "+para[0]+", d_w_id = "+para[1]+", d_name = "+para[2]+", d_street_1 = "+para[3]+", d_street_2 = "+para[4]+", d_city = "+para[5]+", d_state = "+para[6]+", d_zip = "+para[7]+", d_tax = "+para[8]+", d_ytd = "+para[9]+", d_next_o_id = "+para[10]+" WHERE d_w_id = "+para[11]+" AND d_id = "+para[12]+"";
				break;
			case 11:
				sql = "UPDATE item"+id+" SET i_id = "+para[0]+", i_im_id = "+para[1]+", i_name = "+para[2]+", i_price = "+para[3]+", i_data = "+para[4]+" WHERE i_id = "+para[5]+"";
				break;
			case 12:
				sql = "UPDATE new_orders"+id+" SET no_o_id = "+para[0]+",no_d_id = "+para[1]+",no_w_id = "+para[2]+" WHERE no_w_id = "+para[3]+" AND no_d_id = "+para[4]+" AND no_o_id = "+para[5]+"";
				break;
			case 13:
				sql = "UPDATE order_line"+id+" SET ol_o_id = "+para[0]+", ol_d_id = "+para[1]+",ol_w_id = "+para[2]+",ol_number = "+para[3]+",ol_i_id = "+para[4]+", ol_supply_w_id = "+para[5]+",ol_delivery_d = "+para[6]+", ol_quantity = "+para[7]+", ol_amount = "+para[8]+", ol_dist_info = "+para[9]+" WHERE ol_w_id = "+para[10]+" AND ol_d_id = "+para[11]+" AND ol_o_id = "+para[12]+" AND ol_number = "+para[13]+"";
				break;
			case 14:
				sql = "UPDATE orders"+id+" SET o_id = "+para[0]+", o_d_id = "+para[1]+", o_w_id = "+para[2]+",o_c_id = "+para[3]+",o_entry_d = "+para[4]+",o_carrier_id = "+para[5]+",o_ol_cnt = "+para[6]+", o_all_local = "+para[7]+" WHERE o_w_id = "+para[8]+" AND o_d_id = "+para[9]+" AND o_id = "+para[10]+"";
				break;
			case 15:
				sql = "UPDATE stock"+id+" SET s_i_id = "+para[0]+", s_w_id = "+para[1]+", s_quantity = "+para[2]+", s_dist_01 = "+para[3]+", s_dist_02 = "+para[4]+",s_dist_03 = "+para[5]+",s_dist_04 = "+para[6]+", s_dist_05 = "+para[7]+", s_dist_06 = "+para[8]+", s_dist_07 = "+para[9]+", s_dist_08 = "+para[10]+", s_dist_09 = "+para[11]+", s_dist_10 = "+para[12]+", s_ytd = "+para[13]+", s_order_cnt = "+para[14]+", s_remote_cnt = "+para[15]+",s_data = "+para[16]+" WHERE s_w_id = "+para[17]+" AND s_i_id = "+para[18]+"";
				break;
			case 16:
				sql = "UPDATE warehouse"+id+" SET w_id = "+para[0]+",	w_name = "+para[1]+",w_street_1 = "+para[2]+",w_street_2 = "+para[3]+",w_city = "+para[4]+",w_state = "+para[5]+",w_zip = "+para[6]+",w_tax = "+para[7]+",	w_ytd = "+para[8]+" WHERE w_id = "+para[9]+"";
				break;
			case 17:
				sql = "UPDATE history"+id+" SET h_c_id = "+para[0]+", h_c_d_id = "+para[1]+", h_c_w_id = "+para[2]+",h_d_id = "+para[3]+",h_w_id = "+para[4]+",h_date = "+para[5]+",h_amount = "+para[6]+",h_data = "+para[7]+" WHERE h_c_id = "+para[8]+" AND h_c_d_id = "+para[9]+" AND h_c_w_id = "+para[10]+"";
				break;
			case 18:
				sql = "DELETE FROM customer"+id+" WHERE c_id = "+para[0]+" AND c_w_id = "+para[1]+" AND c_d_id = "+para[2]+"";
				break;
			case 19:
				sql = "DELETE FROM district"+id+" WHERE d_w_id = "+para[0]+" AND d_id = "+para[1]+"";
				break;
			case 20:
				sql = "DELETE FROM item"+id+" WHERE i_id = "+para[0]+"";
				break;
			case 21:
				sql = "DELETE FROM new_orders"+id+" WHERE no_w_id = "+para[0]+" AND no_d_id = "+para[1]+" AND no_o_id = "+para[2]+"";
				break;
			case 22:
				sql = "DELETE FROM order_line"+id+" WHERE ol_w_id = "+para[0]+" AND ol_d_id = "+para[1]+" AND ol_o_id = "+para[2]+" AND ol_number = "+para[3]+"";
				break;
			case 23:
				sql = "DELETE FROM orders"+id+" WHERE o_w_id = "+para[0]+" AND o_d_id = "+para[1]+" AND o_id = "+para[2]+"";
				break;
			case 24:
				sql = "DELETE FROM stock"+id+" WHERE s_w_id = "+para[0]+" AND s_i_id = "+para[1]+"";
				break;
			case 25:
				sql = "DELETE FROM warehouse"+id+" WHERE w_id = "+para[0]+"";
				break;
			case 26:
				sql = "DELETE FROM history"+id+" WHERE h_c_id = "+para[0]+" AND h_c_d_id = "+para[1]+" AND h_c_w_id = "+para[2]+"";
				break;
			case 27:
				sql = "INSERT INTO customer"+id+" VALUES ("+para[0]+","+para[1]+","+para[2]+","+para[3]+","+para[4]+","+para[5]+","+para[6]+","+para[7]+","+para[8]+","+para[9]+","+para[10]+","+para[11]+","+para[12]+","+para[13]+","+para[14]+","+para[15]+","+para[16]+","+para[17]+","+para[18]+","+para[19]+","+para[20]+")"; //21
				break;
			case 28:
				sql = "INSERT INTO district"+id+" VALUES ("+para[0]+","+para[1]+","+para[2]+","+para[3]+","+para[4]+","+para[5]+","+para[6]+","+para[7]+","+para[8]+","+para[9]+","+para[10]+")"; //11
				break;
			case 29:
				sql = "INSERT INTO item"+id+" VALUES ("+para[0]+","+para[1]+","+para[2]+","+para[3]+","+para[4]+")"; //5
				break;
			case 30:
				sql = "INSERT INTO new_orders"+id+" VALUES ("+para[0]+","+para[1]+","+para[2]+")"; //3
				break;
			case 31:
				sql = "INSERT INTO order_line"+id+" VALUES ("+para[0]+","+para[1]+","+para[2]+","+para[3]+","+para[4]+","+para[5]+","+para[6]+","+para[7]+","+para[8]+","+para[9]+")"; //10
				break;
			case 32:
				sql = "INSERT INTO orders"+id+" VALUES ("+para[0]+","+para[1]+","+para[2]+","+para[3]+","+para[4]+","+para[5]+","+para[6]+","+para[7]+")"; //8
				break;
			case 33:
				sql = "INSERT INTO stock"+id+" VALUES ("+para[0]+","+para[1]+","+para[2]+","+para[3]+","+para[4]+","+para[5]+","+para[6]+","+para[7]+","+para[8]+","+para[9]+","+para[10]+","+para[11]+","+para[12]+","+para[13]+","+para[14]+","+para[15]+","+para[16]+")"; //17
				break;
			case 34:
				sql = "INSERT INTO warehouse"+id+" VALUES ("+para[0]+","+para[1]+","+para[2]+","+para[3]+","+para[4]+","+para[5]+","+para[6]+","+para[7]+","+para[8]+")"; //9
				break;
			case 35:
				sql = "INSERT INTO history"+id+" VALUES ("+para[0]+","+para[1]+","+para[2]+","+para[3]+","+para[4]+","+para[5]+","+para[6]+","+para[7]+")"; //8
				break;
				default:
			}
			this.stmt.execute(sql);
			if(careResult == true && queryId == 0){
				ResultSet rs = stmt.getResultSet();
				if(rs.next()) return true;
				return false;
			}
			return true;
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}
	}
	
	public boolean doSQLInVoltdb(int tableId, int queryId, int paraNumber, Object[] para, boolean careResult, int[] state) {
		if(this.voltdbConn == null){
			this.voltdbConn = DBManager.connectVoltdb(this.voltdbServer);
		}

		ClientResponse response = null;
		for (int i = 0; i < 2; i++) {
			response = callProc(request.tenantId, paraNumber, tableId, queryId, para);
			if (response != null && response.getStatus() == ClientResponse.SUCCESS) {
				break;
			}
		}
		if (response == null || response.getStatus() != ClientResponse.SUCCESS) {
			System.out.println("response failed");
			return true;
		}
		if (careResult && queryId == 0) {
			VoltTable result = response.getResults()[0];
			if (result.getRowCount() == 0) {
				return false;
			} else {
				state[0] = (int) result.fetchRow(0).get("is_insert", VoltType.INTEGER);
				state[1] = (int) result.fetchRow(0).get("is_update", VoltType.INTEGER);
				return true;
			}
		}
		return true;
	}
	
	public ClientResponse callProc(int threadId, int paraNumber, int tableId, int queryId, Object[] para){
		ClientResponse response = null;
		int volumnId = Main.tenants[threadId - Main.IDStart].idInVoltdb;
		try{
		switch(paraNumber){
		case 1:
			response = this.voltdbConn.callProcedure(tables[tableId]+volumnId+"."+querys[queryId], para[0]);
			break;
		case 2:
			response = this.voltdbConn.callProcedure(tables[tableId]+volumnId+"."+querys[queryId], para[0], para[1]);
			break;
		case 3:
			response = this.voltdbConn.callProcedure(tables[tableId]+volumnId+"."+querys[queryId], para[0], para[1], para[2]);
			break;
		case 4:
			response = this.voltdbConn.callProcedure(tables[tableId]+volumnId+"."+querys[queryId], para[0], para[1], para[2], para[3]);
			break;
		case 5:
			response = this.voltdbConn.callProcedure(tables[tableId]+volumnId+"."+querys[queryId], para[0], para[1], para[2], para[3], para[4]);
			break;
		case 6:
			response = this.voltdbConn.callProcedure(tables[tableId]+volumnId+"."+querys[queryId], para[0], para[1], para[2], para[3], para[4], para[5]);
			break;
		case 7:
			response = this.voltdbConn.callProcedure(tables[tableId]+volumnId+"."+querys[queryId], para[0], para[1], para[2], para[3], para[4], para[5], para[6]);
			break;
		case 8:
			response = this.voltdbConn.callProcedure(tables[tableId]+volumnId+"."+querys[queryId], para[0], para[1], para[2], para[3], para[4], para[5], para[6], para[7]);
			break;
		case 9:
			response = this.voltdbConn.callProcedure(tables[tableId]+volumnId+"."+querys[queryId], para[0], para[1], para[2], para[3], para[4], para[5], para[6], para[7], para[8]);
			break;
		case 10:
			response = this.voltdbConn.callProcedure(tables[tableId]+volumnId+"."+querys[queryId], para[0], para[1], para[2], para[3], para[4], para[5], para[6], para[7], para[8], para[9]);
			break;
		case 11:
			response = this.voltdbConn.callProcedure(tables[tableId]+volumnId+"."+querys[queryId], para[0], para[1], para[2], para[3], para[4], para[5], para[6], para[7], para[8], para[9], para[10]);
			break;
		case 12:
			response = this.voltdbConn.callProcedure(tables[tableId]+volumnId+"."+querys[queryId], para[0], para[1], para[2], para[3], para[4], para[5], para[6], para[7], para[8], para[9], para[10], para[11]);
			break;
		case 13:
			response = this.voltdbConn.callProcedure(tables[tableId]+volumnId+"."+querys[queryId], para[0], para[1], para[2], para[3], para[4], para[5], para[6], para[7], para[8], para[9], para[10], para[11], para[12]);
			break;
		case 14:
			response = this.voltdbConn.callProcedure(tables[tableId]+volumnId+"."+querys[queryId], para[0], para[1], para[2], para[3], para[4], para[5], para[6], para[7], para[8], para[9], para[10], para[11], para[12], para[13]);
			break;
		case 15:
			response = this.voltdbConn.callProcedure(tables[tableId]+volumnId+"."+querys[queryId], para[0], para[1], para[2], para[3], para[4], para[5], para[6], para[7], para[8], para[9], para[10], para[11], para[12], para[13], para[14]);
			break;
		case 16:
			response = this.voltdbConn.callProcedure(tables[tableId]+volumnId+"."+querys[queryId], para[0], para[1], para[2], para[3], para[4], para[5], para[6], para[7], para[8], para[9], para[10], para[11], para[12], para[13], para[14], para[15]);
			break;
		case 17:
			response = this.voltdbConn.callProcedure(tables[tableId]+volumnId+"."+querys[queryId], para[0], para[1], para[2], para[3], para[4], para[5], para[6], para[7], para[8], para[9], para[10], para[11], para[12], para[13], para[14], para[15], para[16]);
			break;
		case 18:
			response = this.voltdbConn.callProcedure(tables[tableId]+volumnId+"."+querys[queryId], para[0], para[1], para[2], para[3], para[4], para[5], para[6], para[7], para[8], para[9], para[10], para[11], para[12], para[13], para[14], para[15], para[16], para[17]);
			break;
		case 19:
			response = this.voltdbConn.callProcedure(tables[tableId]+volumnId+"."+querys[queryId], para[0], para[1], para[2], para[3], para[4], para[5], para[6], para[7], para[8], para[9], para[10], para[11], para[12], para[13], para[14], para[15], para[16], para[17], para[18]);
			break;
		case 20:
			response = this.voltdbConn.callProcedure(tables[tableId]+volumnId+"."+querys[queryId], para[0], para[1], para[2], para[3], para[4], para[5], para[6], para[7], para[8], para[9], para[10], para[11], para[12], para[13], para[14], para[15], para[16], para[17], para[18], para[19]);
			break;
		case 21:
			response = this.voltdbConn.callProcedure(tables[tableId]+volumnId+"."+querys[queryId], para[0], para[1], para[2], para[3], para[4], para[5], para[6], para[7], para[8], para[9], para[10], para[11], para[12], para[13], para[14], para[15], para[16], para[17], para[18], para[19], para[20]);
			break;
		case 22:
			response = this.voltdbConn.callProcedure(tables[tableId]+volumnId+"."+querys[queryId], para[0], para[1], para[2], para[3], para[4], para[5], para[6], para[7], para[8], para[9], para[10], para[11], para[12], para[13], para[14], para[15], para[16], para[17], para[18], para[19], para[20], para[21]);
			break;
		case 23:
			response = this.voltdbConn.callProcedure(tables[tableId]+volumnId+"."+querys[queryId], para[0], para[1], para[2], para[3], para[4], para[5], para[6], para[7], para[8], para[9], para[10], para[11], para[12], para[13], para[14], para[15], para[16], para[17], para[18], para[19], para[20], para[21], para[22]);
			break;
		case 24:
			response = this.voltdbConn.callProcedure(tables[tableId]+volumnId+"."+querys[queryId], para[0], para[1], para[2], para[3], para[4], para[5], para[6], para[7], para[8], para[9], para[10], para[11], para[12], para[13], para[14], para[15], para[16], para[17], para[18], para[19], para[20], para[21], para[22], para[23]);
			break;
		case 25:
			response = this.voltdbConn.callProcedure(tables[tableId]+volumnId+"."+querys[queryId], para[0], para[1], para[2], para[3], para[4], para[5], para[6], para[7], para[8], para[9], para[10], para[11], para[12], para[13], para[14], para[15], para[16], para[17], para[18], para[19], para[20], para[21], para[22], para[23], para[24]);
			break;
		case 26:
			response = this.voltdbConn.callProcedure(tables[tableId]+volumnId+"."+querys[queryId], para[0], para[1], para[2], para[3], para[4], para[5], para[6], para[7], para[8], para[9], para[10], para[11], para[12], para[13], para[14], para[15], para[16], para[17], para[18], para[19], para[20], para[21], para[22], para[23], para[24], para[25]);
			break;
		case 27:
			response = this.voltdbConn.callProcedure(tables[tableId]+volumnId+"."+querys[queryId], para[0], para[1], para[2], para[3], para[4], para[5], para[6], para[7], para[8], para[9], para[10], para[11], para[12], para[13], para[14], para[15], para[16], para[17], para[18], para[19], para[20], para[21], para[22], para[23], para[24], para[25], para[26]);
			break;
		case 28:
			response = this.voltdbConn.callProcedure(tables[tableId]+volumnId+"."+querys[queryId], para[0], para[1], para[2], para[3], para[4], para[5], para[6], para[7], para[8], para[9], para[10], para[11], para[12], para[13], para[14], para[15], para[16], para[17], para[18], para[19], para[20], para[21], para[22], para[23], para[24], para[25], para[26], para[27]);
			break;
			default:
		}
		}catch(Exception e){
		}
		return response;
	}

	public boolean isUsingVoltdb(){
		return Main.usingVoltdb[request.tenantId - Main.IDStart];
	}
	
	public boolean isPartiallUsingVoltdb(){
		return Main.partiallyUsingVoltdb[request.tenantId - Main.IDStart];
	}
	
}
