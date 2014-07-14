package hybridTest;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import org.voltdb.client.Client;

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
	public Statement[] statements;
	public boolean doSQLNow = false;  //set in Tenant. very important
	
	public int sqlId;
	public Object[] para;
	public int[] paraType;
	public int paraNumber;
	
	public HConnection(int connId, int id, String url, String username, String password, String serverlist){
		this.connId = connId;
		this.tenantId = id;
		this.mysqlURL = url;
		this.mysqlUsername = username;
		this.mysqlPassword = password;
		this.voltdbServer = serverlist;
		statements = new Statement[35];
	}

	public void run(){
		conn = DBManager.connectDB(mysqlURL, mysqlUsername, mysqlPassword);
		if(conn == null){
			System.out.println("Tenant "+tenantId+" connecting mysql failed...");
		}
		voltdbConn = DBManager.connectVoltdb(voltdbServer);
		if(voltdbConn == null){
			System.out.println("Tenant "+tenantId+" connectiong voltdb failed...");
		}
		try {
			sqlPrepare();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		while(true){
			if(doSQLNow){
				doSQL(sqlId, para, paraType, paraNumber);
				doSQLNow = false;
			}
		}
	}
	
	public boolean doSQL(int sqlId, Object[] para, int[] paraType, int paraNumber){
		
		return false;
	}
	
	public void setPara(int sqlId, Object[] para, int[] paraType, int paraNumber){
		this.sqlId = sqlId;
		this.para = para;
		this.paraType = paraType;
		this.paraNumber = paraNumber;
	}
	
	public void sqlPrepare() throws SQLException{
		int id = tenantId;
		statements[0] = conn.prepareStatement("SELECT d_next_o_id, d_tax FROM district"+id+" WHERE d_id = ? AND d_w_id = ?");
		statements[1] = conn.prepareStatement("SELECT i_price, i_name, i_data FROM item"+id+" WHERE i_id = ?");
		statements[2] = conn.prepareStatement("SELECT s_quantity, s_data, s_dist_01, s_dist_02, s_dist_03, s_dist_04, " +
				"s_dist_05, s_dist_06, s_dist_07, s_dist_08, s_dist_09, s_dist_10 FROM stock"+id+" WHERE s_i_id = ? AND s_w_id = ?");
		statements[3] = conn.prepareStatement("SELECT w_street_1, w_street_2, w_city, w_state, w_zip, w_name FROM warehouse"+id+" WHERE w_id = ?");
		statements[4] = conn.prepareStatement("SELECT d_street_1, d_street_2, d_city, d_state, d_zip, d_name FROM district"+id+" WHERE d_w_id = ? AND d_id = ?");
		statements[5] = conn.prepareStatement("SELECT count(c_id) FROM customer"+id+" WHERE c_w_id = ? AND c_d_id = ? AND c_last = ?");
		statements[6] = conn.prepareStatement("SELECT c_id FROM customer"+id+" WHERE c_w_id = ? AND c_d_id = ? AND c_last = ? ORDER BY c_first");
		statements[7] = conn.prepareStatement("SELECT c_first, c_middle, c_last, c_street_1, c_street_2, c_city, c_state, c_zip, c_phone, c_credit, c_credit_lim, c_discount, c_balance, c_since FROM customer"+id+" WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?");
		statements[8] = conn.prepareStatement("SELECT c_data FROM customer"+id+" WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?");
		statements[9] = conn.prepareStatement("SELECT count(c_id) FROM customer"+id+" WHERE c_w_id = ? AND c_d_id = ? AND c_last = ?");
		statements[10] = conn.prepareStatement("SELECT c_balance, c_first, c_middle, c_last FROM customer"+id+" WHERE c_w_id = ? AND c_d_id = ? AND c_last = ? ORDER BY c_first");
		statements[11] = conn.prepareStatement("SELECT c_balance, c_first, c_middle, c_last FROM customer"+id+" WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?");
		statements[12] = conn.prepareStatement("SELECT ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, ol_delivery_d FROM order_line"+id+" WHERE ol_w_id = ? AND ol_d_id = ? AND ol_o_id = ?");
		statements[13] = conn.prepareStatement("SELECT COALESCE(MIN(no_o_id),0) FROM new_orders"+id+" WHERE no_d_id = ? AND no_w_id = ?");
		statements[14] = conn.prepareStatement("SELECT o_c_id FROM orders"+id+" WHERE o_id = ? AND o_d_id = ? AND o_w_id = ?");
		statements[15] = conn.prepareStatement("SELECT SUM(ol_amount) FROM order_line"+id+" WHERE ol_o_id = ? AND ol_d_id = ? AND ol_w_id = ?");
		statements[16] = conn.prepareStatement("SELECT d_next_o_id FROM district"+id+" WHERE d_id = ? AND d_w_id = ?");
		statements[17] = conn.prepareStatement("SELECT DISTINCT ol_i_id FROM order_line"+id+" WHERE ol_w_id = ? AND ol_d_id = ? AND ol_o_id < ? AND ol_o_id >= (? - 20)");
		statements[18] = conn.prepareStatement("SELECT count(*) FROM stock"+id+" WHERE s_w_id = ? AND s_i_id = ? AND s_quantity < ?");
		
		statements[19] = conn.prepareStatement("SELECT c_discount, c_last, c_credit, w_tax FROM customer"+id+", warehouse"+id+" WHERE w_id = ? AND c_w_id = w_id AND c_d_id = ? AND c_id = ?");
		statements[20] = conn.prepareStatement("SELECT o_id, o_entry_d, COALESCE(o_carrier_id,0) FROM orders"+id+" WHERE o_w_id = ? AND o_d_id = ? AND o_c_id = ? AND o_id = (SELECT MAX(o_id) FROM orders"+id+" WHERE o_w_id = ? AND o_d_id = ? AND o_c_id = ?)");
		//read 21(0~20). write 14(21~34)
		statements[21] = conn.prepareStatement("INSERT INTO orders"+id+" (o_id, o_d_id, o_w_id, o_c_id, o_entry_d, o_ol_cnt, o_all_local) VALUES(?, ?, ?, ?, ?, ?, ?)");//8 o_carrier_id
		statements[22] = conn.prepareStatement("INSERT INTO new_orders"+id+" (no_o_id, no_d_id, no_w_id) VALUES (?,?,?)");
		statements[23] = conn.prepareStatement("INSERT INTO order_line"+id+" (ol_o_id, ol_d_id, ol_w_id, ol_number, ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, ol_dist_info) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)");//10 ol_delivery_d
		statements[24] = conn.prepareStatement("INSERT INTO history"+id+"(h_c_d_id, h_c_w_id, h_c_id, h_d_id, h_w_id, h_date, h_amount, h_data) VALUES(?, ?, ?, ?, ?, ?, ?, ?)");
		
		statements[25] = conn.prepareStatement("UPDATE district"+id+" SET d_next_o_id = d_next_o_id + 1 WHERE d_id = ? AND d_w_id = ?");
		statements[26] = conn.prepareStatement("UPDATE stock"+id+" SET s_quantity = ? WHERE s_i_id = ? AND s_w_id = ?"); //
		statements[27] = conn.prepareStatement("UPDATE warehouse"+id+" SET w_ytd = w_ytd + ? WHERE w_id = ?");//
		statements[28] = conn.prepareStatement("UPDATE district"+id+" SET d_ytd = d_ytd + ? WHERE d_w_id = ? AND d_id = ?");
		statements[29] = conn.prepareStatement("UPDATE customer"+id+" SET c_balance = ?, c_data = ? WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?");
		statements[30] = conn.prepareStatement("UPDATE customer"+id+" SET c_balance = ? WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?");
		statements[31] = conn.prepareStatement("UPDATE orders"+id+" SET o_carrier_id = ? WHERE o_id = ? AND o_d_id = ? AND o_w_id = ?");//
		statements[32] = conn.prepareStatement("UPDATE order_line"+id+" SET ol_delivery_d = ? WHERE ol_o_id = ? AND ol_d_id = ? AND ol_w_id = ?");
		statements[33] = conn.prepareStatement("UPDATE customer"+id+" SET c_balance = c_balance + ? , c_delivery_cnt = c_delivery_cnt + 1 WHERE c_id = ? AND c_d_id = ? AND c_w_id = ?");//
		
		statements[34] = conn.prepareStatement("DELETE FROM new_orders"+id+" WHERE no_o_id = ? AND no_d_id = ? AND no_w_id = ?");
	}

}
