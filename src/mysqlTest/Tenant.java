package mysqlTest;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import utility.DBManager;
import utility.Sequence;


public class Tenant extends Thread {
	public static PreparedStatement[][] statements;
	public static Tenant[] tenants;
	public static void init(int numberOfConnection,double writePercent, String dbURL, String dbUserName, String dbPassword, boolean copyTable){
		Tenant.statements = new PreparedStatement[numberOfConnection][35];
		tenants = new Tenant[numberOfConnection];
		int write = (int)(writePercent*100);
		Connection conn = DBManager.connectDB(Main.dbURL, Main.dbUserName, Main.dbPassword);
		for(int i=0; i<numberOfConnection; i++){
			tenants[i] = new Tenant(i, dbURL, dbUserName, dbPassword, 100-write, write, copyTable, conn);
		}
	}
	
	public static void main(String[] args){
		//new Tenant(1, )
	}
	
	public int id;
	public String dbURL;
	public String dbUserName;
	public String dbPassword;
	public Connection conn;
	public Sequence sequence;
	public boolean isLoaded = false;
	public Tenant(int id, String dbURL, String dbUserName, String dbPassword, int r, int w, boolean copyTable, Connection conn){
		this.id = id;
		this.dbURL = dbURL;
		this.dbUserName = dbUserName;
		this.dbPassword = dbPassword;
		sequence = new Sequence();
		sequence.initSequence(r, w);
		if(copyTable)
			DBManager.copyTables(id, conn);
	}
	
	public void run(){
		conn = DBManager.connectDB(dbURL,dbUserName,dbPassword);
		if(conn == null) {
			System.out.println("DB connection problem...");
			return;
		}
		System.out.println("thread "+id+": db connected...");
		try {
			sqlPrepare0();
			System.out.println("thread "+id+": sql prepared...");
		} catch (SQLException e) {
			e.printStackTrace();
			System.out.println("error in preparing sql!");
			return;
		}
//		try {
//			conn.setAutoCommit(false);
//		} catch (SQLException e) {
//			e.printStackTrace();
//			System.out.println("Error in set autoCommit!");
//			return;
//		}
		new Driver(id);
//		try {
//			conn.commit();
//		} catch (SQLException e) {
//			e.printStackTrace();
//			System.out.println("Error in commiting data!");
//			return;
//		}
		try {
			conn.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	public void sqlPrepare() throws SQLException{
		statements[id][0] = conn.prepareStatement("SELECT d_next_o_id, d_tax FROM district WHERE d_id = ? AND d_w_id = ?");
		statements[id][1] = conn.prepareStatement("SELECT i_price, i_name, i_data FROM item WHERE i_id = ?");
		statements[id][2] = conn.prepareStatement("SELECT s_quantity, s_data, s_dist_01, s_dist_02, s_dist_03, s_dist_04, " +
				"s_dist_05, s_dist_06, s_dist_07, s_dist_08, s_dist_09, s_dist_10 FROM stock WHERE s_i_id = ? AND s_w_id = ?");
		statements[id][3] = conn.prepareStatement("SELECT w_street_1, w_street_2, w_city, w_state, w_zip, w_name FROM warehouse WHERE w_id = ?");
		statements[id][4] = conn.prepareStatement("SELECT d_street_1, d_street_2, d_city, d_state, d_zip, d_name FROM district WHERE d_w_id = ? AND d_id = ?");
		statements[id][5] = conn.prepareStatement("SELECT count(c_id) FROM customer WHERE c_w_id = ? AND c_d_id = ? AND c_last = ?"); //not only id in where
		statements[id][6] = conn.prepareStatement("SELECT c_id FROM customer WHERE c_w_id = ? AND c_d_id = ? AND c_last = ? ORDER BY c_first"); //not only id in where
		statements[id][7] = conn.prepareStatement("SELECT c_first, c_middle, c_last, c_street_1, c_street_2, c_city, c_state, c_zip, c_phone, c_credit, c_credit_lim, c_discount, c_balance, c_since FROM customer WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?");
		statements[id][8] = conn.prepareStatement("SELECT c_data FROM customer WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?");
		statements[id][9] = conn.prepareStatement("SELECT count(c_id) FROM customer WHERE c_w_id = ? AND c_d_id = ? AND c_last = ?"); //not only id in where
		statements[id][10] = conn.prepareStatement("SELECT c_balance, c_first, c_middle, c_last FROM customer WHERE c_w_id = ? AND c_d_id = ? AND c_last = ? ORDER BY c_first"); //not only id in where
		statements[id][11] = conn.prepareStatement("SELECT c_balance, c_first, c_middle, c_last FROM customer WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?");
		statements[id][12] = conn.prepareStatement("SELECT ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, ol_delivery_d FROM order_line WHERE ol_w_id = ? AND ol_d_id = ? AND ol_o_id = ?");
		statements[id][13] = conn.prepareStatement("SELECT COALESCE(MIN(no_o_id),0) FROM new_orders WHERE no_d_id = ? AND no_w_id = ?");
		statements[id][14] = conn.prepareStatement("SELECT o_c_id FROM orders WHERE o_id = ? AND o_d_id = ? AND o_w_id = ?");
		statements[id][15] = conn.prepareStatement("SELECT SUM(ol_amount) FROM order_line WHERE ol_o_id = ? AND ol_d_id = ? AND ol_w_id = ?");
		statements[id][16] = conn.prepareStatement("SELECT d_next_o_id FROM district WHERE d_id = ? AND d_w_id = ?");
		statements[id][17] = conn.prepareStatement("SELECT DISTINCT ol_i_id FROM order_line WHERE ol_w_id = ? AND ol_d_id = ? AND ol_o_id < ? AND ol_o_id >= (? - 20)"); //not only id in where
		statements[id][18] = conn.prepareStatement("SELECT count(*) FROM stock WHERE s_w_id = ? AND s_i_id = ? AND s_quantity < ?"); //not only id in where
		//read 19(0~18). write 16(19~34)
		statements[id][19] = conn.prepareStatement("SELECT c_discount, c_last, c_credit, w_tax FROM customer, warehouse WHERE w_id = ? AND c_w_id = w_id AND c_d_id = ? AND c_id = ?");
		statements[id][20] = conn.prepareStatement("SELECT o_id, o_entry_d, COALESCE(o_carrier_id,0) FROM orders WHERE o_w_id = ? AND o_d_id = ? AND o_c_id = ? AND o_id = (SELECT MAX(o_id) FROM orders WHERE o_w_id = ? AND o_d_id = ? AND o_c_id = ?)");
		
		statements[id][21] = conn.prepareStatement("INSERT INTO orders (o_id, o_d_id, o_w_id, o_c_id, o_entry_d, o_ol_cnt, o_all_local) VALUES(?, ?, ?, ?, ?, ?, ?)");
		statements[id][22] = conn.prepareStatement("INSERT INTO new_orders (no_o_id, no_d_id, no_w_id) VALUES (?,?,?)");
		statements[id][23] = conn.prepareStatement("INSERT INTO order_line (ol_o_id, ol_d_id, ol_w_id, ol_number, ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, ol_dist_info) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)");
		statements[id][24] = conn.prepareStatement("INSERT INTO history(h_c_d_id, h_c_w_id, h_c_id, h_d_id, h_w_id, h_date, h_amount, h_data) VALUES(?, ?, ?, ?, ?, ?, ?, ?)");
		
		statements[id][25] = conn.prepareStatement("UPDATE district SET d_next_o_id = d_next_o_id + 1 WHERE d_id = ? AND d_w_id = ?");
		statements[id][26] = conn.prepareStatement("UPDATE stock SET s_quantity = ? WHERE s_i_id = ? AND s_w_id = ?");
		statements[id][27] = conn.prepareStatement("UPDATE warehouse SET w_ytd = w_ytd + ? WHERE w_id = ?");
		statements[id][28] = conn.prepareStatement("UPDATE district SET d_ytd = d_ytd + ? WHERE d_w_id = ? AND d_id = ?");
		statements[id][29] = conn.prepareStatement("UPDATE customer SET c_balance = ?, c_data = ? WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?");
		statements[id][30] = conn.prepareStatement("UPDATE customer SET c_balance = ? WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?");
		statements[id][31] = conn.prepareStatement("UPDATE orders SET o_carrier_id = ? WHERE o_id = ? AND o_d_id = ? AND o_w_id = ?");
		statements[id][32] = conn.prepareStatement("UPDATE order_line SET ol_delivery_d = ? WHERE ol_o_id = ? AND ol_d_id = ? AND ol_w_id = ?");
		statements[id][33] = conn.prepareStatement("UPDATE customer SET c_balance = c_balance + ? , c_delivery_cnt = c_delivery_cnt + 1 WHERE c_id = ? AND c_d_id = ? AND c_w_id = ?");
		
		statements[id][34] = conn.prepareStatement("DELETE FROM new_orders WHERE no_o_id = ? AND no_d_id = ? AND no_w_id = ?");
		
	}
	
	public void sqlPrepare0() throws SQLException{
		statements[id][0] = conn.prepareStatement("SELECT d_next_o_id, d_tax FROM district"+id+" WHERE d_id = ? AND d_w_id = ?");
		statements[id][1] = conn.prepareStatement("SELECT i_price, i_name, i_data FROM item"+id+" WHERE i_id = ?");
		statements[id][2] = conn.prepareStatement("SELECT s_quantity, s_data, s_dist_01, s_dist_02, s_dist_03, s_dist_04, " +
				"s_dist_05, s_dist_06, s_dist_07, s_dist_08, s_dist_09, s_dist_10 FROM stock"+id+" WHERE s_i_id = ? AND s_w_id = ?");
		statements[id][3] = conn.prepareStatement("SELECT w_street_1, w_street_2, w_city, w_state, w_zip, w_name FROM warehouse"+id+" WHERE w_id = ?");
		statements[id][4] = conn.prepareStatement("SELECT d_street_1, d_street_2, d_city, d_state, d_zip, d_name FROM district"+id+" WHERE d_w_id = ? AND d_id = ?");
		statements[id][5] = conn.prepareStatement("SELECT count(c_id) FROM customer"+id+" WHERE c_w_id = ? AND c_d_id = ? AND c_last = ?");
		statements[id][6] = conn.prepareStatement("SELECT c_id FROM customer"+id+" WHERE c_w_id = ? AND c_d_id = ? AND c_last = ? ORDER BY c_first");
		statements[id][7] = conn.prepareStatement("SELECT c_first, c_middle, c_last, c_street_1, c_street_2, c_city, c_state, c_zip, c_phone, c_credit, c_credit_lim, c_discount, c_balance, c_since FROM customer"+id+" WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?");
		statements[id][8] = conn.prepareStatement("SELECT c_data FROM customer"+id+" WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?");
		statements[id][9] = conn.prepareStatement("SELECT count(c_id) FROM customer"+id+" WHERE c_w_id = ? AND c_d_id = ? AND c_last = ?");
		statements[id][10] = conn.prepareStatement("SELECT c_balance, c_first, c_middle, c_last FROM customer"+id+" WHERE c_w_id = ? AND c_d_id = ? AND c_last = ? ORDER BY c_first");
		statements[id][11] = conn.prepareStatement("SELECT c_balance, c_first, c_middle, c_last FROM customer"+id+" WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?");
		statements[id][12] = conn.prepareStatement("SELECT ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, ol_delivery_d FROM order_line"+id+" WHERE ol_w_id = ? AND ol_d_id = ? AND ol_o_id = ?");
		statements[id][13] = conn.prepareStatement("SELECT COALESCE(MIN(no_o_id),0) FROM new_orders"+id+" WHERE no_d_id = ? AND no_w_id = ?");
		statements[id][14] = conn.prepareStatement("SELECT o_c_id FROM orders"+id+" WHERE o_id = ? AND o_d_id = ? AND o_w_id = ?");
		statements[id][15] = conn.prepareStatement("SELECT SUM(ol_amount) FROM order_line"+id+" WHERE ol_o_id = ? AND ol_d_id = ? AND ol_w_id = ?");
		statements[id][16] = conn.prepareStatement("SELECT d_next_o_id FROM district"+id+" WHERE d_id = ? AND d_w_id = ?");
		statements[id][17] = conn.prepareStatement("SELECT DISTINCT ol_i_id FROM order_line"+id+" WHERE ol_w_id = ? AND ol_d_id = ? AND ol_o_id < ? AND ol_o_id >= (? - 20)");
		statements[id][18] = conn.prepareStatement("SELECT count(*) FROM stock"+id+" WHERE s_w_id = ? AND s_i_id = ? AND s_quantity < ?");
		//read 19(0~18). write 16(19~34)
		statements[id][19] = conn.prepareStatement("SELECT c_discount, c_last, c_credit, w_tax FROM customer"+id+", warehouse"+id+" WHERE w_id = ? AND c_w_id = w_id AND c_d_id = ? AND c_id = ?");
		statements[id][20] = conn.prepareStatement("SELECT o_id, o_entry_d, COALESCE(o_carrier_id,0) FROM orders"+id+" WHERE o_w_id = ? AND o_d_id = ? AND o_c_id = ? AND o_id = (SELECT MAX(o_id) FROM orders"+id+" WHERE o_w_id = ? AND o_d_id = ? AND o_c_id = ?)");
		
		statements[id][21] = conn.prepareStatement("INSERT INTO orders"+id+" (o_id, o_d_id, o_w_id, o_c_id, o_entry_d, o_ol_cnt, o_all_local) VALUES(?, ?, ?, ?, ?, ?, ?)");
		statements[id][22] = conn.prepareStatement("INSERT INTO new_orders"+id+" (no_o_id, no_d_id, no_w_id) VALUES (?,?,?)");
		statements[id][23] = conn.prepareStatement("INSERT INTO order_line"+id+" (ol_o_id, ol_d_id, ol_w_id, ol_number, ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, ol_dist_info) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)");
		statements[id][24] = conn.prepareStatement("INSERT INTO history"+id+"(h_c_d_id, h_c_w_id, h_c_id, h_d_id, h_w_id, h_date, h_amount, h_data) VALUES(?, ?, ?, ?, ?, ?, ?, ?)");
		
		statements[id][25] = conn.prepareStatement("UPDATE district"+id+" SET d_next_o_id = d_next_o_id + 1 WHERE d_id = ? AND d_w_id = ?");
		statements[id][26] = conn.prepareStatement("UPDATE stock"+id+" SET s_quantity = ? WHERE s_i_id = ? AND s_w_id = ?");
		statements[id][27] = conn.prepareStatement("UPDATE warehouse"+id+" SET w_ytd = w_ytd + ? WHERE w_id = ?");
		statements[id][28] = conn.prepareStatement("UPDATE district"+id+" SET d_ytd = d_ytd + ? WHERE d_w_id = ? AND d_id = ?");
		statements[id][29] = conn.prepareStatement("UPDATE customer"+id+" SET c_balance = ?, c_data = ? WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?");
		statements[id][30] = conn.prepareStatement("UPDATE customer"+id+" SET c_balance = ? WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?");
		statements[id][31] = conn.prepareStatement("UPDATE orders"+id+" SET o_carrier_id = ? WHERE o_id = ? AND o_d_id = ? AND o_w_id = ?");
		statements[id][32] = conn.prepareStatement("UPDATE order_line"+id+" SET ol_delivery_d = ? WHERE ol_o_id = ? AND ol_d_id = ? AND ol_w_id = ?");
		statements[id][33] = conn.prepareStatement("UPDATE customer"+id+" SET c_balance = c_balance + ? , c_delivery_cnt = c_delivery_cnt + 1 WHERE c_id = ? AND c_d_id = ? AND c_w_id = ?");
		
		statements[id][34] = conn.prepareStatement("DELETE FROM new_orders"+id+" WHERE no_o_id = ? AND no_d_id = ? AND no_w_id = ?");
	}
}
