package hybridUtility;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;

public class MCopyPK {
	public static Connection conn;
	public static PreparedStatement st;

	public static String server = "10.20.2.28";
	public static String db = "tpcc_m";

	public static void main(String[] args) throws SQLException {
		int tenantNumber = 500;
		int startId = 500;
		if (args.length > 0) {
			server = args[0];
		}
		if (args.length > 1) {
			db = args[1];
		}
		if (args.length > 2) {
			startId = Integer.parseInt(args[2]);
		}
		if (args.length > 3) {
			tenantNumber = Integer.parseInt(args[3]);
		}
		CopyTables(server, db, startId, tenantNumber);
		conn.close();
//		extendTable();
	}

	public static void CopyTables(String server, String db, int startId,
			int tenantNumber) {
		try {
			Class.forName("com.mysql.jdbc.Driver");
			conn = DriverManager.getConnection("jdbc:mysql://" + server + "/"
					+ db, "remote", "remote");
			System.out.println("db connected~");
		} catch (ClassNotFoundException | SQLException e1) {
			e1.printStackTrace();
		}
		for (int id = startId; id < startId+tenantNumber; id++) {
			copyTables(id);
		}
//		copyTables2(0);
//		for(int id = 3; id < 1500; id++){
//			copyTables(id);
//		}
//		copyTables2(1);
//		for(int id = 1500; id < 2400; id++){
//			copyTables(id);
//		}
//		copyTables2(2);
//		for(int id = 2400; id < 3000; id++){
//			copyTables(id);
//		}
//		copyTables2(0);
//		for(int id = 1; id < 3; id++){
//			copyTables(id);
//		}
		
		try {
			conn.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	public static void extendTable(){
		try {
			checkConn();
			Statement stmt = conn.createStatement();
			for(int i = 0; i < 9; i++){
				stmt.execute("ALTER TABLE "+tables[i]+" ADD COLUMN is_insert tinyint default 0 AFTER "+lastColumn[i]);
				stmt.execute("ALTER TABLE "+tables[i]+" ADD COLUMN is_update tinyint default 0 AFTER is_insert");
				stmt.execute("ALTER TABLE "+tables[i]+" ADD COLUMN is_delete tinyint default 0 AFTER is_update");
			}
		} catch (ClassNotFoundException | SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public static void copyTables2(int id) { // copy table_id to table
		try {
			checkConn();
			Statement stmt = conn.createStatement();
			for (int i = 0; i < 9; i++) {
				stmt.execute("TRUNCATE TABLE " + tables[i]);
				// stmt.execute("CREATE TABLE "+tables[i]+id+" ("+columns[i]+" )Engine=InnoDB;");
				stmt.execute("INSERT INTO " + tables[i] + " SELECT * FROM "
						+ tables[i] + id);
			}
			System.out.println("recopy table finished!");
		} catch (SQLException | ClassNotFoundException e) {
			e.printStackTrace();
		}
	}

	public static String[] tables = { "customer", "district", "history",
			"item", "new_orders", "orders", "order_line", "stock", "warehouse" };
	public static String[] lastColumn = {"c_data", "d_next_o_id", "h_data", "i_data", "no_w_id", "o_all_local", "ol_dist_info", "s_data", "w_ytd"};
	public static String[] columns = {
			"c_id int not null, c_d_id tinyint not null,c_w_id smallint not null, c_first varchar(16), c_middle char(2), "
					+ "c_last varchar(16), c_street_1 varchar(20), c_street_2 varchar(20), c_city varchar(20), c_state char(2), "
					+ "c_zip char(9), c_phone char(16), c_since datetime, c_credit char(2), c_credit_lim bigint, c_discount decimal(4,2), "
					+ "c_balance decimal(12,2), c_ytd_payment decimal(12,2), c_payment_cnt smallint, c_delivery_cnt smallint, c_data text, CONSTRAINT w PRIMARY KEY (c_id, c_w_id, c_d_id)",
			"d_id tinyint not null, d_w_id smallint not null, d_name varchar(10), d_street_1 varchar(20), d_street_2 varchar(20), d_city varchar(20), d_state char(2), d_zip char(9), d_tax decimal(4,2), d_ytd decimal(12,2), d_next_o_id int, CONSTRAINT d PRIMARY KEY (d_w_id, d_id)",
			"h_c_id int, h_c_d_id tinyint, h_c_w_id smallint,h_d_id tinyint,h_w_id smallint,h_date datetime,h_amount decimal(6,2),h_data varchar(24), CONSTRAINT h PRIMARY KEY (h_c_id, h_c_d_id, h_c_w_id)",
			"i_id int not null, i_im_id int, i_name varchar(24), i_price decimal(5,2), i_data varchar(50),CONSTRAINT i PRIMARY KEY (i_id)",
			"no_o_id int not null,no_d_id tinyint not null,no_w_id smallint not null, CONSTRAINT no PRIMARY KEY (no_w_id, no_d_id, no_o_id)",
			"o_id int not null, o_d_id tinyint not null, o_w_id smallint not null,o_c_id int,o_entry_d datetime,o_carrier_id tinyint,o_ol_cnt tinyint, o_all_local tinyint, CONSTRAINT o PRIMARY KEY (o_w_id, o_d_id, o_id)",
			"ol_o_id int not null, ol_d_id tinyint not null,ol_w_id smallint not null,ol_number tinyint not null,ol_i_id int, ol_supply_w_id smallint,ol_delivery_d datetime, ol_quantity tinyint, ol_amount decimal(6,2), ol_dist_info char(24), CONSTRAINT ol PRIMARY KEY (ol_w_id, ol_d_id, ol_o_id, ol_number)",
			"s_i_id int not null, s_w_id smallint not null, s_quantity smallint, s_dist_01 char(24), s_dist_02 char(24),s_dist_03 char(24),s_dist_04 char(24), s_dist_05 char(24), s_dist_06 char(24), s_dist_07 char(24), s_dist_08 char(24), s_dist_09 char(24), s_dist_10 char(24), s_ytd decimal(8,0), s_order_cnt smallint, s_remote_cnt smallint,s_data varchar(50), CONSTRAINT s PRIMARY KEY (s_w_id, s_i_id)",
			"w_id smallint not null,w_name varchar(10), w_street_1 varchar(20), w_street_2 varchar(20), w_city varchar(20), w_state char(2), w_zip char(9), w_tax decimal(4,2), w_ytd decimal(12,2), CONSTRAINT w PRIMARY KEY (w_id)" };
	public static String[] columns_extend = {
			"c_id int not null, c_d_id tinyint not null,c_w_id smallint not null, c_first varchar(16), c_middle char(2), "
					+ "c_last varchar(16), c_street_1 varchar(20), c_street_2 varchar(20), c_city varchar(20), c_state char(2), "
					+ "c_zip char(9), c_phone char(16), c_since datetime, c_credit char(2), c_credit_lim bigint, c_discount decimal(4,2), "
					+ "c_balance decimal(12,2), c_ytd_payment decimal(12,2), c_payment_cnt smallint, c_delivery_cnt smallint, c_data text, is_insert tinyint, is_update tinyint, is_delete tinyint, CONSTRAINT w PRIMARY KEY (c_id, c_w_id, c_d_id)",
			"d_id tinyint not null, d_w_id smallint not null, d_name varchar(10), d_street_1 varchar(20), d_street_2 varchar(20), d_city varchar(20), d_state char(2), d_zip char(9), d_tax decimal(4,2), d_ytd decimal(12,2), d_next_o_id int, is_insert tinyint, is_update tinyint, is_delete tinyint, CONSTRAINT d PRIMARY KEY (d_w_id, d_id)",
			"h_c_id int, h_c_d_id tinyint, h_c_w_id smallint,h_d_id tinyint,h_w_id smallint,h_date datetime,h_amount decimal(6,2),h_data varchar(24), is_insert tinyint, is_update tinyint, is_delete tinyint, CONSTRAINT h PRIMARY KEY (h_c_id, h_c_d_id, h_c_w_id)",
			"i_id int not null, i_im_id int, i_name varchar(24), i_price decimal(5,2), i_data varchar(50), is_insert tinyint, is_update tinyint, is_delete tinyint, CONSTRAINT i PRIMARY KEY (i_id)",
			"no_o_id int not null,no_d_id tinyint not null,no_w_id smallint not null, is_insert tinyint, is_update tinyint, is_delete tinyint, CONSTRAINT no PRIMARY KEY (no_w_id, no_d_id, no_o_id)",
			"o_id int not null, o_d_id tinyint not null, o_w_id smallint not null,o_c_id int,o_entry_d datetime,o_carrier_id tinyint,o_ol_cnt tinyint, o_all_local tinyint, is_insert tinyint, is_update tinyint, is_delete tinyint, CONSTRAINT o PRIMARY KEY (o_w_id, o_d_id, o_id)",
			"ol_o_id int not null, ol_d_id tinyint not null,ol_w_id smallint not null,ol_number tinyint not null,ol_i_id int, ol_supply_w_id smallint,ol_delivery_d datetime, ol_quantity tinyint, ol_amount decimal(6,2), ol_dist_info char(24), is_insert tinyint, is_update tinyint, is_delete tinyint, CONSTRAINT ol PRIMARY KEY (ol_w_id, ol_d_id, ol_o_id, ol_number)",
			"s_i_id int not null, s_w_id smallint not null, s_quantity smallint, s_dist_01 char(24), s_dist_02 char(24),s_dist_03 char(24),s_dist_04 char(24), s_dist_05 char(24), s_dist_06 char(24), s_dist_07 char(24), s_dist_08 char(24), s_dist_09 char(24), s_dist_10 char(24), s_ytd decimal(8,0), s_order_cnt smallint, s_remote_cnt smallint,s_data varchar(50), is_insert tinyint, is_update tinyint, is_delete tinyint, CONSTRAINT s PRIMARY KEY (s_w_id, s_i_id)",
			"w_id smallint not null,w_name varchar(10), w_street_1 varchar(20), w_street_2 varchar(20), w_city varchar(20), w_state char(2), w_zip char(9), w_tax decimal(4,2), w_ytd decimal(12,2), is_insert tinyint, is_update tinyint, is_delete tinyint, CONSTRAINT w PRIMARY KEY (w_id)" };
	public static int[] mtest = {7};
	public static void copyTables(int id) {
		try {
			checkConn();
			Statement stmt = conn.createStatement();
			Long start = System.currentTimeMillis();
//			for(int i=0; i<9; i++){
			for(int i : mtest){
				stmt.execute("DROP TABLE IF EXISTS " + tables[i] + id);
				stmt.execute("CREATE TABLE " + tables[i] + id + " (" + columns[i]
						+ " )Engine=InnoDB;");
				 stmt.execute("INSERT INTO "+tables[i]+id+" SELECT * FROM "+tables[i]);
			 }
			Long end = System.currentTimeMillis();
			System.out.println("copy tables for tenant " + id
					+ ". Time spent: " + (end - start) / 1000F);
		} catch (SQLException | ClassNotFoundException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * currently not used.
	 * @param id
	 */
	public static void copyTables_extend(int id) {
		try {
			checkConn();
			Statement stmt = conn.createStatement();
			Long start = System.currentTimeMillis();
			for(int i=0; i<9; i++){
//			int i = 4;
				stmt.execute("DROP TABLE IF EXISTS " + tables[i] + id);
				stmt.execute("CREATE TABLE " + tables[i] + id + " (" + columns_extend[i]
						+ " )Engine=InnoDB;");
				 stmt.execute("INSERT INTO "+tables[i]+id+" SELECT * FROM "+tables[i]);
			 }
			Long end = System.currentTimeMillis();
			System.out.println("copy tables for tenant " + id
					+ ". Time spent: " + (end - start) / 1000F);
		} catch (SQLException | ClassNotFoundException e) {
			e.printStackTrace();
		}
	}
	
	public static void checkConn() throws ClassNotFoundException, SQLException{
		if (conn == null || conn.isClosed()) {
			Class.forName("com.mysql.jdbc.Driver");
			conn = DriverManager.getConnection("jdbc:mysql://" + server
					+ "/" + db, "remote", "remote");
			System.out.println("db connected~");
		}
	}

}
