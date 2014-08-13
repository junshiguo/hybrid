package hybridUtility;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;

public class MCopyPK {
	public static Connection conn;
	public static PreparedStatement st;
	
	public static void main(String[] args){
		int tenantNumber = 1500;
		int startId = 0;
		String server = "127.0.0.1";
		String db = "tpcc3000";
		if(args.length > 0){
			server = args[0];
		}
		if(args.length > 1){
			db = args[1];
		}
		if(args.length > 2){
			startId = Integer.parseInt(args[2]);
		}
		if(args.length > 3){
			tenantNumber = Integer.parseInt(args[3]);
		}
		CopyTables(server, db, startId, tenantNumber);
	}
	
	public static void CopyTables(String server, String db, int startId, int tenantNumber){
		try {
			Class.forName("com.mysql.jdbc.Driver");
			conn = DriverManager.getConnection("jdbc:mysql://"+server+"/"+db, "remote", "remote");
			System.out.println("db connected~");
		} catch (ClassNotFoundException | SQLException e1) {
			e1.printStackTrace();
		}		
		int endId = startId + tenantNumber;
		for(int id = startId; id <endId; id++)
			copyTables(id);
		
		try {
			conn.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	public static void copyTables(int id){
		String[] tables = {"customer", "district", "history", "item", "new_orders",
				"orders", "order_line", "stock", "warehouse"};
		String[] columns = {
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
				"w_id smallint not null,w_name varchar(10), w_street_1 varchar(20), w_street_2 varchar(20), w_city varchar(20), w_state char(2), w_zip char(9), w_tax decimal(4,2), w_ytd decimal(12,2), CONSTRAINT w PRIMARY KEY (w_id)"
		};
//		String[] pkey = {
//				"ALTER TABLE customer  ADD CONSTRAINT pkey_customer  PRIMARY KEY(c_w_id, c_d_id, c_id);",
//				"ALTER TABLE district  ADD CONSTRAINT pkey_district  PRIMARY KEY(d_w_id, d_id);",
		
//				"ALTER TABLE item ADD CONSTRAINT pkey_item PRIMARY KEY(i_id);",
//				"ALTER TABLE new_orders ADD CONSTRAINT pkey_new_orders PRIMARY KEY(no_w_id, no_d_id, no_o_id);",
//				"ALTER TABLE orders    ADD CONSTRAINT pkey_orders    PRIMARY KEY(o_w_id, o_d_id, o_id);",
//				"ALTER TABLE order_line ADD CONSTRAINT pkey_order_line PRIMARY KEY(ol_w_id, ol_d_id, ol_o_id, ol_number);",
//				"ALTER TABLE stock ADD CONSTRAINT pkey_stock PRIMARY KEY(s_w_id, s_i_id);",
//				"ALTER TABLE warehouse ADD CONSTRAINT pkey_warehouse PRIMARY KEY(w_id);"
//		};

		try {
			Statement stmt = conn.createStatement();
			Long start = System.currentTimeMillis();
			for(int i=0; i<9; i++){
				stmt.execute("DROP TABLE IF EXISTS "+tables[i]+id);
				stmt.execute("CREATE TABLE "+tables[i]+id+" ("+columns[i]+" )Engine=InnoDB;");
				stmt.execute("INSERT INTO "+tables[i]+id+" SELECT * FROM "+tables[i]);
			}
//			stmt.execute("ALTER TABLE customer"+id+" ADD INDEX (c_w_id, c_d_id, c_id)");
//			stmt.execute("ALTER TABLE district"+id+" ADD INDEX (d_w_id, d_id)");
//			stmt.execute("ALTER TABLE item"+id+" ADD INDEX (i_id)");
//			stmt.execute("ALTER TABLE new_orders"+id+" ADD INDEX (no_w_id, no_d_id, no_o_id)");
//			stmt.execute("ALTER TABLE orders"+id+" ADD INDEX (o_w_id, o_d_id, o_id)");
//			stmt.execute("ALTER TABLE order_line"+id+" ADD INDEX (ol_w_id, ol_d_id, ol_o_id)");
//			stmt.execute("ALTER TABLE stock"+id+" ADD INDEX (s_w_id, s_i_id)");
//			stmt.execute("ALTER TABLE warehouse"+id+" ADD INDEX (w_id)");
			Long end = System.currentTimeMillis();
			System.out.println("copy tables for tenant "+id+". Time spent: "+(end-start)/1000F);
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
		
}
