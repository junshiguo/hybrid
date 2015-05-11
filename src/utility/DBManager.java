package utility;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import org.voltdb.client.Client;
import org.voltdb.client.ClientFactory;

public class DBManager {
	
	public static void main(String[] args){
		connectDB("jdbc:mysql://10.171.5.62:3306", "kevin", "123456");
	}
	
	public static Connection connectDB(String url, String username, String password){
		try {
			Class.forName("com.mysql.jdbc.Driver");
			Connection conn = DriverManager.getConnection(url,username,password);
			return conn;
		} catch (ClassNotFoundException | SQLException e) {
			e.printStackTrace();
			System.exit(0);
			return null;
		}
	}
	
	public static Client connectVoltdb(String serverlist){
        String[] servers = serverlist.split(",");

        org.voltdb.client.Client myApp;
        myApp = ClientFactory.createClient();
        for (String server: servers) { 
            try {
				myApp.createConnection(server);
			} catch (IOException e) {
				e.printStackTrace();
				return null;
			}
        }
        return myApp;
	}
	
	public static void copyTables(int id, Connection conn){
		String[] tables = {"customer", "district", "history", "item", "new_orders",
				"orders", "order_line", "stock", "warehouse"};
		String[] columns = {
				"c_id int not null, c_d_id tinyint not null,c_w_id smallint not null, c_first varchar(16), c_middle char(2), "
						+ "c_last varchar(16), c_street_1 varchar(20), c_street_2 varchar(20), c_city varchar(20), c_state char(2), "
						+ "c_zip char(9), c_phone char(16), c_since datetime, c_credit char(2), c_credit_lim bigint, c_discount decimal(4,2), "
						+ "c_balance decimal(12,2), c_ytd_payment decimal(12,2), c_payment_cnt smallint, c_delivery_cnt smallint, c_data text",
				"d_id tinyint not null, d_w_id smallint not null, d_name varchar(10), d_street_1 varchar(20), d_street_2 varchar(20), d_city varchar(20), d_state char(2), d_zip char(9), d_tax decimal(4,2), d_ytd decimal(12,2), d_next_o_id int",
				"h_c_id int, h_c_d_id tinyint, h_c_w_id smallint,h_d_id tinyint,h_w_id smallint,h_date datetime,h_amount decimal(6,2),h_data varchar(24)",
				"i_id int not null, i_im_id int, i_name varchar(24), i_price decimal(5,2), i_data varchar(50)",
				"no_o_id int not null,no_d_id tinyint not null,no_w_id smallint not null",
				"o_id int not null, o_d_id tinyint not null, o_w_id smallint not null,o_c_id int,o_entry_d datetime,o_carrier_id tinyint,o_ol_cnt tinyint, o_all_local tinyint",
				"ol_o_id int not null, ol_d_id tinyint not null,ol_w_id smallint not null,ol_number tinyint not null,ol_i_id int, ol_supply_w_id smallint,ol_delivery_d datetime, ol_quantity tinyint, ol_amount decimal(6,2), ol_dist_info char(24)",
				"s_i_id int not null, s_w_id smallint not null, s_quantity smallint, s_dist_01 char(24), s_dist_02 char(24),s_dist_03 char(24),s_dist_04 char(24), s_dist_05 char(24), s_dist_06 char(24), s_dist_07 char(24), s_dist_08 char(24), s_dist_09 char(24), s_dist_10 char(24), s_ytd decimal(8,0), s_order_cnt smallint, s_remote_cnt smallint,s_data varchar(50)",
				"w_id smallint not null,w_name varchar(10), w_street_1 varchar(20), w_street_2 varchar(20), w_city varchar(20), w_state char(2), w_zip char(9), w_tax decimal(4,2), w_ytd decimal(12,2)"
		};
			
		try {
			Statement stmt = conn.createStatement();
			Long start = System.currentTimeMillis();
			for(int i=0; i<9; i++){
				stmt.execute("DROP TABLE IF EXISTS "+tables[i]+id);
				stmt.execute("CREATE TABLE "+tables[i]+id+" ("+columns[i]+" )Engine=InnoDB;");
				stmt.execute("INSERT INTO "+tables[i]+id+" SELECT * FROM "+tables[i]);
			}
			stmt.execute("ALTER TABLE customer"+id+" ADD INDEX (c_w_id, c_d_id)");
			stmt.execute("ALTER TABLE district"+id+" ADD INDEX (d_w_id, d_id)");
			stmt.execute("ALTER TABLE item"+id+" ADD INDEX (i_id)");
			stmt.execute("ALTER TABLE new_orders"+id+" ADD INDEX (no_w_id, no_d_id, no_o_id)");
			stmt.execute("ALTER TABLE orders"+id+" ADD INDEX (o_w_id, o_d_id, o_id)");
			stmt.execute("ALTER TABLE order_line"+id+" ADD INDEX (ol_w_id, ol_d_id, ol_o_id)");
			stmt.execute("ALTER TABLE stock"+id+" ADD INDEX (s_w_id, s_i_id)");
			stmt.execute("ALTER TABLE warehouse"+id+" ADD INDEX (w_id)");
			
			Long end = System.currentTimeMillis();
			System.out.println("copy tables for tenant "+id+". Time spent: "+(end-start)/1000F);
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
}
