package utility;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

public class SQLFile {
	public static void main(String[] args){
		FileWriter fstream = null;
		BufferedWriter out = null;
		String path = "../voltTable/";
		int tenantNumber = 100;
		try {
			fstream = new FileWriter(path+"table1.sql", false);
			out = new BufferedWriter(fstream);
			for(int tenantId = 0; tenantId < tenantNumber; tenantId++){
				out.write("create table warehouse"+tenantId+" (w_id int not null,	w_name varchar(10),w_street_1 varchar(20),w_street_2 varchar(20),w_city varchar(20),w_state varchar(2),w_zip varchar(9),w_tax decimal(4,2),	w_ytd decimal(12,2),is_insert int,is_update int"
						+ ", CONSTRAINT w"+tenantId+"_hash PRIMARY KEY (w_id));");
				out.newLine();
				out.write("create table district"+tenantId+" (d_id int not null, d_w_id smallint not null, d_name varchar(10), d_street_1 varchar(20), d_street_2 varchar(20), d_city varchar(20), d_state varchar(2), d_zip varchar(9), d_tax decimal(4,2), d_ytd decimal(12,2), d_next_o_id int,is_insert int,is_update int"
						+ ", CONSTRAINT d"+tenantId+"_hash PRIMARY KEY (d_w_id, d_id));");
				out.newLine();
				out.write("create table customer"+tenantId+" (c_id int not null, c_d_id tinyint not null,c_w_id smallint not null, c_first varchar(16), c_middle varchar(2), c_last varchar(16), c_street_1 varchar(20), c_street_2 varchar(20),c_city varchar(20),c_state varchar(2),c_zip varchar(9), c_phone varchar(16),c_since TIMESTAMP, c_credit varchar(2), c_credit_lim bigint, c_discount decimal(4,2), c_balance decimal(12,2), c_ytd_payment decimal(12,2),c_payment_cnt smallint, c_delivery_cnt smallint, c_data varchar(500),is_insert int,is_update int"
						+ ", CONSTRAINT c"+tenantId+"_hash PRIMARY KEY (c_id, c_w_id, c_d_id)) ;");
				out.newLine();
				out.write("create table history"+tenantId+" (h_c_id int not null, h_c_d_id tinyint, h_c_w_id smallint,h_d_id tinyint,h_w_id smallint,h_date TIMESTAMP,h_amount decimal(6,2),h_data varchar(24),is_insert int,is_update int "
						+ ", CONSTRAINT h"+tenantId+"_hash PRIMARY KEY (h_c_id, h_c_d_id, h_c_w_id));");
				out.newLine();
				out.write("create table new_orders"+tenantId+" (no_o_id int not null,no_d_id tinyint not null,no_w_id smallint not null,is_insert int,is_update int"
						+ ", CONSTRAINT no"+tenantId+"_hash PRIMARY KEY (no_w_id, no_d_id, no_o_id));");
				out.newLine();
				out.write("create table orders"+tenantId+" (o_id int not null, o_d_id tinyint not null, o_w_id smallint not null,o_c_id int,o_entry_d TIMESTAMP,o_carrier_id tinyint,o_ol_cnt tinyint, o_all_local tinyint,is_insert int,is_update int"
						+ ", CONSTRAINT o"+tenantId+"_hash PRIMARY KEY (o_w_id, o_d_id, o_id)) ;");
				out.newLine();
				out.write("create table order_line"+tenantId+" ( ol_o_id int not null, ol_d_id tinyint not null,ol_w_id smallint not null,ol_number tinyint not null,ol_i_id int, ol_supply_w_id smallint,ol_delivery_d TIMESTAMP, ol_quantity tinyint, ol_amount decimal(6,2), ol_dist_info varchar(24),is_insert int,is_update int"
						+ ", CONSTRAINT ol"+tenantId+"_hash PRIMARY KEY (ol_w_id, ol_d_id, ol_o_id, ol_number));");
				out.newLine();
				out.write("create table item"+tenantId+" (i_id int not null, i_im_id int, i_name varchar(24), i_price decimal(5,2), i_data varchar(50),is_insert int,is_update int "
						+ ", CONSTRAINT i"+tenantId+"_hash PRIMARY KEY (i_id));");
				out.newLine();
				out.write("create table stock"+tenantId+" (s_i_id int not null, s_w_id smallint not null, s_quantity smallint, s_dist_01 varchar(24), s_dist_02 varchar(24),s_dist_03 varchar(24),s_dist_04 varchar(24), s_dist_05 varchar(24), s_dist_06 varchar(24), s_dist_07 varchar(24), s_dist_08 varchar(24), s_dist_09 varchar(24), s_dist_10 varchar(24), s_ytd decimal(8,0), s_order_cnt smallint, s_remote_cnt smallint,s_data varchar(50),is_insert int,is_update int"
						+ ", CONSTRAINT s"+tenantId+"_hash PRIMARY KEY (s_w_id, s_i_id));");
				out.newLine();
			}
			
//			for(int tenantId = 0; tenantId < tenantNumber; tenantId++){
//				out.write("CREATE INDEX cIndex"+tenantId+" ON customer"+tenantId+" (c_id, c_w_id, c_d_id);\n");
//				out.write("CREATE INDEX dIndex"+tenantId+" ON district"+tenantId+" (d_w_id, d_id);\n");
//				out.write("CREATE INDEX iIndex"+tenantId+" ON item"+tenantId+" (i_id);\n");
//				out.write("CREATE INDEX noIndex"+tenantId+" ON new_orders"+tenantId+" (no_w_id, no_d_id, no_o_id);\n");
//				out.write("CREATE INDEX oIndex"+tenantId+" ON orders"+tenantId+" (o_w_id, o_d_id, o_id);\n");
//				out.write("CREATE INDEX olIndex"+tenantId+" ON order_line"+tenantId+" (ol_w_id, ol_d_id, ol_o_id);\n");
//				out.write("CREATE INDEX sIndex"+tenantId+" ON stock"+tenantId+" (s_w_id, s_i_id);\n");
//				out.write("CREATE INDEX wIndex"+tenantId+" ON warehouse"+tenantId+" (w_id);\n");
//			}

			for(int tenantId = 0; tenantId < tenantNumber; tenantId++){
				out.write("PARTITION TABLE warehouse"+tenantId+" ON COLUMN w_id;\n");
				out.write("PARTITION TABLE district"+tenantId+" ON COLUMN d_id;\n");
				out.write("PARTITION TABLE customer"+tenantId+" ON COLUMN c_id;\n");
				out.write("PARTITION TABLE history"+tenantId+" ON COLUMN h_c_id;\n");
				out.write("PARTITION TABLE new_orders"+tenantId+" ON COLUMN no_o_id;\n");
				out.write("PARTITION TABLE orders"+tenantId+" ON COLUMN o_id;\n");
				out.write("PARTITION TABLE order_line"+tenantId+" ON COLUMN ol_o_id;\n");
				out.write("PARTITION TABLE item"+tenantId+" ON COLUMN i_id;\n");
				out.write("PARTITION TABLE stock"+tenantId+" ON COLUMN s_i_id;\n");
			}
			
//			for(int tenantId = 0; tenantId < tenantNumber; tenantId++){
//				for(int id = 0; id < 35; id++){
//					out.write("CREATE PROCEDURE FROM CLASS Procedure"+id+"_"+tenantId+";");
//					out.newLine();
//				}
//				out.write("CREATE PROCEDURE FROM CLASS ProcedureInsertCustomer_"+tenantId+";");
//				out.newLine();
//				out.write("CREATE PROCEDURE FROM CLASS ProcedureInsertDistrict_"+tenantId+";");
//				out.newLine();
//				out.write("CREATE PROCEDURE FROM CLASS ProcedureInsertHistory_"+tenantId+";");
//				out.newLine();
//				out.write("CREATE PROCEDURE FROM CLASS ProcedureInsertItem_"+tenantId+";");
//				out.newLine();
//				out.write("CREATE PROCEDURE FROM CLASS ProcedureInsertNewOrders_"+tenantId+";");
//				out.newLine();
//				out.write("CREATE PROCEDURE FROM CLASS ProcedureInsertOrderLine_"+tenantId+";");
//				out.newLine();
//				out.write("CREATE PROCEDURE FROM CLASS ProcedureInsertOrders_"+tenantId+";");
//				out.newLine();
//				out.write("CREATE PROCEDURE FROM CLASS ProcedureInsertStock_"+tenantId+";");
//				out.newLine();
//				out.write("CREATE PROCEDURE FROM CLASS ProcedureInsertWarehouse_"+tenantId+";");
//				out.newLine();
//				out.newLine();
//				out.write("PARTITION PROCEDURE Procedure0_"+tenantId+" ON TABLE district"+tenantId+" COLUMN d_id;\n");
//				out.write("PARTITION PROCEDURE Procedure1_"+tenantId+" ON TABLE item"+tenantId+" COLUMN i_id;\n");
//				out.write("PARTITION PROCEDURE Procedure2_"+tenantId+" ON TABLE stock"+tenantId+" COLUMN s_i_id;\n");
//				out.write("PARTITION PROCEDURE Procedure3_"+tenantId+" ON TABLE warehouse"+tenantId+" COLUMN w_id;\n");
//				out.write("PARTITION PROCEDURE Procedure4_"+tenantId+" ON TABLE district"+tenantId+" COLUMN d_id;\n");
////				out.write("PARTITION PROCEDURE Procedure5_"+tenantId+" ON TABLE district COLUMN d_id\n");
////				out.write("PARTITION PROCEDURE Procedure6_"+tenantId+" ON TABLE district COLUMN d_id\n");
//				out.write("PARTITION PROCEDURE Procedure7_"+tenantId+" ON TABLE customer"+tenantId+" COLUMN c_id;\n");
//				out.write("PARTITION PROCEDURE Procedure8_"+tenantId+" ON TABLE customer"+tenantId+" COLUMN c_id;\n");
////				out.write("PARTITION PROCEDURE Procedure9_"+tenantId+" ON TABLE district COLUMN d_id\n");
////				out.write("PARTITION PROCEDURE Procedure10_"+tenantId+" ON TABLE district COLUMN d_id\n");
//				out.write("PARTITION PROCEDURE Procedure11_"+tenantId+" ON TABLE customer"+tenantId+" COLUMN c_id;\n");
//				out.write("PARTITION PROCEDURE Procedure12_"+tenantId+" ON TABLE order_line"+tenantId+" COLUMN ol_w_id;\n");
//				out.write("PARTITION PROCEDURE Procedure13_"+tenantId+" ON TABLE new_orders"+tenantId+" COLUMN no_d_id;\n");
//				out.write("PARTITION PROCEDURE Procedure14_"+tenantId+" ON TABLE orders"+tenantId+" COLUMN o_id;\n");
//				out.write("PARTITION PROCEDURE Procedure15_"+tenantId+" ON TABLE order_line"+tenantId+" COLUMN ol_w_id;\n");
//				out.write("PARTITION PROCEDURE Procedure16_"+tenantId+" ON TABLE district"+tenantId+" COLUMN d_id;\n");
//				out.write("PARTITION PROCEDURE Procedure17_"+tenantId+" ON TABLE ordre_line"+tenantId+" COLUMN ol_w_id;\n");
//				out.write("PARTITION PROCEDURE Procedure18_"+tenantId+" ON TABLE stock"+tenantId+" COLUMN s_i_id;\n");
//				out.write("PARTITION PROCEDURE Procedure19_"+tenantId+" ON TABLE warehouse"+tenantId+" COLUMN w_id;\n");
////				out.write("PARTITION PROCEDURE Procedure20_"+tenantId+" ON TABLE district COLUMN d_id\n");
//				out.write("PARTITION PROCEDURE Procedure21_"+tenantId+" ON TABLE orders"+tenantId+" COLUMN o_id;\n");
//				out.write("PARTITION PROCEDURE Procedure22_"+tenantId+" ON TABLE new_orders"+tenantId+" COLUMN no_d_id;\n");
//				out.write("PARTITION PROCEDURE Procedure23_"+tenantId+" ON TABLE order_line"+tenantId+" COLUMN ol_w_id;\n");
//				out.write("PARTITION PROCEDURE Procedure24_"+tenantId+" ON TABLE history"+tenantId+" COLUMN h_C_id;\n");
//				out.write("PARTITION PROCEDURE Procedure25_"+tenantId+" ON TABLE district"+tenantId+" COLUMN d_id;\n");
//				out.write("PARTITION PROCEDURE Procedure26_"+tenantId+" ON TABLE stock"+tenantId+" COLUMN s_i_id;\n");
//				out.write("PARTITION PROCEDURE Procedure27_"+tenantId+" ON TABLE warehouse"+tenantId+" COLUMN w_id;\n");
//				out.write("PARTITION PROCEDURE Procedure28_"+tenantId+" ON TABLE district"+tenantId+" COLUMN d_id;\n");
//				out.write("PARTITION PROCEDURE Procedure29_"+tenantId+" ON TABLE customer"+tenantId+" COLUMN c_id;\n");
//				out.write("PARTITION PROCEDURE Procedure30_"+tenantId+" ON TABLE customer"+tenantId+" COLUMN c_id;\n");
//				out.write("PARTITION PROCEDURE Procedure31_"+tenantId+" ON TABLE orders"+tenantId+" COLUMN o_id;\n");
//				out.write("PARTITION PROCEDURE Procedure32_"+tenantId+" ON TABLE order_line"+tenantId+" COLUMN ol_w_id;\n");
//				out.write("PARTITION PROCEDURE Procedure33_"+tenantId+" ON TABLE customer"+tenantId+" COLUMN c_id;\n");
//				out.write("PARTITION PROCEDURE Procedure34_"+tenantId+" ON TABLE new_orders"+tenantId+" COLUMN no_d_id;\n");
//				out.write("PARTITION PROCEDURE ProcedureInsertCustomer_"+tenantId+" ON TABLE customer"+tenantId+" COLUMN c_id;\n");
//				out.write("PARTITION PROCEDURE ProcedureInsertDistrict_"+tenantId+" ON TABLE district"+tenantId+" COLUMN d_id;\n");
//				out.write("PARTITION PROCEDURE ProcedureInsertHistory_"+tenantId+" ON TABLE history"+tenantId+" COLUMN h_c_id;\n");
//				out.write("PARTITION PROCEDURE ProcedureInsertItem_"+tenantId+" ON TABLE item"+tenantId+" COLUMN i_id;\n");
//				out.write("PARTITION PROCEDURE ProcedureInsertNewOrders_"+tenantId+" ON TABLE new_orders"+tenantId+" COLUMN no_o_id;\n");
//				out.write("PARTITION PROCEDURE ProcedureInsertOrderLine_"+tenantId+" ON TABLE order_line"+tenantId+" COLUMN ol_o_id;\n");
//				out.write("PARTITION PROCEDURE ProcedureInsertOrders_"+tenantId+" ON TABLE orders"+tenantId+" COLUMN o_id;\n");
//				out.write("PARTITION PROCEDURE ProcedureInsertStock_"+tenantId+" ON TABLE stock"+tenantId+" COLUMN s_i_id;\n");
//				out.write("PARTITION PROCEDURE ProcedureInsertWarehouse_"+tenantId+" ON TABLE warehouse"+tenantId+" COLUMN w_id;\n");
//				out.newLine();
//			}
			out.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
}
