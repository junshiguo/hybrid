package utility;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

public class VoltProcedureGenerator {
	
	public static String getImport(){
		return "import org.voltdb.*;";
	}
	public static String getClassHead(String name){
		return "public class "+name+" extends VoltProcedure";
	}
	public static String getSQLStmt(String sql){
		return "	public final SQLStmt sql = new SQLStmt(\""+sql+"\");";
	}
	public static String getRunHead(String var){
		return "	public long run("+var+") throws VoltAbortException ";
	}
	public static String commonCode0 = "		VoltTable[] results =  voltExecuteSQL();\n"
				+ "		if(results[0].getRowCount() == 0)\n"
				+ "			return 0;\n"
				+ "		return 1;";
	public static String commonCode1 = "		voltExecuteSQL();\n		return 1;";
	public static String commonCode2 = "		VoltTable[] reuslt = voltExecuteSQL();\n"
			+ "		if(reuslt[0].asScalarLong() == 0)\n"
			+ "			return 0;\n"
			+ "		return 1;";
	public static String enter = "\n";
	public static String leftBrace = "{";
	public static String rightBrace = "}";
	
	public static void main(String[] args){
		FileWriter fstream = null;
		BufferedWriter out = null;
		String path = "../voltTable/procedures/";
		int tenantNumber = 500;
		try {
			for(int tenantId = 0; tenantId < tenantNumber; tenantId++){
				/*
				fstream = new FileWriter(path+"Procedure0_"+tenantId+".java", false);
				out = new BufferedWriter(fstream);
				out.write(getImport() + enter
						+ getClassHead("Procedure0_"+tenantId) + leftBrace + enter
						+ getSQLStmt("SELECT d_next_o_id, d_tax FROM district"+tenantId+" WHERE d_id = ? AND d_w_id = ?") + enter
						+ getRunHead("int d_id, int d_w_id") + leftBrace + enter
						+ "		voltQueueSQL(sql, d_id, d_w_id);" + enter
						+ commonCode0+enter
						+ "	"+rightBrace + enter + rightBrace);
				out.flush();out.close();
				
				fstream = new FileWriter(path+"Procedure1_"+tenantId+".java", false);
				out = new BufferedWriter(fstream);
				out.write("import org.voltdb.*;\n"
						+ "public class Procedure1_"+tenantId+" extends VoltProcedure {\n"
						+ "	public final SQLStmt sql = new SQLStmt(\"SELECT i_price, i_name, i_data FROM item"+tenantId+" WHERE i_id = ?\");\n"
								+ "	public long run(int i_id) throws VoltAbortException {\n"
								+ "		voltQueueSQL(sql, i_id);\n"
								+ commonCode0+"\n"
								+ "	}\n"
								+ "}");
				out.flush();out.close();
				
				fstream = new FileWriter(path+"Procedure2_"+tenantId+".java", false);
				out = new BufferedWriter(fstream);
				out.write("import org.voltdb.*;\n"
						+ "public class Procedure2_"+tenantId+" extends VoltProcedure {\n"
						+ "	public final SQLStmt sql = new SQLStmt(\"SELECT s_quantity, s_data, s_dist_01, s_dist_02, s_dist_03, s_dist_04, " +
				"s_dist_05, s_dist_06, s_dist_07, s_dist_08, s_dist_09, s_dist_10 FROM stock"+tenantId
				+ " WHERE s_i_id = ? AND s_w_id = ?\");\n"
				+ "	public long run(int s_i_id, int s_w_id) throws VoltAbortException {\n"
				+ commonCode0+"\n"
				+ "	}\n}");
				out.flush();out.close();
				
				fstream = new FileWriter(path+"Procedure3_"+tenantId+".java", false);
				out = new BufferedWriter(fstream);
				out.write("import org.voltdb.*;\n"
						+ "public class Procedure3_"+tenantId+" extends VoltProcedure {\n"
						+ "	public final SQLStmt sql = new SQLStmt(\"SELECT w_street_1, w_street_2, w_city, w_state, w_zip, w_name "
			+ "FROM warehouse"+tenantId+" WHERE w_id = ?\");\n"
			+ "	public long run(int w_id) throws VoltAbortException{\n"
			+ "		voltQueueSQL(sql, w_id);\n"
			+ commonCode0+"\n"
			+ "	}\n}");
				out.flush();out.close();
				
				fstream = new FileWriter(path+"Procedure4_"+tenantId+".java", false);
				out = new BufferedWriter(fstream);
				out.write("import org.voltdb.*;\n"
						+ "public class Procedure4_"+tenantId+" extends VoltProcedure {\n"
						+ "	public final SQLStmt sql = new SQLStmt(\"SELECT d_street_1, d_street_2, d_city, d_state, d_zip, d_name "
			+ "FROM district"+tenantId+" WHERE d_id = ? AND d_w_id = ?\");\n"
			+ "	public long run(int d_id, int d_w_id) throws VoltAbortException {\n"
			+ "		voltQueueSQL(sql, d_id, d_w_id);\n"
			+ commonCode0+"\n"
			+ "	}\n}");
				out.flush();out.close();
				
				fstream = new FileWriter(path+"Procedure5_"+tenantId+".java", false);
				out = new BufferedWriter(fstream);
				out.write("import org.voltdb.*;\n"
						+ "public class Procedure5_"+tenantId+" extends VoltProcedure {\n"
						+ "	public final SQLStmt sql = new SQLStmt(\"SELECT count(c_id) FROM customer"+tenantId+" WHERE c_w_id = ? AND c_d_id = ? AND c_last = ?\");\n"
						+ "	public long run(int c_w_id, int c_d_id, String c_last) throws VoltAbortException {\n"
						+ "		voltQueueSQL(sql, c_w_id, c_d_id, c_last);\n"
						+ "		VoltTable[] results = voltExecuteSQL();\n"
						+ "		if(results[0].getRowCount() == 0 || results[0].fetchRow(0).getLong(0) == 0)\n"
						+ "			return 0;\n"
						+ "		return 1;\n"
						+ "	}\n}");
				out.flush();out.close();
				
				fstream = new FileWriter(path+"Procedure6_"+tenantId+".java", false);
				out = new BufferedWriter(fstream);
				out.write("import org.voltdb.*;\n" +
						"public class Procedure6_"+tenantId+" extends VoltProcedure {\n" +
						"	public final SQLStmt sql = new SQLStmt(\"SELECT c_id FROM customer"+tenantId+" "
			+ "WHERE c_w_id = ? AND c_d_id = ? AND c_last = ? ORDER BY c_first\");\n" +
			"	public long run(int c_w_id, int c_d_id, String c_last) throws VoltAbortException {\n" +
			"		voltQueueSQL(sql, c_w_id, c_d_id, c_last);\n" 
			+ commonCode0+"\n"
			+ "	}\n}");
				out.flush();out.close();
				
				fstream = new FileWriter(path+"Procedure7_"+tenantId+".java", false);
				out = new BufferedWriter(fstream);
				out.write("import org.voltdb.*;\n" +
						"public class Procedure7_"+tenantId+" extends VoltProcedure {\n" +
						"	public final SQLStmt sql = new SQLStmt(\"SELECT c_first, c_middle, c_last, c_street_1, c_street_2, c_city, c_state, c_zip, c_phone, c_credit, c_credit_lim, c_discount, c_balance, c_since "
			+ "FROM customer"+tenantId+" WHERE c_id = ? AND c_w_id = ? AND c_d_id = ?\");\n" +
			"	public long run(int c_id, int c_w_id, int c_d_id) throws VoltAbortException {\n" +
			"		voltQueueSQL(sql, c_id, c_w_id, c_d_id);\n" 
			+ commonCode0+"\n"
			+ "	}\n}");
				out.flush();out.close();
				
				fstream = new FileWriter(path+"Procedure8_"+tenantId+".java", false);
				out = new BufferedWriter(fstream);
				out.write("import org.voltdb.*;\n" +
						"public class Procedure8_"+tenantId+" extends VoltProcedure {\n" +
						"	public final SQLStmt sql = new SQLStmt(\"SELECT c_data FROM customer"+tenantId
						+" WHERE c_id = ? AND c_w_id = ? AND c_d_id = ?\");\n" +
						"	public long run(int c_id, int c_w_id, int c_d_id) throws VoltAbortException{\n" +
						"		voltQueueSQL(sql, c_id, c_w_id, c_d_id);\n" 
						+ commonCode0+"\n"
						+ "	}\n}");
				out.flush();out.close();
				
				fstream = new FileWriter(path+"Procedure9_"+tenantId+".java", false);
				out = new BufferedWriter(fstream);
				out.write("import org.voltdb.*;\n" +
						"public class Procedure9_"+tenantId+" extends VoltProcedure {\n" +
						"	public final SQLStmt sql = new SQLStmt(\"SELECT count(c_id) FROM customer"+tenantId +
						" WHERE c_w_id = ? AND c_d_id = ? AND c_last = ?\");\n" +
						"	public long run(int c_w_id, int c_d_id, String c_last) throws VoltAbortException {\n" +
						"		voltQueueSQL(sql, c_w_id, c_d_id, c_last);\n" +
						"		VoltTable[] results = voltExecuteSQL();\n" +
						"		if(results[0].getRowCount() == 0 || results[0].fetchRow(0).getLong(0) == 0)\n" +
						"			return 0;\n" +
						"		return 1;\n	}\n}");
				out.flush();out.close();
				
				fstream = new FileWriter(path+"Procedure10_"+tenantId+".java", false);
				out = new BufferedWriter(fstream);
				out.write("import org.voltdb.*;\n" +
						"public class Procedure10_"+tenantId+" extends VoltProcedure {\n" +
						"	public final SQLStmt sql = new SQLStmt(\"SELECT c_balance, c_first, c_middle, c_last FROM customer"+tenantId
			+ " WHERE c_w_id = ? AND c_d_id = ? AND c_last = ? ORDER BY c_first\");\n" +
			"	public long run(int c_w_id, int c_d_id, String c_last) throws VoltAbortException{\n" +
			"		voltQueueSQL(sql, c_w_id, c_d_id, c_last);\n" +
			commonCode0+"\n"
			+ "	}\n}");
				out.flush();out.close();
				
				fstream = new FileWriter(path+"Procedure11_"+tenantId+".java", false);
				out = new BufferedWriter(fstream);
				out.write("import org.voltdb.*;\n" +
						"public class Procedure11_"+tenantId+" extends VoltProcedure {\n" +
						"	public final SQLStmt sql = new SQLStmt(\"SELECT c_balance, c_first, c_middle, c_last FROM customer"+tenantId
			+ " WHERE c_id = ? AND c_w_id = ? AND c_d_id = ?\");\n" +
			"	public long run(int c_id, int c_w_id, int c_d_id) throws VoltAbortException{\n" +
			"		voltQueueSQL(sql, c_id, c_w_id, c_d_id);\n" +
			commonCode0+"\n"
			+ "}\n}");
				out.flush();out.close();
				
				fstream = new FileWriter(path+"Procedure12_"+tenantId+".java", false);
				out = new BufferedWriter(fstream);
				out.write("import org.voltdb.*;\n" +
						"public class Procedure12_"+tenantId+" extends VoltProcedure {\n" +
						"	public final SQLStmt sql = new SQLStmt(\"SELECT ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, ol_delivery_d "
					+ "FROM order_line"+tenantId+" WHERE ol_w_id = ? AND ol_d_id = ? AND ol_o_id = ?\");\n" +
					"	public long run(int ol_w_id, int ol_d_id,\n" +
					"			int ol_o_id) throws VoltAbortException {\n" +
					"		voltQueueSQL(sql, ol_w_id, ol_d_id, ol_o_id);\n" +
					commonCode0+"\n"
					+ "	}\n}");
				out.flush();out.close();
				
				fstream = new FileWriter(path+"Procedure13_"+tenantId+".java", false);
				out = new BufferedWriter(fstream);
				out.write("import org.voltdb.*;\n" +
						"public class Procedure13_"+tenantId+" extends VoltProcedure {\n" +
						"	public final SQLStmt sql = new SQLStmt(\"SELECT COALESCE(MIN(no_o_id),0) FROM new_orders"+tenantId
			+ " WHERE no_d_id = ? AND no_w_id = ?\");\n" +
			"	public long run(int no_d_id, int no_w_id) throws VoltAbortException{\n" +
			"		voltQueueSQL(sql, no_d_id, no_w_id);\n" +
			"		VoltTable[] results = voltExecuteSQL();\n" +
			"		if(results[0].getRowCount() == 0 || (results[0].fetchRow(0).getLong(0)) == 0)\n" +
			"			return 0;\n" +
			"		return 1;\n	}\n}");
				out.flush();out.close();
				
				fstream = new FileWriter(path+"Procedure14_"+tenantId+".java", false);
				out = new BufferedWriter(fstream);
				out.write("import org.voltdb.*;\n"
						+ "public class Procedure14_"+tenantId+" extends VoltProcedure {\n"
						+ "	public final SQLStmt sql = new SQLStmt(\"SELECT o_c_id FROM orders"+tenantId
			+ " WHERE o_id = ? AND o_d_id = ? AND o_w_id = ?\");\n"
			+ "	public long run(int o_id, int o_d_id, int o_w_id) throws VoltAbortException {\n"
			+ "		voltQueueSQL(sql, o_id, o_d_id, o_w_id);\n"
			+ commonCode0+"\n"
			+ "	}\n}");
				out.flush();out.close();
				
				fstream = new FileWriter(path+"Procedure15_"+tenantId+".java", false);
				out = new BufferedWriter(fstream);
				out.write("import org.voltdb.*;\n"
						+ "public class Procedure15_"+tenantId+" extends VoltProcedure {\n"
						+ "	public final SQLStmt sql = new SQLStmt(\"SELECT SUM(ol_amount) FROM order_line"+tenantId
			+ " WHERE ol_w_id = ? AND ol_o_id = ? AND ol_d_id = ?\");\n"
			+ "	public long run(int ol_w_id, int ol_o_id, int ol_d_id) throws VoltAbortException {\n"
			+ "		voltQueueSQL(sql, ol_w_id, ol_o_id, ol_d_id);\n"
			+ commonCode0+"\n"
			+ "	}\n}");
				out.flush();out.close();
				
				fstream = new FileWriter(path+"Procedure16_"+tenantId+".java", false);
				out = new BufferedWriter(fstream);
				out.write("import org.voltdb.*;\n"
						+ "public class Procedure16_"+tenantId+" extends VoltProcedure {\n"
						+ "	public final SQLStmt sql = new SQLStmt(\"SELECT d_next_o_id FROM district"+tenantId+" WHERE d_id = ? AND d_w_id = ?\");\n"
						+ "	public long run(int d_id, int d_w_id) throws VoltAbortException{\n"
						+ "		voltQueueSQL(sql, d_id, d_w_id);\n"
						+ commonCode0+"\n"
						+ "	}\n}");
				out.flush();out.close();
				
				fstream = new FileWriter(path+"Procedure17_"+tenantId+".java", false);
				out = new BufferedWriter(fstream);
				out.write("import org.voltdb.*;\n"
						+ "public class Procedure17_"+tenantId+" extends VoltProcedure {\n"
						+ "	public final SQLStmt sql = new SQLStmt(\"SELECT DISTINCT ol_i_id FROM order_line"+tenantId
			+ " WHERE ol_w_id = ? AND ol_d_id = ? AND ol_o_id < ? AND ol_o_id >= (? - 20)\");\n"
			+ "	public long run(int ol_w_id, int ol_d_id, int ol_o_id, int ol_o_id1) throws VoltAbortException {\n"
			+ "		voltQueueSQL(sql,ol_w_id, ol_d_id, ol_o_id, ol_o_id);\n"
			+ commonCode0 + "\n"
			+ "	}\n}");
				out.flush();out.close();
				
				fstream = new FileWriter(path+"Procedure18_"+tenantId+".java", false);
				out = new BufferedWriter(fstream);
				out.write("import org.voltdb.*;\n"
						+ "public class Procedure18_"+tenantId+" extends VoltProcedure {\n"
						+ "	public final SQLStmt sql = new SQLStmt(\"SELECT count(*) FROM stock"+tenantId
			+ " WHERE s_i_id = ? AND s_w_id = ? AND s_quantity < ?\");\n"
			+ "	public long run(int s_i_id, int s_w_id, int s_quantity) throws VoltAbortException{\n"
			+ "		voltQueueSQL(sql, s_i_id, s_w_id, s_quantity);\n"
			+ commonCode0
			+ "\n	}\n}");
				out.flush();out.close();
				
				fstream = new FileWriter(path+"Procedure19_"+tenantId+".java", false);
				out = new BufferedWriter(fstream);
				out.write("import org.voltdb.*;\n"
						+ "public class Procedure19_"+tenantId+" extends VoltProcedure {\n"
						+ "	public final SQLStmt sql = new SQLStmt(\"SELECT c_discount, c_last, c_credit, w_tax FROM customer"+tenantId+", warehouse"+tenantId
			+ " WHERE w_id = ? AND c_w_id = w_id AND c_d_id = ? AND c_id = ?\");\n"
			+ "	public long run(int w_id, int c_d_id, int c_id) throws VoltAbortException{\n"
			+ "		voltQueueSQL(sql, w_id, c_d_id, c_id);\n"
			+ commonCode0
			+ "\n	}\n}");
				out.flush();out.close();
				
				fstream = new FileWriter(path+"Procedure20_"+tenantId+".java", false);
				out = new BufferedWriter(fstream);
				out.write("import org.voltdb.*;\n"
						+ "public class Procedure20_"+tenantId+" extends VoltProcedure {\n"
						+ "	public final SQLStmt sql = new SQLStmt(\"SELECT o_id, o_entry_d, COALESCE(o_carrier_id,0) FROM orders"+tenantId
			+ " WHERE o_w_id = ? AND o_d_id = ? AND o_c_id = ? AND o_id = ?\");\n"
			+ "	public final SQLStmt sql0 = new SQLStmt(\"SELECT MAX(o_id) FROM orders"+tenantId
			+ " WHERE o_w_id = ? AND o_d_id = ? AND o_c_id = ?\");\n"
			+ "	public long run(int o_w_id, int o_d_id, int o_c_id, int o_w_id1, int o_d_id1, int o_c_id1) throws VoltAbortException{\n"
			+ "		voltQueueSQL(sql0, o_w_id1, o_d_id1, o_c_id1);\n"
			+ "		VoltTable[] results = voltExecuteSQL();\n"
			+ "		if(results[0].getRowCount() == 0)\n"
			+ "			return 0;\n"
			+ "		int o_id = (int) results[0].fetchRow(0).getLong(0);\n"
			+ "		voltQueueSQL(sql, o_w_id, o_d_id, o_c_id, o_id);\n"
			+ "		results = voltExecuteSQL();\n"
			+ "		if(results[0].getRowCount() == 0)\n"
			+ "			return 0;\n"
			+ "		return 1;\n	}\n}");
				out.flush();out.close();
				
				fstream = new FileWriter(path+"Procedure21_"+tenantId+".java", false);
				out = new BufferedWriter(fstream);
				out.write("import org.voltdb.*;\n"
						+ "import org.voltdb.types.TimestampType;\n"
						+ "public class Procedure21_"+tenantId+" extends VoltProcedure {\n"
						+ "	public final SQLStmt sql = new SQLStmt(\"INSERT INTO orders"+tenantId+" (o_id, o_d_id, o_w_id, o_c_id, o_entry_d, o_ol_cnt, o_all_local, is_insert, is_update) "
					+ "VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)\");\n"
					+ "	public long run(int o_id, int o_d_id, int o_w_id, int o_c_id,\n"
					+ "			String o_entry_d, int o_ol_cnt, int o_all_local)\n"
					+ "			throws VoltAbortException {\n"
					+ "		voltQueueSQL(sql, o_id, o_d_id, o_w_id, o_c_id, new TimestampType(o_entry_d), o_ol_cnt, o_all_local, 1, 0);\n"
					+ commonCode1
					+ "\n	}\n}");
				out.flush();out.close();
				
				fstream = new FileWriter(path+"Procedure22_"+tenantId+".java", false);
				out = new BufferedWriter(fstream);
				out.write("import org.voltdb.*;\n"
						+ "public class Procedure22_"+tenantId+" extends VoltProcedure {\n"
						+ "	public final SQLStmt sql = new SQLStmt(\"INSERT INTO new_orders"+tenantId+" (no_d_id, no_o_id, no_w_id, is_insert, is_update) "
			+ "VALUES (?,?,?,?,?)\");\n"
			+ "	public long run(int no_d_id, int no_o_id, int no_w_id) throws VoltAbortException{\n"
			+ "		voltQueueSQL(sql, no_d_id, no_o_id, no_w_id, 1, 0);\n"
			+ commonCode1
			+ "\n	}\n}");
				out.flush();out.close();
				
				fstream = new FileWriter(path+"Procedure23_"+tenantId+".java", false);
				out = new BufferedWriter(fstream);
				out.write("import org.voltdb.*;\n"
						+ "public class Procedure23_"+tenantId+" extends VoltProcedure {\n"
						+ "	public final SQLStmt sql = new SQLStmt(\"INSERT INTO order_line"+tenantId+" (ol_w_id, ol_d_id, ol_o_id, ol_number, ol_i_id, "
			+ "ol_supply_w_id, ol_quantity, ol_amount, ol_dist_info, is_insert, is_update) "
			+ "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)\");\n"
			+ "	public long run(int ol_w_id, int ol_d_id, int ol_o_id, int ol_number, int ol_i_id,\n"
			+ "			int ol_supply_w_id, int ol_quantity, double ol_amount, String ol_dist_info) throws VoltAbortException{\n"
			+ "		voltQueueSQL(sql, ol_w_id, ol_d_id, ol_o_id, ol_number, ol_i_id,ol_supply_w_id, ol_quantity, ol_amount, ol_dist_info, 1, 0);\n"
			+ commonCode1 
			+ "\n	}\n}");
				out.flush();out.close();
				
				fstream = new FileWriter(path+"Procedure24_"+tenantId+".java", false);
				out = new BufferedWriter(fstream);
				out.write("import org.voltdb.*;\n"
						+ "import org.voltdb.types.TimestampType;\n"
						+ "public class Procedure24_"+tenantId+" extends VoltProcedure {\n"
						+ "	public final SQLStmt sql = new SQLStmt(\"INSERT INTO history"+tenantId+" (h_c_d_id, h_c_w_id, h_c_id, h_d_id, "
					+ "h_w_id, h_date, h_amount, h_data, is_insert, is_update) "
					+ "VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)\");\n"
					+ "	public long run(int h_c_id, int h_c_d_id, int h_c_w_id, int h_d_id,\n"
					+ "			int h_w_id, String h_date, double h_amount, String h_data) throws VoltAbortException {\n"
					+ "		voltQueueSQL(sql, h_c_d_id, h_c_w_id, h_c_id, h_d_id, h_w_id, new TimestampType(h_date), h_amount, h_data, 1, 0);\n"
					+ commonCode1
					+ "\n	}\n}");
				out.flush();out.close();
				
				fstream = new FileWriter(path+"Procedure25_"+tenantId+".java", false);
				out = new BufferedWriter(fstream);
				out.write("import org.voltdb.*;\n"
						+ "public class Procedure25_"+tenantId+" extends VoltProcedure {\n"
						+ "	public final SQLStmt sql = new SQLStmt(\"UPDATE district"+tenantId+" SET d_next_o_id = d_next_o_id + 1 , is_update = 1 "
			+ "WHERE d_id = ? AND d_w_id = ?\");\n"
			+ "	public long run(int d_id, int d_w_id) throws VoltAbortException{\n"
			+ "		voltQueueSQL(sql, d_id, d_w_id);\n"
			+ commonCode2
			+ "\n	}\n}");
				out.flush();out.close();
				
				fstream = new FileWriter(path+"Procedure26_"+tenantId+".java", false);
				out = new BufferedWriter(fstream);
				out.write("import org.voltdb.*;\n"
						+ "public class Procedure26_"+tenantId+" extends VoltProcedure {\n"
						+ "	public final SQLStmt sql = new SQLStmt(\"UPDATE stock"+tenantId+" SET s_quantity = ? , is_update = 1 "
			+ "WHERE s_i_id = ? AND s_w_id = ?\");\n"
			+ "	public long run(int s_i_id, int s_quantity, int s_w_id) throws VoltAbortException{\n"
			+ "		voltQueueSQL(sql,s_quantity, s_i_id, s_w_id);\n"
			+ commonCode2
			+ "\n	}\n}");
				out.flush();out.close();
				
				fstream = new FileWriter(path+"Procedure27_"+tenantId+".java", false);
				out = new BufferedWriter(fstream);
				out.write("import java.math.BigDecimal;\n"
						+ "import org.voltdb.*;\n"
						+ "public class Procedure27_"+tenantId+" extends VoltProcedure {\n"
						+ "	public final SQLStmt sql = new SQLStmt(\"UPDATE warehouse"+tenantId+" SET w_ytd = w_ytd + ? , is_update = 1 WHERE w_id = ?\");\n"
						+ "	public long run(int w_id, double h_amount) throws VoltAbortException{\n"
						+ "		voltQueueSQL(sql, new BigDecimal(h_amount), w_id);\n"
						+ commonCode2
						+ "\n	}\n}");
				out.flush();out.close();
				
				fstream = new FileWriter(path+"Procedure28_"+tenantId+".java", false);
				out = new BufferedWriter(fstream);
				out.write("import java.math.BigDecimal;\n"
						+ "import org.voltdb.*;\n"
						+ "public class Procedure28_"+tenantId+" extends VoltProcedure {\n"
						+ "	public final SQLStmt sql = new SQLStmt(\"UPDATE district"+tenantId+" SET d_ytd = d_ytd + ? , is_update = 1 "
			+ "WHERE d_w_id = ? AND d_id = ?\");\n"
			+ "	public long run(int d_id, double h_amount, int d_w_id) throws VoltAbortException{\n"
			+ "		voltQueueSQL(sql, new BigDecimal(h_amount), d_w_id, d_id);\n"
			+ commonCode2
			+ "\n	}\n}");
				out.flush();out.close();
				
				fstream = new FileWriter(path+"Procedure29_"+tenantId+".java", false);
				out = new BufferedWriter(fstream);
				out.write("import org.voltdb.*;\n"
						+ "public class Procedure29_"+tenantId+" extends VoltProcedure {\n"
						+ "	public final SQLStmt sql = new SQLStmt(\"UPDATE customer"+tenantId+" SET c_balance = ?, c_data = ? , is_update = 1 "
			+ "WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?\");\n"
			+ "	public long run(int c_id, double c_balance, String c_data, int c_w_id, int c_d_id) throws VoltAbortException {\n"
			+ "		voltQueueSQL(sql, c_balance, c_data, c_w_id, c_d_id, c_id);\n"
			+ commonCode2
			+ "\n	}\n}");
				out.flush();out.close();
				
				fstream = new FileWriter(path+"Procedure30_"+tenantId+".java", false);
				out = new BufferedWriter(fstream);
				out.write("import org.voltdb.*;\n"
						+ "public class Procedure30_"+tenantId+" extends VoltProcedure {\n"
						+ "	public final SQLStmt sql = new SQLStmt(\"UPDATE customer"+tenantId+" SET c_balance = ? , is_update = 1 "
			+ "WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?\");\n"
			+ "	public long run(int c_id, double c_balance, int c_w_id, int c_d_id) throws VoltAbortException{\n"
			+ "		voltQueueSQL(sql, c_balance, c_w_id, c_d_id, c_id);\n"
			+ commonCode2
			+ "\n	}\n}");
				out.flush();out.close();
				
				fstream = new FileWriter(path+"Procedure31_"+tenantId+".java", false);
				out = new BufferedWriter(fstream);
				out.write("import org.voltdb.*;\n"
						+ "public class Procedure31_"+tenantId+" extends VoltProcedure {\n"
						+ "	public final SQLStmt sql = new SQLStmt(\"UPDATE orders"+tenantId+" SET o_carrier_id = ? , is_update = 1 "
			+ "WHERE o_id = ? AND o_d_id = ? AND o_w_id = ?\");\n"
			+ "	public long run(int o_id, int o_carrier_id, int o_d_id, int o_w_id) throws VoltAbortException {\n"
			+ "		voltQueueSQL(sql, o_carrier_id, o_id, o_d_id, o_w_id);\n"
			+ commonCode2
			+ "\n	}\n}");
				out.flush();out.close();
				
				fstream = new FileWriter(path+"Procedure32_"+tenantId+".java", false);
				out = new BufferedWriter(fstream);
				out.write("import org.voltdb.*;\n"
						+ "import org.voltdb.types.TimestampType;\n"
						+ "public class Procedure32_"+tenantId+" extends VoltProcedure {\n"
						+ "	public final SQLStmt sql = new SQLStmt(\"UPDATE order_line"+tenantId+" SET ol_delivery_d = ? , is_update = 1 "
			+ "WHERE ol_o_id = ? AND ol_d_id = ? AND ol_w_id = ?\");\n"
			+ "	public long run(int ol_w_id, String ol_delivery_d, int ol_o_id, int ol_d_id) throws VoltAbortException {\n"
			+ "		voltQueueSQL(sql, new TimestampType(ol_delivery_d), ol_o_id, ol_d_id, ol_w_id);\n"
			+ commonCode2
			+ "\n	}\n}");
				out.flush();out.close();
				
				fstream = new FileWriter(path+"Procedure33_"+tenantId+".java", false);
				out = new BufferedWriter(fstream);
				out.write("import java.math.BigDecimal;\n"
						+ "import java.sql.SQLException;\n"
						+ "import org.voltdb.*;\n"
						+ "public class Procedure33_"+tenantId+" extends VoltProcedure {\n"
						+ "	public final SQLStmt sql = new SQLStmt(\"UPDATE customer"+tenantId+" SET c_balance = c_balance + ? , c_delivery_cnt = c_delivery_cnt + 1 , is_update = 1 "
			+ "WHERE c_id = ? AND c_d_id = ? AND c_w_id = ?\");\n"
			+ "	public long run(int c_id, double c_balance, int c_d_id, int c_w_id) throws VoltAbortException, ClassNotFoundException, SQLException {\n"
			+ "		voltQueueSQL(sql, new BigDecimal(c_balance).setScale(12, BigDecimal.ROUND_HALF_DOWN), c_id, c_d_id, c_w_id);\n"
			+ commonCode2
			+ "\n	}\n}");
				out.flush();out.close();
				
				fstream = new FileWriter(path+"Procedure34_"+tenantId+".java", false);
				out = new BufferedWriter(fstream);
				out.write("import org.voltdb.*;\n"
						+ "public class Procedure34_"+tenantId+" extends VoltProcedure {\n"
						+ "	public final SQLStmt sql = new SQLStmt(\"DELETE FROM new_orders"+tenantId
			+ " WHERE no_o_id = ? AND no_d_id = ? AND no_w_id = ?\");\n"
			+ "	public long run( int no_d_id, int no_o_id, int no_w_id) throws VoltAbortException {\n"
			+ "		voltQueueSQL(sql, no_o_id, no_d_id, no_w_id);\n"
			+ commonCode1
			+ "\n	}\n}");
				out.flush();out.close();
				*/
				fstream = new FileWriter(path+"ProcedureInsertCustomer_"+tenantId+".java", false);
				out = new BufferedWriter(fstream);
				out.write("import org.voltdb.*;\n"
						+ "import org.voltdb.types.TimestampType;\n"
						+ "public class ProcedureInsertCustomer_"+tenantId+" extends VoltProcedure {\n"
						+ "	public final SQLStmt sqlInsert = new SQLStmt(\"INSERT INTO customer"+tenantId+" (c_id, c_d_id, c_w_id ,c_first ,c_middle ,"
			+ "c_last ,c_street_1 ,c_street_2 ,c_city ,c_state ,c_zip , c_phone, c_since, c_credit, c_credit_lim, "
			+ "c_discount, c_balance, c_ytd_payment, c_payment_cnt, c_delivery_cnt, c_data, "
			+ " is_insert, is_update) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)\");\n"
			+ "	public long run(int c_id, int c_d_id, int c_w_id, String c_first, String c_middle,\n"
			+ "			String c_last, String c_street_1, String c_street_2, String c_city, String c_state,\n"
			+ "			String c_zip, String c_phone, TimestampType c_since, String c_credit, int c_credit_lim,\n"
			+ "			double c_discount, double c_balance, double c_ytd_payment, int c_payment_cnt, int c_delivery_cnt,\n"
			+ "			String c_data, int is_insert, int is_update) throws VoltAbortException {\n"
			+ "		voltQueueSQL(sqlInsert, c_id, c_d_id, c_w_id ,c_first ,c_middle ,\n"
			+ "			c_last ,c_street_1 ,c_street_2 ,c_city ,c_state ,c_zip , c_phone, c_since, c_credit, c_credit_lim,\n"
			+ "			c_discount, c_balance, c_ytd_payment, c_payment_cnt,\n"
			+ "			c_delivery_cnt, c_data, is_insert, is_update);\n"
			+ "		VoltTable[] result = voltExecuteSQL();\n"
			+ "		if(result[0].asScalarLong() == 0)\n"
			+ "			return 0;\n"
			+ "		return 1;\n	}\n}");
				out.flush();out.close();
				
				fstream = new FileWriter(path+"ProcedureInsertDistrict_"+tenantId+".java", false);
				out = new BufferedWriter(fstream);
				out.write("import org.voltdb.*;\n"
						+ "public class ProcedureInsertDistrict_"+tenantId+" extends VoltProcedure {\n"
						+ "	public final SQLStmt sqlInsert = new SQLStmt(\"INSERT INTO district"+tenantId+" (d_id, d_w_id, d_name, d_street_1, d_street_2, d_city, d_state, "
			+ "d_zip, d_tax, d_ytd, d_next_o_id, is_insert, is_update) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)\");\n"
			+ "	public long run(int d_id, int d_w_id, String d_name, String d_Street_1, String d_street_2, String d_city, String d_state,\n"
			+ "			String d_zip, double d_tax, double d_ytd, int d_next_o_id, int is_insert, int is_update) throws VoltAbortException {\n"
			+ "		voltQueueSQL(sqlInsert, d_id, d_w_id, d_name, d_Street_1, d_street_2, d_city, d_state,\n"
			+ "			d_zip, d_tax, d_ytd, d_next_o_id, is_insert, is_update);\n"
			+ "		VoltTable[] result = voltExecuteSQL();\n"
			+ "		if(result[0].asScalarLong() == 0)\n"
			+ "			return 0;\n"
			+ "		return 1;\n	}\n}");
				out.flush();out.close();
				
				fstream = new FileWriter(path+"ProcedureInsertHistory_"+tenantId+".java", false);
				out = new BufferedWriter(fstream);
				out.write("import org.voltdb.*;\n"
						+ "public class ProcedureInsertHistory_"+tenantId+" extends VoltProcedure {\n"
						+ "	public final SQLStmt sqlInsert = new SQLStmt(\"INSERT INTO history"+tenantId+" ( h_c_id, h_c_d_id, h_c_w_id, h_d_id, h_w_id, h_date, h_amount, h_data, "
			+ " is_insert, is_update) VALUES (?,?,?,?,?,?,?,?,?,?) \");\n"
			+ "	public long run(int h_c_id, int h_c_d_id, int h_c_w_id, int h_d_id, int h_w_id, String h_date, double h_amount,\n"
			+ "			String h_data, int is_insert, int is_update) throws VoltAbortException {\n"
			+ "		voltQueueSQL(sqlInsert, h_c_id, h_c_d_id, h_c_w_id, h_d_id, h_w_id, h_date, h_amount, h_data, is_insert, is_update);\n"
			+ "		VoltTable[] result = voltExecuteSQL();\n"
			+ "		if(result[0].asScalarLong() == 0)\n"
			+ "			return 0;\n"
			+ "		return 1;\n	}\n}");
				out.flush();out.close();
				
				fstream = new FileWriter(path+"ProcedureInsertItem_"+tenantId+".java", false);
				out = new BufferedWriter(fstream);
				out.write("import org.voltdb.*;\n"
						+ "public class ProcedureInsertItem_"+tenantId+" extends VoltProcedure{\n"
						+ "	public final SQLStmt sqlInsert = new SQLStmt(\"INSERT INTO item"+tenantId+" (i_id, i_im_id, i_name, i_price, i_data, is_insert, is_update) "
			+ "VALUES (?,?,?,?,?,?,?)\");\n"
			+ "	public long run(int i_id, int i_im_id, String i_name, double i_price, String i_data, int is_insert, int is_update) throws VoltAbortException {\n"
			+ "		voltQueueSQL(sqlInsert, i_id, i_im_id, i_name, i_price, i_data, is_insert, is_update);\n"
			+ "		VoltTable[] result = voltExecuteSQL();\n"
			+ "		if(result[0].asScalarLong() == 0)\n"
			+ "			return 0;\n"
			+ "		return 1;\n	}\n}");
				out.flush();out.close();
				
				fstream = new FileWriter(path+"ProcedureInsertNewOrders_"+tenantId+".java", false);
				out = new BufferedWriter(fstream);
				out.write("import org.voltdb.*;\n"
						+ "public class ProcedureInsertNewOrders_"+tenantId+" extends VoltProcedure {\n"
						+ "	public final SQLStmt sqlInsert = new SQLStmt(\"INSERT INTO new_orders"+tenantId+" (no_o_id, no_d_id, no_w_id, is_insert, is_update) "
			+ "VALUES (?,?,?,?,?)\");\n"
			+ "	public long run(int no_o_id, int no_d_id, int no_w_id, int is_insert, int is_update) throws VoltAbortException {\n"
			+ "		voltQueueSQL(sqlInsert, no_o_id, no_d_id, no_w_id, is_insert, is_update);\n"
			+ "		VoltTable[] result = voltExecuteSQL();\n"
			+ "		if(result[0].asScalarLong() == 0)\n"
			+ "			return 0;\n"
			+ "		return 1;\n	}\n}");
				out.flush();out.close();
				
				fstream = new FileWriter(path+"ProcedureInsertOrderLine_"+tenantId+".java", false);
				out = new BufferedWriter(fstream);
				out.write("import org.voltdb.*;\n"
						+ "public class ProcedureInsertOrderLine_"+tenantId+" extends VoltProcedure {\n"
						+ "	public final SQLStmt sqlInsert = new SQLStmt(\"INSERT INTO order_line"+tenantId+" (ol_o_id, ol_d_id,ol_w_id,"
			+ "ol_number,ol_i_id, ol_supply_w_id,ol_delivery_d, ol_quantity, ol_amount, ol_dist_info,"
			+ " is_insert, is_update) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)\");\n"
			+ "	public long run(int ol_o_id, int ol_w_id, int ol_d_id, int ol_number, int ol_i_id,\n"
			+ "			int ol_supply_w_id, String ol_delivery_d, int ol_quantity, double ol_amount, String ol_dist_info,\n"
			+ "			 int is_insert, int is_update) throws VoltAbortException {\n"
			+ "		voltQueueSQL(sqlInsert, ol_o_id, ol_d_id,ol_w_id, ol_number,ol_i_id, ol_supply_w_id,ol_delivery_d,\n"
			+ "				ol_quantity, ol_amount, ol_dist_info, is_insert, is_update);\n"
			+ "		VoltTable[] result = voltExecuteSQL();\n"
			+ "		if(result[0].asScalarLong() == 0)\n"
			+ "			return 0;\n"
			+ "		return 1;\n"
			+ "	}\n}");
				out.flush();out.close();
				
				fstream = new FileWriter(path+"ProcedureInsertOrders_"+tenantId+".java", false);
				out = new BufferedWriter(fstream);
				out.write("import org.voltdb.*;\n"
						+ "public class ProcedureInsertOrders_"+tenantId+" extends VoltProcedure {\n"
						+ "	public final SQLStmt sqlInsert = new SQLStmt(\"INSERT INTO orders"+tenantId+" (o_id, o_d_id, o_w_id, o_c_id, o_entry_d, "
			+ "o_carrier_id, o_ol_cnt, o_all_local, is_insert, is_update) VALUES (?,?,?,?,?,?,?,?,?,?)\");\n"
			+ "	public long run(int o_id, int o_d_id, int o_w_id, int o_c_id, String o_entry_d,\n"
			+ "			int o_carrier_id, int o_ol_cnt, int o_all_local, int is_insert,\n"
			+ "			int is_update) throws VoltAbortException {\n"
			+ "		voltQueueSQL(sqlInsert, o_id, o_d_id, o_w_id, o_c_id, o_entry_d,\n"
			+ "			o_carrier_id, o_ol_cnt, o_all_local, is_insert, is_update);\n"
			+ "		VoltTable[] result = voltExecuteSQL();\n"
			+ "		if(result[0].asScalarLong() == 0)\n"
			+ "			return 0;\n"
			+ "		return 1;\n	}\n}");
				out.flush();out.close();
				
				fstream = new FileWriter(path+"ProcedureInsertStock_"+tenantId+".java", false);
				out = new BufferedWriter(fstream);
				out.write("import org.voltdb.*;\n"
						+ "public class ProcedureInsertStock_"+tenantId+" extends VoltProcedure {\n"
						+ "	public final SQLStmt sqlInsert = new SQLStmt(\"INSERT INTO stock"+tenantId+" (s_i_id, s_w_id, s_quantity, s_dist_01, s_dist_02,s_dist_03,s_dist_04, s_dist_05, s_dist_06, "
			+ "s_dist_07, s_dist_08, s_dist_09, s_dist_10, s_ytd, s_order_cnt, s_remote_cnt,s_data, is_insert, is_update) "
			+ "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)\");\n"
			+ "	public long run(int s_i_id, int s_w_id, int s_quantity, String s_dist_01, String s_dist_02, String s_dist_03,\n"
			+ "			String s_dist_04, String s_dist_05, String s_dist_06, String s_dist_07, String s_dist_08,\n"
			+ "			String s_dist_09, String s_dist_10, double s_ytd, int s_order_cnt, int s_remote_cnt, String s_data,\n"
			+ "			 int is_insert, int is_update) throws VoltAbortException {\n"
			+ "		voltQueueSQL(sqlInsert, s_i_id, s_w_id, s_quantity, s_dist_01, s_dist_02,s_dist_03,s_dist_04, s_dist_05,\n"
			+ "				s_dist_06, s_dist_07, s_dist_08, s_dist_09, s_dist_10, s_ytd, s_order_cnt, s_remote_cnt,s_data,\n"
			+ "				is_insert, is_update);\n"
			+ "		VoltTable[] result = voltExecuteSQL();\n"
			+ "		if(result[0].asScalarLong() == 0)\n"
			+ "			return 0;\n"
			+ "		return 1;\n	}\n}");
				out.flush();out.close();
				
				fstream = new FileWriter(path+"ProcedureInsertWarehouse_"+tenantId+".java", false);
				out = new BufferedWriter(fstream);
				out.write("import org.voltdb.*;\n"
						+ "public class ProcedureInsertWarehouse_"+tenantId+" extends VoltProcedure {\n"
						+ "	public final SQLStmt sqlInsert = new SQLStmt(\"INSERT INTO warehouse"+tenantId+" (w_id, w_name, w_street_1, w_street_2, "
			+ "w_city, w_state, w_zip, w_tax, w_ytd, is_insert, is_update) VALUES(?,?,?,?,?,?,?,?,?,?,?)\");\n"
			+ "	public long run(int w_id, String w_name, String w_street_1, String w_street_2, String w_city,\n"
			+ "			String w_state, String w_zip, double w_tax, double w_ytd, int is_insert, int is_update)\n"
			+ "					throws VoltAbortException {\n"
			+ "		voltQueueSQL(sqlInsert, w_id, w_name, w_street_1, w_street_2,\n"
			+ "			w_city, w_state, w_zip, w_tax, w_ytd, is_insert, is_update);\n"
			+ "		VoltTable[] result = voltExecuteSQL();\n"
			+ "		if(result[0].asScalarLong() == 0)\n"
			+ "			return 0;\n"
			+ "		return 1;\n	}\n}");
				out.flush();out.close();
				
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
