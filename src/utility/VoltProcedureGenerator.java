package utility;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltProcedure.VoltAbortException;

public class VoltProcedureGenerator {
	
	public static void main(String[] args){
		FileWriter fstream = null;
		BufferedWriter out = null;
		String path = "../procedures/";
		int tenantNumber = 1;
		try {
			for(int tenantId = 0; tenantId < tenantNumber; tenantId++){
				fstream = new FileWriter(path+"Procedure0_"+tenantId+".java", false);
				out = new BufferedWriter(fstream);
				out.write("import org.voltdb.*;\n"
						+ "public class Procedure0_"+tenantId+" extends VoltProcedure {\n"
						+ "	public final SQLStmt sql = new SQLStmt(\"SELECT d_next_o_id, d_tax FROM district"+tenantId+" WHERE d_id = ? AND d_w_id = ?\");\n"
						+ "	public long run(int d_id, int d_w_id) throws VoltAbortException {\n"
						+ "		voltQueueSQL(sql, d_id, d_w_id);\n"
						+ "		VoltTable[] results =  voltExecuteSQL();\n"
						+ "		if(results[0].getRowCount() == 0)\n"
						+ "			return 0;\n"
						+ "		return 1;\n"
						+ "	}\n"
						+ "}");
				out.flush();out.close();
				
				fstream = new FileWriter(path+"Procedure1_"+tenantId+".java", false);
				out = new BufferedWriter(fstream);
				out.write("import org.voltdb.*;\n"
						+ "public class Procedure1_"+tenantId+" extends VoltProcedure {\n"
						+ "	public final SQLStmt sql = new SQLStmt(\"SELECT i_price, i_name, i_data FROM item"+tenantId+" WHERE i_id = ?\");\n"
								+ "	public long run(int i_id) throws VoltAbortException {\n"
								+ "		voltQueueSQL(sql, i_id);\n"
								+ "		VoltTable[] results =  voltExecuteSQL();\n"
								+ "		if(results[0].getRowCount() == 0)\n"
								+ "			return 0;\n"
								+ "		return 1;\n"
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
				+ "		voltQueueSQL(sql, s_i_id, s_w_id);\n"
				+ "		VoltTable[] results = voltExecuteSQL();\n"
				+ "		if(results[0].getRowCount() == 0)\n"
				+ "			return 0;\n"
				+ "		return 1;\n	}\n}");
				out.flush();out.close();
				
				fstream = new FileWriter(path+"Procedure3_"+tenantId+".java", false);
				out = new BufferedWriter(fstream);
				out.write("import org.voltdb.*;\n"
						+ "public class Procedure3_"+tenantId+" extends VoltProcedure {\n"
						+ "	public final SQLStmt sql = new SQLStmt(\"SELECT w_street_1, w_street_2, w_city, w_state, w_zip, w_name "
			+ "FROM warehouse"+tenantId+" WHERE w_id = ?\");\n"
			+ "	public long run(int w_id) throws VoltAbortException{\n"
			+ "		voltQueueSQL(sql, w_id);\n"
			+ "		VoltTable[] results = voltExecuteSQL();\n"
			+ "		if(results[0].getRowCount() == 0)\n"
			+ "			return 0;\n"
			+ "		return 1;\n"
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
			+ "		VoltTable[] results = voltExecuteSQL();\n"
			+ "		if(results[0].getRowCount() == 0)\n"
			+ "			return 0;\n"
			+ "		return 1;\n	}\n}");
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
						+ "		return 1;\n	}\n}");
				out.flush();out.close();
				
				fstream = new FileWriter(path+"Procedure6_"+tenantId+".java", false);
				out = new BufferedWriter(fstream);
				out.write("import org.voltdb.*;\n" +
						"public class Procedure6_"+tenantId+" extends VoltProcedure {\n" +
						"	public final SQLStmt sql = new SQLStmt(\"SELECT c_id FROM customer"+tenantId+" "
			+ "WHERE c_w_id = ? AND c_d_id = ? AND c_last = ? ORDER BY c_first\");\n" +
			"	public long run(int c_w_id, int c_d_id, String c_last) throws VoltAbortException {\n" +
			"		voltQueueSQL(sql, c_w_id, c_d_id, c_last);\n" +
			"		VoltTable[] results = voltExecuteSQL();\n" +
			"		if(results[0].getRowCount() == 0)\n" +
			"			return 0;\n" +
			"		return 1;\n	}\n}");
				out.flush();out.close();
				
				fstream = new FileWriter(path+"Procedure7_"+tenantId+".java", false);
				out = new BufferedWriter(fstream);
				out.write("import org.voltdb.*;\n" +
						"public class Procedure7_"+tenantId+" extends VoltProcedure {\n" +
						"	public final SQLStmt sql = new SQLStmt(\"SELECT c_first, c_middle, c_last, c_street_1, c_street_2, c_city, c_state, c_zip, c_phone, c_credit, c_credit_lim, c_discount, c_balance, c_since "
			+ "FROM customer"+tenantId+" WHERE c_id = ? AND c_w_id = ? AND c_d_id = ?\");\n" +
			"	public long run(int c_id, int c_w_id, int c_d_id) throws VoltAbortException {\n" +
			"		voltQueueSQL(sql, c_id, c_w_id, c_d_id);\n" +
			"		VoltTable[] results = voltExecuteSQL();\n" +
			"		if(results[0].getRowCount() == 0)\n" +
			"			return 0;\n" +
			"		return 1;\n	}\n}");
				out.flush();out.close();
				
				fstream = new FileWriter(path+"Procedure8_"+tenantId+".java", false);
				out = new BufferedWriter(fstream);
				out.write("import org.voltdb.*;\n" +
						"public class Procedure8_"+tenantId+" extends VoltProcedure {\n" +
						"	public final SQLStmt sql = new SQLStmt(\"SELECT c_data FROM customer"+tenantId
						+" WHERE c_id = ? AND c_w_id = ? AND c_d_id = ?\");\n" +
						"	public long run(int c_id, int c_w_id, int c_d_id) throws VoltAbortException{\n" +
						"		voltQueueSQL(sql, c_id, c_w_id, c_d_id);\n" +
						"		VoltTable[] results = voltExecuteSQL();\n" +
						"		if(results[0].getRowCount() == 0)\n" +
						"			return 0;\n" +
						"		return 1;\n	}\n}");
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
			"		VoltTable[] results = voltExecuteSQL();\n" +
			"		if(results[0].getRowCount() == 0)\n" +
			"			return 0;\n" +
			"		return 1;\n	}\n}");
				out.flush();out.close();
				
				fstream = new FileWriter(path+"Procedure11_"+tenantId+".java", false);
				out = new BufferedWriter(fstream);
				out.write("import org.voltdb.*;\n" +
						"public class Procedure11_"+tenantId+" extends VoltProcedure {\n" +
						"	public final SQLStmt sql = new SQLStmt(\"SELECT c_balance, c_first, c_middle, c_last FROM customer"+tenantId
			+ " WHERE c_id = ? AND c_w_id = ? AND c_d_id = ?\");\n" +
			"	public long run(int c_id, int c_w_id, int c_d_id) throws VoltAbortException{\n" +
			"		voltQueueSQL(sql, c_id, c_w_id, c_d_id);\n" +
			"		VoltTable[] results = voltExecuteSQL();\n" +
			"		if(results[0].getRowCount() == 0)\n" +
			"			return 0;\n" +
			"		return 1;\n	}\n}");
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
					"		VoltTable[] results = voltExecuteSQL();\n" +
					"		if (results[0].getRowCount() == 0)\n" +
					"			return 0;\n" +
					"		return 1;\n	}\n}");
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
				out.write("");
				out.flush();out.close();
				
				fstream = new FileWriter(path+"Procedure15_"+tenantId+".java", false);
				out = new BufferedWriter(fstream);
				out.write("");
				out.flush();out.close();
				
				fstream = new FileWriter(path+"Procedure16_"+tenantId+".java", false);
				out = new BufferedWriter(fstream);
				out.write("");
				out.flush();out.close();
				
				fstream = new FileWriter(path+"Procedure17_"+tenantId+".java", false);
				out = new BufferedWriter(fstream);
				out.write("");
				out.flush();out.close();
				
				fstream = new FileWriter(path+"Procedure18_"+tenantId+".java", false);
				out = new BufferedWriter(fstream);
				out.write("");
				out.flush();out.close();
				
				fstream = new FileWriter(path+"Procedure19_"+tenantId+".java", false);
				out = new BufferedWriter(fstream);
				out.write("");
				out.flush();out.close();
				
				fstream = new FileWriter(path+"Procedure20_"+tenantId+".java", false);
				out = new BufferedWriter(fstream);
				out.write("");
				out.flush();out.close();
				
				fstream = new FileWriter(path+"Procedure21_"+tenantId+".java", false);
				out = new BufferedWriter(fstream);
				out.write("");
				out.flush();out.close();
				
				fstream = new FileWriter(path+"Procedure22_"+tenantId+".java", false);
				out = new BufferedWriter(fstream);
				out.write("");
				out.flush();out.close();
				
				fstream = new FileWriter(path+"Procedure23_"+tenantId+".java", false);
				out = new BufferedWriter(fstream);
				out.write("");
				out.flush();out.close();
				
				fstream = new FileWriter(path+"Procedure24_"+tenantId+".java", false);
				out = new BufferedWriter(fstream);
				out.write("");
				out.flush();out.close();
				
				fstream = new FileWriter(path+"Procedure25_"+tenantId+".java", false);
				out = new BufferedWriter(fstream);
				out.write("");
				out.flush();out.close();
				
				fstream = new FileWriter(path+"Procedure26_"+tenantId+".java", false);
				out = new BufferedWriter(fstream);
				out.write("");
				out.flush();out.close();
				
				fstream = new FileWriter(path+"Procedure27_"+tenantId+".java", false);
				out = new BufferedWriter(fstream);
				out.write("");
				out.flush();out.close();
				
				fstream = new FileWriter(path+"Procedure28_"+tenantId+".java", false);
				out = new BufferedWriter(fstream);
				out.write("");
				out.flush();out.close();
				
				fstream = new FileWriter(path+"Procedure29_"+tenantId+".java", false);
				out = new BufferedWriter(fstream);
				out.write("");
				out.flush();out.close();
				
				fstream = new FileWriter(path+"Procedure30_"+tenantId+".java", false);
				out = new BufferedWriter(fstream);
				out.write("");
				out.flush();out.close();
				
				fstream = new FileWriter(path+"Procedure31_"+tenantId+".java", false);
				out = new BufferedWriter(fstream);
				out.write("");
				out.flush();out.close();
				
				fstream = new FileWriter(path+"Procedure32_"+tenantId+".java", false);
				out = new BufferedWriter(fstream);
				out.write("");
				out.flush();out.close();
				
				fstream = new FileWriter(path+"Procedure33_"+tenantId+".java", false);
				out = new BufferedWriter(fstream);
				out.write("");
				out.flush();out.close();
				
				fstream = new FileWriter(path+"Procedure34_"+tenantId+".java", false);
				out = new BufferedWriter(fstream);
				out.write("");
				out.flush();out.close();
				
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
