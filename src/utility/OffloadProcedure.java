package utility;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

public class OffloadProcedure {
	public static void main(String[] args) throws IOException{
		FileWriter fstream = null;
		BufferedWriter out = null;
		String path = "tmp/";
		int tenantNumber = 50;
		
		for(int volumnId = 0; volumnId < tenantNumber; volumnId++){
			for(int tableId = 0; tableId < 9; tableId++){
				fstream = new FileWriter(path+"Offload"+Table[tableId]+"_"+volumnId+".java", false);
				out = new BufferedWriter(fstream);
				out.write(procedure(tableId, volumnId));
				out.flush(); out.close();
			}
		}
	}
	
	public static int[] attrN = {24, 14, 8, 6, 13, 11, 20, 12, 11};
	public static String[] table = {"customer", "district", "item", "new_orders", "order_line", "orders", "stock", "warehouse", "history"};
	public static String[] Table = {"Customer", "District", "Item", "NewOrders", "OrderLine", "Orders", "Stock", "Warehouse", "History"};
	
	public static String procedure(int tableId, int volumnId){
		String ret = "import org.voltdb.*;\n"
				+ "import org.voltdb.types.TimestampType;\n"
				+ "public class Offload"+Table[tableId]+"_"+volumnId+" extends VoltProcedure {\n"
				+ "	public final SQLStmt sql = new SQLStmt(\""+sql(table[tableId], volumnId, attrN[tableId])+"\");\n"
						+ "	public VoltTable[] run(int tenantId, String[] lines, int length) throws VoltAbortException {\n"
						+ "		String[] values;"
						+ "		for(int i = 0; i < length; i++){\n"
						+ "			values = lines[i].split(\",\");"
						+ "			voltQueueSQL(sql, "+valueContent(tableId)+");\n"
								+ "		}\n		voltExecuteSQL();\n		return null;\n	}\n}";
		return ret;
	}
	
	public static String sql(String table, int volumnId, int n){
		String ret = "insert into "+table+volumnId+" values (?";
		for(int i = 1; i < n; i++){
			ret += ",?";
		}
		ret += ");";
		return ret;
	}
	
	public final static int CUSTOMER = 0;
	public final static int DISTRICT = 1;
	public final static int ITEM = 2;
	public final static int NEW_ORDERS = 3;
	public final static int ORDER_LINE = 4;
	public final static int ORDERS = 5;
	public final static int STOCK = 6;
	public final static int WAREHOUSE = 7;
	public final static int HISTORY = 8;
	public static String valueContent(int table){
		String ret = "";
		switch(table){
		case CUSTOMER:
			ret = "Integer.parseInt(values[0]), Integer.parseInt(values[1]), Integer.parseInt(values[2]), values[3], values[4],\n"
					+ "					values[5], values[6], values[7], values[8], values[9],\n"
					+ "					values[10], values[11], new TimestampType(values[12]), values[13], Integer.parseInt(values[14]),\n"
					+ "					Double.parseDouble(values[15]), Double.parseDouble(values[16]), Double.parseDouble(values[17]), Integer.parseInt(values[18]), Integer.parseInt(values[19]),\n"
					+ "					values[20], tenantId, 0, 0";
			break;
		case DISTRICT:
//			ret = "rs.getInt(\"d_id\"), rs.getInt(\"d_w_id\"), \n"
//					+ "						rs.getString(\"d_name\"), rs.getString(\"d_street_1\"), rs.getString(\"d_street_2\"), rs.getString(\"d_city\"),\n"
//					+ "						rs.getString(\"d_state\"), rs.getString(\"d_zip\"),	rs.getDouble(\"d_tax\"), rs.getDouble(\"d_ytd\"),\n"
//					+ "						rs.getInt(\"d_next_o_id\"), tenantId, 0, 0";
			ret = "Integer.parseInt(values[0]), Integer.parseInt(values[1]), values[2], values[3], values[4],\n"
					+ "values[5], values[6], values[7], Double.parseDouble(values[8]), Double.parseDouble(values[9]),"
					+ "Integer.parseInt(values[10]), tenantId, 0, 0";
			break;
		case ITEM:
//			ret = "rs.getInt(\"i_id\"), rs.getInt(\"i_im_id\"), \n"
//					+ "							rs.getString(\"i_name\"), rs.getDouble(\"i_price\"), rs.getString(\"i_data\"), \n"
//					+ "							tenantId, 0, 0";
			ret = "Integer.parseInt(values[0]), Integer.parseInt(values[1]), values[2], Double.parseDouble(values[3]), values[4],"
					+ "tenantId, 0, 0";
			break;
		case NEW_ORDERS:
//			ret = "rs.getInt(\"no_o_id\"), rs.getInt(\"no_d_id\"), \n"
//					+ "					rs.getInt(\"no_w_id\"), tenantId, 0, 0";
			ret = "Integer.parseInt(values[0]), Integer.parseInt(values[1]), Integer.parseInt(values[2]), tenantId, 0, 0";
			break;
		case ORDER_LINE:
//			ret = "rs.getInt(\"ol_o_id\"), rs.getInt(\"ol_d_id\"), \n"
//					+ "							rs.getInt(\"ol_w_id\"), rs.getInt(\"ol_number\"), rs.getInt(\"ol_i_id\"), rs.getInt(\"ol_supply_w_id\"), rs.getTimestamp(\"ol_delivery_d\"),\n"
//					+ "							rs.getInt(\"ol_quantity\"), rs.getDouble(\"ol_amount\"), rs.getString(\"ol_dist_info\"), tenantId, 0, 0";
			ret = "Integer.parseInt(values[0]), Integer.parseInt(values[1]), Integer.parseInt(values[2]), Integer.parseInt(values[3]), Integer.parseInt(values[4]),\n"
					+ "					Integer.parseInt(values[5]), new TimestampType(values[6]), Integer.parseInt(values[7]), Double.parseDouble(values[8]), values[9],\n"
					+ "					tenantId, 0, 0";
			break;
		case ORDERS:
//			ret = "rs.getInt(\"o_id\"), rs.getInt(\"o_d_id\"),\n"
//					+ "							rs.getInt(\"o_w_id\"), rs.getInt(\"o_c_id\"),	rs.getTimestamp(\"o_entry_d\"),\n"
//					+ "							rs.getInt(\"o_carrier_id\"), rs.getInt(\"o_ol_cnt\"), rs.getInt(\"o_all_local\"), tenantId, 0, 0";
			ret = "Integer.parseInt(values[0]), Integer.parseInt(values[1]), Integer.parseInt(values[2]), Integer.parseInt(values[3]), new TimestampType(values[4]),\n"
					+ "					Integer.parseInt(values[5]), Integer.parseInt(values[6]), Integer.parseInt(values[7]), tenantId, 0, 0";
			break;
		case STOCK:
//			ret = "rs.getInt(\"s_i_id\"), rs.getInt(\"s_w_id\"),\n"
//					+ "							rs.getInt(\"s_quantity\"), rs.getString(\"s_dist_01\"), rs.getString(\"s_dist_02\"), rs.getString(\"s_dist_03\"),\n"
//					+ "							rs.getString(\"s_dist_04\"), rs.getString(\"s_dist_05\"), rs.getString(\"s_dist_06\"), rs.getString(\"s_dist_07\"),\n"
//					+ "							rs.getString(\"s_dist_08\"), rs.getString(\"s_dist_09\"), rs.getString(\"s_dist_10\"), rs.getDouble(\"s_ytd\"),\n"
//					+ "							rs.getInt(\"s_order_cnt\"), rs.getInt(\"s_remote_cnt\"), rs.getString(\"s_data\"), tenantId, 0, 0";
			ret = "Integer.parseInt(values[0]), Integer.parseInt(values[1]), Integer.parseInt(values[2]), values[3], values[4],\n"
					+ "					values[5], values[6], values[7], values[8], values[9],\n"
					+ "					values[10], values[11], values[12], Double.parseDouble(values[13]), Integer.parseInt(values[14]),\n"
					+ "					Integer.parseInt(values[15]), values[16], tenantId, 0, 0";
			break;
		case WAREHOUSE:
//			ret = "rs.getInt(\"w_id\"), rs.getString(\"w_name\"), rs.getString(\"w_street_1\"), rs.getString(\"w_street_2\"), rs.getString(\"w_city\"),\n"
//					+ "						rs.getString(\"w_state\"), rs.getString(\"w_zip\"), rs.getDouble(\"w_tax\"), rs.getDouble(\"w_ytd\"),\n"
//					+ "						tenantId, 0, 0";
			ret = "Integer.parseInt(values[0]), values[1], values[2], values[3], values[4],\n"
					+ "					values[5], values[6], Double.parseDouble(values[7]), Double.parseDouble(values[8]), tenantId,"
					+ "0, 0";
			break;
		case HISTORY:
//			ret = "rs.getInt(\"h_c_id\"), rs.getInt(\"h_c_d_id\"), rs.getInt(\"h_c_w_id\"), rs.getInt(\"h_d_id\"), rs.getInt(\"h_w_id\"),\n"
//					+ "							rs.getTimestamp(\"h_date\"), rs.getDouble(\"h_amount\"), rs.getString(\"h_data\"),\n"
//					+ "							tenantId, 0, 0";
			ret = "Integer.parseInt(values[0]), Integer.parseInt(values[1]), Integer.parseInt(values[2]), Integer.parseInt(values[3]), Integer.parseInt(values[4]),\n"
					+ "					new TimestampType(values[5]), Double.parseDouble(values[6]), values[7], tenantId, 0, 0";
			break;
			default:
		}
		return ret;
	}
	
}
