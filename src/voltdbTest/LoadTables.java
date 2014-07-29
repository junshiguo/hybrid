package voltdbTest;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.voltdb.client.Client;
import org.voltdb.client.ProcCallException;

import utility.DBManager;

public class LoadTables {
	public static Connection conn = null;
	public static Client voltdbConn = null;
	public static int tenantNumber;
	
	public static void main(String[] args){
		conn = DBManager.connectDB("jdbc:mysql://10.20.2.211/tpcc10", "root", "root");
		voltdbConn = DBManager.connectVoltdb("10.20.2.211");
		System.out.println("db connected...");
//		tenantNumber = Main.numberOfThread;
		tenantNumber = 100;
		loadData();
	}
	
	public static void loadData(){
		if(conn == null || voltdbConn == null) return;
		int tenantId = 0;
		try {
			Statement stmt = conn.createStatement();
			for(tenantId=1; tenantId<tenantNumber; tenantId++){
				long start = System.nanoTime();
				//********************load warehouse******************//
				ResultSet rs = stmt.executeQuery("SELECT  * FROM warehouse"+tenantId);
				while(rs.next()){
					voltdbConn.callProcedure("WAREHOUSE"+tenantId+".insert", rs.getInt("w_id"), 
							rs.getString("w_name"), rs.getString("w_street_1"), rs.getString("w_street_2"), rs.getString("w_city"), 
							rs.getString("w_state"), rs.getString("w_zip"), rs.getDouble("w_tax"), rs.getDouble("w_ytd"), 
							0, 0);
				}
				//*******************load district************************//
				rs = stmt.executeQuery("SELECT * FROM district"+tenantId);
				while(rs.next()){
					voltdbConn.callProcedure("DISTRICT"+tenantId+".insert", rs.getInt("d_id"), rs.getInt("d_w_id"), 
							rs.getString("d_name"), rs.getString("d_street_1"), rs.getString("d_street_2"), rs.getString("d_city"), 
							rs.getString("d_state"), rs.getString("d_zip"),	rs.getDouble("d_tax"), rs.getDouble("d_ytd"), 
							rs.getInt("d_next_o_id"), 0, 0);
				}
				//******************load customer**************************//
				rs = stmt.executeQuery("SELECT * FROM customer"+tenantId );
				while(rs.next()){
					voltdbConn.callProcedure("CUSTOMER"+tenantId+".insert", rs.getInt("c_id"), rs.getInt("c_d_id"), 
							rs.getInt("c_w_id"), rs.getString("c_first"), rs.getString("c_middle"), rs.getString("c_last"),
							rs.getString("c_street_1"), rs.getString("c_street_2"), rs.getString("c_city"),rs.getString("c_state"),
							rs.getString("c_zip"), rs.getString("c_phone"), rs.getTimestamp("c_since"), 
							rs.getString("c_credit"), rs.getInt("c_credit_lim"), rs.getDouble("c_discount"), rs.getDouble("c_balance"), 
							rs.getDouble("c_ytd_payment"), rs.getInt("c_payment_cnt"), rs.getInt("c_delivery_cnt"), rs.getString("c_data"),
							0, 0);
				}
				//******************load history*****************************//
				rs = stmt.executeQuery("SELECT * FROM history"+tenantId );
				while(rs.next()){
					voltdbConn.callProcedure("HISTORY"+tenantId+".insert", rs.getInt("h_c_id"), rs.getInt("h_c_d_id"), 
							rs.getInt("h_c_w_id"), rs.getInt("h_d_id"), rs.getInt("h_w_id"), 
							rs.getTimestamp("h_date"), rs.getDouble("h_amount"), rs.getString("h_data"),
							0, 0);
				}
				//*******************load new orders*************************//
				rs = stmt.executeQuery("SELECT * FROM new_orders"+tenantId );
				while(rs.next()){
					voltdbConn.callProcedure("NEW_ORDERS"+tenantId+".insert", rs.getInt("no_o_id"), rs.getInt("no_d_id"), 
							rs.getInt("no_w_id"), 0, 0);
				}
				//******************load orders******************************//
				rs = stmt.executeQuery("SELECT * FROM orders"+tenantId );
				while(rs.next()){
					voltdbConn.callProcedure("ORDERS"+tenantId+".insert", rs.getInt("o_id"), rs.getInt("o_d_id"),
							rs.getInt("o_w_id"), rs.getInt("o_c_id"),
							rs.getTimestamp("o_entry_d"), 
							rs.getInt("o_carrier_id"), rs.getInt("o_ol_cnt"), rs.getInt("o_all_local"), 0, 0);
				}
				//***********************load order line********************//
				rs = stmt.executeQuery("SELECT * FROM order_line"+tenantId );
				while(rs.next()){
					voltdbConn.callProcedure("ORDER_LINE"+tenantId+".insert", rs.getInt("ol_o_id"), rs.getInt("ol_d_id"), 
							rs.getInt("ol_w_id"), rs.getInt("ol_number"), rs.getInt("ol_i_id"), rs.getInt("ol_supply_w_id"),
							rs.getTimestamp("ol_delivery_d"), 
							rs.getInt("ol_quantity"), rs.getDouble("ol_amount"), rs.getString("ol_dist_info"), 0, 0);
				}
				//********************load item***************************//
				rs = stmt.executeQuery("SELECT * FROM item"+tenantId );
				while(rs.next()){
					voltdbConn.callProcedure("ITEM"+tenantId+".insert", rs.getInt("i_id"), rs.getInt("i_im_id"), 
							rs.getString("i_name"), rs.getDouble("i_price"), rs.getString("i_data"), 
							0, 0);
				}
				//*********************load stock*************************//
				rs = stmt.executeQuery("SELECT * FROM stock"+tenantId );
				while(rs.next()){
					voltdbConn.callProcedure("STOCK"+tenantId+".insert", rs.getInt("s_i_id"), rs.getInt("s_w_id"),
							rs.getInt("s_quantity"), rs.getString("s_dist_01"), rs.getString("s_dist_02"), rs.getString("s_dist_03"), 
							rs.getString("s_dist_04"), rs.getString("s_dist_05"), rs.getString("s_dist_06"), rs.getString("s_dist_07"), 
							rs.getString("s_dist_08"), rs.getString("s_dist_09"), rs.getString("s_dist_10"), rs.getDouble("s_ytd"), 
							rs.getInt("s_order_cnt"), rs.getInt("s_remote_cnt"), rs.getString("s_data"), 0, 0);
				}
				long end = System.nanoTime();
				System.out.println("Load tables for tenant "+tenantId+" done! Time: "+(end-start)/1000000000.0+" seconds!");
			}
		} catch (SQLException | IOException | ProcCallException e) {
			e.printStackTrace();
		}
	}
	
}
