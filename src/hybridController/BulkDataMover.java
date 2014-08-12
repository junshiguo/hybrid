package hybridController;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import org.voltdb.client.Client;
import org.voltdb.client.ProcCallException;

import utility.DBManager;

public class BulkDataMover {
	public static String voltdbServer = "127.0.0.1";
	public static String dbUrl = "jdbc:mysql://127.0.0.1/tpcc3000";
	public static String dbUsername = "remote";
	public static String dbPassword = "remote";
	
	public static void main(String[] args) throws SQLException, IOException, InterruptedException, ProcCallException{
		int tenantId = 1222;
		if(args.length > 0){
			tenantId = Integer.parseInt(args[0]);
		}
		long start = System.nanoTime();
		new BulkDataMover().Mysql2Voltdb(tenantId, 0);
		long end = System.nanoTime();
		System.out.println("MySQL ---> VoltDB time: "+(end-start)/1000000000.0+" seconds!");
	}
	
	public void Mysql2Voltdb(int tenantId, int volumnId) throws SQLException, IOException, InterruptedException, ProcCallException{
		Client client = DBManager.connectVoltdb(voltdbServer);
		Connection conn = DBManager.connectDB(dbUrl, dbUsername, dbPassword);
		Statement stmt = conn.createStatement();
		String[] fileName = {"customer"+tenantId+".csv", "district"+tenantId+".csv", "history"+tenantId+".csv", "item"+tenantId+".csv", "new_orders"+tenantId+".csv",
				"order_line"+tenantId+".csv", "orders"+tenantId+".csv", "stock"+tenantId+".csv", "warehouse"+tenantId+".csv"
		};
		String[] tables = {"customer", "district", "history", "item", "new_orders", "order_line", "orders", "stock", "warehouse"};
		for(int i = 0; i < 9; i++){
			try{
				Files.delete(Paths.get("/tmp/hybrid/"+fileName[i]));
			}catch(Exception e){}
		}
		String[] sql = new String[9];
		sql[0] = "select concat(c_id, ',', c_d_id, ',',c_w_id, ',', c_first, ',', c_middle, ',', c_last, ',', c_street_1, ',', c_street_2, ',',c_city, ',',c_state, ',',c_zip, ',', c_phone, ',',c_since, ',', c_credit, ',', c_credit_lim , ',', c_discount, ',', c_balance, ',', c_ytd_payment, ',',c_payment_cnt, ',', c_delivery_cnt, ',', c_data, ',','"+tenantId+"', ',', '0', ',', '0')"
				+ " from customer"+tenantId+" into outfile '/tmp/hybrid/customer"+tenantId+".csv'";
		sql[1] = "select concat(d_id, ',', d_w_id, ',', d_name, ',', d_street_1, ',', d_street_2, ',', d_city, ',', d_state, ',', d_zip, ',', d_tax, ',', d_ytd, ',', d_next_o_id, ',', '"+tenantId+"', ',', '0', ',', '0') "
				+ "from district"+tenantId+" into outfile '/tmp/hybrid/district"+tenantId+".csv'";
		sql[2] = "select concat(h_c_id , ',', h_c_d_id, ',', h_c_w_id, ',',h_d_id, ',',h_w_id, ',',h_date, ',',h_amount, ',',h_data, ',', '"+tenantId+"', ',', '0', ',', '0' ) "
				+ "from history"+tenantId+" into outfile '/tmp/hybrid/history"+tenantId+".csv'";
		sql[3] = "select concat(i_id, ',', i_im_id, ',', i_name, ',', i_price, ',', i_data, ',', '"+tenantId+"', ',', '0', ',', '0') "
				+ "from item"+tenantId+" into outfile '/tmp/hybrid/item"+tenantId+".csv'";
		sql[4] = "select concat(no_o_id, ',',no_d_id, ',',no_w_id, ',', '"+tenantId+"', ',', '0', ',', '0') "
				+ "from new_orders"+tenantId+" into outfile '/tmp/hybrid/new_orders"+tenantId+".csv'";
		sql[5] = "select concat(ol_o_id, ',', ol_d_id, ',',ol_w_id, ',',ol_number, ',',ol_i_id, ',', ol_supply_w_id, ',',ol_delivery_d, ',', ol_quantity, ',', ol_amount, ',', ol_dist_info, ',', '"+tenantId+"', ',', '0', ',', '0') "
				+ "from order_line"+tenantId+" into outfile '/tmp/hybrid/order_line"+tenantId+".csv'";
		sql[6] = "select concat(o_id, ',', o_d_id, ',', o_w_id, ',', o_c_id, ',', o_entry_d, ',', o_carrier_id, ',', o_ol_cnt, ',', o_all_local, ',', '"+tenantId+"', ',', '0', ',', '0') "
				+ "from orders"+tenantId+" into outfile '/tmp/hybrid/orders"+tenantId+".csv'";
		sql[7] = "select concat(s_i_id, ',', s_w_id, ',', s_quantity, ',', s_dist_01, ',', s_dist_02, ',',s_dist_03, ',',s_dist_04, ',', s_dist_05, ',', s_dist_06, ',', s_dist_07, ',', s_dist_08, ',', s_dist_09, ',', s_dist_10, ',', s_ytd, ',', s_order_cnt, ',', s_remote_cnt, ',',s_data, ',', '"+tenantId+"', ',', '0', ',', '0') "
				+ "from stock"+tenantId+" into outfile '/tmp/hybrid/stock"+tenantId+".csv'";
		sql[8] = "select concat(w_id, ',',	w_name, ',',w_street_1, ',',w_street_2, ',',w_city, ',',w_state, ',',w_zip, ',',w_tax, ',',	w_ytd, ',', '"+tenantId+"', ',', '0', ',', '0') "
				+ "from warehouse"+tenantId+" into outfile '/tmp/hybrid/warehouse"+tenantId+".csv'";
		Process[] pr = new Process[9];
		for(int i = 0; i < 9; i++){
			stmt.execute(sql[i]);
			client.callProcedure("@AdHoc", "delete from "+tables[i]+volumnId+" where tenant_id = "+tenantId);
			pr[i] = Runtime.getRuntime().exec(getLoader(tables[i], tenantId, volumnId));
			pr[i].waitFor();
			System.out.println(tables[i]+" offloadding finished");
		}
//		for(int i = 0; i < 9; i++){
//			pr[i].waitFor();
//		}
		stmt.close();
		conn.close();
	}
	
	public String[] getLoader(String table, int tid, int vid){
		String[] ret = {"/bin/sh", "-c", "/usr/voltdb/bin/csvloader "+table+vid+" -f /tmp/hybrid/"+table+tid+".csv -r /tmp/hybrid/tmp"};
		return ret;
	}

}
