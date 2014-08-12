package hybridController;

import hybridConfig.HConfig;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.voltdb.client.Client;
import org.voltdb.client.NoConnectionsException;
import org.voltdb.client.ProcCallException;

import retrivers.*;
import utility.DBManager;

public class DataMover extends Thread {
	public Connection conn;
	public Client voltdbConn;
	public String dbURL = "jdbc:mysql://127.0.0.1/tpcc3000";
	public String dbUsername = "remote";
	public String dbPassword = "remote";
	public String voltdbServer = "127.0.0.1";
	public int tenantId;
	public boolean isM2V;
	
	public static void main(String[] args){
		boolean M2V = true;
		if(args.length > 0){
			int tmp = Integer.parseInt(args[0]);
			if(tmp == 0) M2V = false;
			else M2V = true;
		}
		new DataMover("jdbc:mysql://127.0.0.1/test", "remote", "remote", "127.0.0.1", 0, M2V).start();
	}

	public DataMover(String url, String username, String password, String voltdbServer, int tenantId, boolean isM2V){
		this.dbURL = url;
		this.dbUsername = username;
		this.dbPassword = password;
		this.voltdbServer = voltdbServer;
		this.tenantId = tenantId;
		this.isM2V = isM2V;
		conn = DBManager.connectDB(url, username, password);
		voltdbConn = DBManager.connectVoltdb(voltdbServer);
	}
	
	public void run(){
		if(isM2V){
			int emptyVolumn = VMMatch.findVolumn();
//			emptyVolumn = 1;
			if(emptyVolumn != -1){
//				HybridController.sendTask[HConfig.getType(tenantId)].sendInfo(tenantId, 1, 1, emptyVolumn);
				long start = System.nanoTime();
				VMMatch.addMatch(emptyVolumn, tenantId);
				try {
					this.Mysql2VoltdbBulk(tenantId, emptyVolumn);
				} catch (SQLException | IOException | InterruptedException
						| ProcCallException e) {
					e.printStackTrace();
				}
				long end = System.nanoTime();
				System.out.println("Tenant "+tenantId+" MySQL ---> VoltDB! Time spent: "+(end-start)/1000000000.0+" seconds!");
				HybridController.sendTask[HConfig.getType(tenantId)].sendInfo(tenantId, 1, 0, emptyVolumn);
			}
		}else{
			int volumnId = VMMatch.findTenant(tenantId);
//			volumnId = 1;
			if(volumnId != -1){
				HybridController.sendTask[HConfig.getType(tenantId)].sendInfo(tenantId, 1, 1, volumnId);
				long start = System.nanoTime();
				this.Voltdb2Mysql(tenantId, volumnId);
				VMMatch.deleteMatch(volumnId, tenantId);
				long end = System.nanoTime();
				System.out.println("Tenant "+tenantId+" VoltDB ---> MySQL! Time spent: "+(end-start)/1000000000.0+" seconds!");
				HybridController.sendTask[HConfig.getType(tenantId)].sendInfo(tenantId, 0, 0, -1);
			}
		}
	}
	
	public void Mysql2VoltdbBulk(int tenantId, int volumnId) throws SQLException, IOException, InterruptedException, ProcCallException{
		Client client = DBManager.connectVoltdb(voltdbServer);
		Connection conn = DBManager.connectDB(dbURL, dbUsername, dbPassword);
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
//			pr[i].waitFor();
		}
		for(int i = 0; i < 9; i++){
			pr[i].waitFor();
		}
		stmt.close();
		conn.close();
	}
	
	public String[] getLoader(String table, int tid, int vid){
		String[] ret = {"/bin/sh", "-c", "/usr/voltdb/bin/csvloader "+table+vid+" -f /tmp/hybrid/"+table+tid+".csv -r /tmp/hybrid/tmp"};
		return ret;
	}

	
	public void Mysql2VoltdbThreading(int tenantId, int volumnId){
		OffloadingThread[] offloading = new OffloadingThread[9];
		for(int i = 0; i < 9; i++){
			offloading[i] = new OffloadingThread(i, tenantId, volumnId, this.dbURL, this.dbUsername, this.dbPassword, this.voltdbServer);
			offloading[i].start();
		}
		try{
			for(int i = 0; i < 9; i++){
				offloading[i].join();
			}
		}catch(Exception e){}
	}
	
	public void Mysql2Voltdb(int tenantId, int volumnId) throws SQLException, NoConnectionsException, IOException, ProcCallException{
		if(conn == null){
			conn = DBManager.connectDB(this.dbURL, this.dbUsername, this.dbPassword);
		}
		if(voltdbConn == null){
			voltdbConn = DBManager.connectVoltdb(this.voltdbServer);
		}
		Statement stmt = conn.createStatement();
//		ClientResponse response = null;
		//********************load warehouse******************//
		ResultSet rs = stmt.executeQuery("SELECT  * FROM warehouse"+tenantId);
		while(rs.next()){
//			response = voltdbConn.callProcedure("WAREHOUSE"+volumnId+".select", rs.getInt("w_id"), tenantId);
//			if(response.getStatus() == ClientResponse.SUCCESS && response.getResults()[0].getRowCount() != 0){
//				continue;
//			}
			try{
			voltdbConn.callProcedure("WAREHOUSE"+volumnId+".insert", rs.getInt("w_id"), 
					rs.getString("w_name"), rs.getString("w_street_1"), rs.getString("w_street_2"), rs.getString("w_city"), 
					rs.getString("w_state"), rs.getString("w_zip"), rs.getDouble("w_tax"), rs.getDouble("w_ytd"), 
					tenantId, 0, 0);
			}catch(Exception e){}
		}
		//*******************load district************************//
		rs = stmt.executeQuery("SELECT * FROM district"+tenantId);
		while(rs.next()){
//			response = voltdbConn.callProcedure("DISTRICT"+volumnId+".select", rs.getInt("d_w_id"), rs.getInt("d_id"), tenantId);
//			if(response.getStatus() == ClientResponse.SUCCESS && response.getResults()[0].getRowCount() != 0){
//				continue;
//			}
			try{
			voltdbConn.callProcedure("DISTRICT"+volumnId+".insert", rs.getInt("d_id"), rs.getInt("d_w_id"), 
					rs.getString("d_name"), rs.getString("d_street_1"), rs.getString("d_street_2"), rs.getString("d_city"), 
					rs.getString("d_state"), rs.getString("d_zip"),	rs.getDouble("d_tax"), rs.getDouble("d_ytd"), 
					rs.getInt("d_next_o_id"), tenantId, 0, 0);
			}catch(Exception e){}
		}
		//******************load customer**************************//
		rs = stmt.executeQuery("SELECT * FROM customer"+tenantId );
		while(rs.next()){
//			response = voltdbConn.callProcedure("CUSTOMER"+volumnId+".select", rs.getInt("c_id"), rs.getInt("c_w_id"), rs.getInt("c_d_id"), tenantId);
//			if(response.getStatus() == ClientResponse.SUCCESS && response.getResults()[0].getRowCount() != 0){
//				continue;
//			}
			try{
			voltdbConn.callProcedure("CUSTOMER"+volumnId+".insert", rs.getInt("c_id"), rs.getInt("c_d_id"), 
					rs.getInt("c_w_id"), rs.getString("c_first"), rs.getString("c_middle"), rs.getString("c_last"),
					rs.getString("c_street_1"), rs.getString("c_street_2"), rs.getString("c_city"),rs.getString("c_state"),
					rs.getString("c_zip"), rs.getString("c_phone"), rs.getTimestamp("c_since"), 
					rs.getString("c_credit"), rs.getInt("c_credit_lim"), rs.getDouble("c_discount"), rs.getDouble("c_balance"), 
					rs.getDouble("c_ytd_payment"), rs.getInt("c_payment_cnt"), rs.getInt("c_delivery_cnt"), rs.getString("c_data"),
					tenantId, 0, 0);
			}catch(Exception e){}
		}
		//******************load history*****************************//
		rs = stmt.executeQuery("SELECT * FROM history"+tenantId );
		while(rs.next()){ 
//			response = voltdbConn.callProcedure("HISTORY"+volumnId+".select", rs.getInt("h_c_id"), rs.getInt("h_c_d_id"), rs.getInt("h_c_w_id"), tenantId);
//			if(response.getStatus() == ClientResponse.SUCCESS && response.getResults()[0].getRowCount() != 0){
//				continue;
//			}
			try{
			voltdbConn.callProcedure("HISTORY"+volumnId+".insert", rs.getInt("h_c_id"), rs.getInt("h_c_d_id"), 
					rs.getInt("h_c_w_id"), rs.getInt("h_d_id"), rs.getInt("h_w_id"), 
					rs.getTimestamp("h_date"), rs.getDouble("h_amount"), rs.getString("h_data"),
					tenantId, 0, 0);
			}catch(Exception e){}
		}
		//*******************load new orders*************************//
		rs = stmt.executeQuery("SELECT * FROM new_orders"+tenantId );
		while(rs.next()){
//			response = voltdbConn.callProcedure("NEW_ORDERS"+volumnId+".select", rs.getInt("no_w_id"), rs.getInt("no_d_id"), rs.getInt("no_o_id"), tenantId);
//			if(response.getStatus() == ClientResponse.SUCCESS && response.getResults()[0].getRowCount() != 0){
//				continue;
//			}		
			try{
			voltdbConn.callProcedure("NEW_ORDERS"+volumnId+".insert", rs.getInt("no_o_id"), rs.getInt("no_d_id"), 
					rs.getInt("no_w_id"), tenantId, 0, 0);
			}catch(Exception e){}
		}
		//******************load orders******************************//
		rs = stmt.executeQuery("SELECT * FROM orders"+tenantId );
		while(rs.next()){
//			response = voltdbConn.callProcedure("ORDERS"+volumnId+".select", rs.getInt("o_w_id"), rs.getInt("o_d_id"), rs.getInt("o_id"), tenantId);
//			if(response.getStatus() == ClientResponse.SUCCESS && response.getResults()[0].getRowCount() != 0){
//				continue;
//			}	
			try{
			voltdbConn.callProcedure("ORDERS"+volumnId+".insert", rs.getInt("o_id"), rs.getInt("o_d_id"),
					rs.getInt("o_w_id"), rs.getInt("o_c_id"),
					rs.getTimestamp("o_entry_d"), 
					rs.getInt("o_carrier_id"), rs.getInt("o_ol_cnt"), rs.getInt("o_all_local"), tenantId, 0, 0);
			}catch(Exception e){}
		}
		//***********************load order line********************//
		rs = stmt.executeQuery("SELECT * FROM order_line"+tenantId );
		while(rs.next()){
//			response = voltdbConn.callProcedure("ORDER_LINE"+volumnId+".select", rs.getInt("ol_w_id"), rs.getInt("ol_d_id"), rs.getInt("ol_o_id"), rs.getInt("ol_number"), tenantId);
//			if(response.getStatus() == ClientResponse.SUCCESS && response.getResults()[0].getRowCount() != 0){
//				continue;
//			}		
			try{
			voltdbConn.callProcedure("ORDER_LINE"+volumnId+".insert", rs.getInt("ol_o_id"), rs.getInt("ol_d_id"), 
					rs.getInt("ol_w_id"), rs.getInt("ol_number"), rs.getInt("ol_i_id"), rs.getInt("ol_supply_w_id"),
					rs.getTimestamp("ol_delivery_d"), 
					rs.getInt("ol_quantity"), rs.getDouble("ol_amount"), rs.getString("ol_dist_info"), tenantId, 0, 0);
			}catch(Exception e){}
		}
		//********************load item***************************//
		rs = stmt.executeQuery("SELECT * FROM item"+tenantId );
		while(rs.next()){
//			response = voltdbConn.callProcedure("ITEM"+volumnId+".select", rs.getInt("i_id"), tenantId);
//			if(response.getStatus() == ClientResponse.SUCCESS && response.getResults()[0].getRowCount() != 0){
//				continue;
//			}	
			try{
			voltdbConn.callProcedure("ITEM"+volumnId+".insert", rs.getInt("i_id"), rs.getInt("i_im_id"), 
					rs.getString("i_name"), rs.getDouble("i_price"), rs.getString("i_data"), 
					tenantId, 0, 0);
			}catch(Exception e){}
		}
		//*********************load stock*************************//
		rs = stmt.executeQuery("SELECT * FROM stock"+tenantId );
		while(rs.next()){
//			response = voltdbConn.callProcedure("STOCK"+volumnId+".select", rs.getInt("s_w_id"), rs.getInt("s_i_id"), tenantId);
//			if(response.getStatus() == ClientResponse.SUCCESS && response.getResults()[0].getRowCount() != 0){
//				continue;
//			}		
			try{
			voltdbConn.callProcedure("STOCK"+volumnId+".insert", rs.getInt("s_i_id"), rs.getInt("s_w_id"),
					rs.getInt("s_quantity"), rs.getString("s_dist_01"), rs.getString("s_dist_02"), rs.getString("s_dist_03"), 
					rs.getString("s_dist_04"), rs.getString("s_dist_05"), rs.getString("s_dist_06"), rs.getString("s_dist_07"), 
					rs.getString("s_dist_08"), rs.getString("s_dist_09"), rs.getString("s_dist_10"), rs.getDouble("s_ytd"), 
					rs.getInt("s_order_cnt"), rs.getInt("s_remote_cnt"), rs.getString("s_data"), tenantId, 0, 0);
			}catch(Exception e){}
		}
	}
	
	public void Voltdb2Mysql(int tenantId, int volumnId){
		CustomerRetriver cr = new CustomerRetriver(this.dbURL, this.dbUsername, this.dbPassword, this.voltdbServer, tenantId, volumnId);
		cr.start();
		DistrictRetriver dr = new DistrictRetriver(this.dbURL, this.dbUsername, this.dbPassword, this.voltdbServer, tenantId, volumnId);
		dr.start();
		HistoryRetriver hr = new HistoryRetriver(this.dbURL, this.dbUsername, this.dbPassword, this.voltdbServer, tenantId, volumnId);
		hr.start();
		NewOrdersRetriver nor = new NewOrdersRetriver(this.dbURL, this.dbUsername, this.dbPassword, this.voltdbServer, tenantId, volumnId);
		nor.start();
		OrderLineRetriver olr = new OrderLineRetriver(this.dbURL, this.dbUsername, this.dbPassword, this.voltdbServer, tenantId, volumnId);
		olr.start();
		OrdersRetriver or = new OrdersRetriver(this.dbURL, this.dbUsername, this.dbPassword, this.voltdbServer, tenantId, volumnId);
		or.start();
		StockRetriver sr = new StockRetriver(this.dbURL, this.dbUsername, this.dbPassword, this.voltdbServer, tenantId, volumnId);
		sr.start();
		WarehouseRetriver wr = new WarehouseRetriver(this.dbURL, this.dbUsername, this.dbPassword, this.voltdbServer, tenantId, volumnId);
		wr.start();
		ItemRetriver ir = new ItemRetriver(this.dbURL, this.dbUsername, this.dbPassword, this.voltdbServer, tenantId, volumnId);
		ir.start();
		try {
			cr.join();
			dr.join();
			hr.join();
			nor.join();
			olr.join();
			or.join();
			sr.join();
			wr.join();
			ir.join();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
	
}
