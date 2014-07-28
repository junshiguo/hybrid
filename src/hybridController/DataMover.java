package hybridController;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.NoConnectionsException;
import org.voltdb.client.ProcCallException;

import retrivers.*;
import utility.DBManager;

public class DataMover {
	public Connection conn;
	public Client voltdbConn;
	public String dbURL = "jdbc:mysql://10.20.2.211/tpcc10";
	public String dbUsername = "remote";
	public String dbPassword = "remote";
	public String voltdbServer = "10.20.2.211";

	public DataMover(String url, String username, String password, String voltdbServer){
		this.dbURL = url;
		this.dbUsername = username;
		this.dbPassword = password;
		this.voltdbServer = voltdbServer;
		conn = DBManager.connectDB(url, username, password);
		voltdbConn = DBManager.connectVoltdb(voltdbServer);
	}
	
	public void Mysql2Voltdb(int tenantId, int volumnId) throws SQLException, NoConnectionsException, IOException, ProcCallException{
		if(conn == null){
			conn = DBManager.connectDB(this.dbURL, this.dbUsername, this.dbPassword);
		}
		if(voltdbConn == null){
			voltdbConn = DBManager.connectVoltdb(this.voltdbServer);
		}
		Statement stmt = conn.createStatement();
		ClientResponse response = null;
//		Main.usingVoltdb[tenantId] = true;
//		Main.partiallyUsingVoltdb[tenantId] = true;
		//********************load warehouse******************//
		ResultSet rs = stmt.executeQuery("SELECT  * FROM warehouse"+tenantId);
		while(rs.next()){
			response = voltdbConn.callProcedure("WAREHOUSE"+volumnId+".select", volumnId, rs.getInt("w_id"), tenantId);
			if(response.getStatus() == ClientResponse.SUCCESS && response.getResults()[0].getRowCount() != 0){
				continue;
			}
			voltdbConn.callProcedure("WAREHOUSE"+volumnId+".insert", volumnId, rs.getInt("w_id"), 
					rs.getString("w_name"), rs.getString("w_street_1"), rs.getString("w_street_2"), rs.getString("w_city"), 
					rs.getString("w_state"), rs.getString("w_zip"), rs.getDouble("w_tax"), rs.getDouble("w_ytd"), 
					tenantId, 0, 0);
		}
		//*******************load district************************//
		rs = stmt.executeQuery("SELECT * FROM district"+tenantId);
		while(rs.next()){
			response = voltdbConn.callProcedure("DISTRICT"+volumnId+".select", volumnId, rs.getInt("d_w_id"), rs.getInt("d_id"), tenantId);
			if(response.getStatus() == ClientResponse.SUCCESS && response.getResults()[0].getRowCount() != 0){
				continue;
			}
			voltdbConn.callProcedure("DISTRICT"+volumnId+".insert", volumnId, rs.getInt("d_id"), rs.getInt("d_w_id"), 
					rs.getString("d_name"), rs.getString("d_street_1"), rs.getString("d_street_2"), rs.getString("d_city"), 
					rs.getString("d_state"), rs.getString("d_zip"),	rs.getDouble("d_tax"), rs.getDouble("d_ytd"), 
					rs.getInt("d_next_o_id"), tenantId, 0, 0);
		}
		//******************load customer**************************//
		rs = stmt.executeQuery("SELECT * FROM customer"+tenantId );
		while(rs.next()){
			response = voltdbConn.callProcedure("CUSTOMER"+volumnId+".select", volumnId, rs.getInt("c_id"), rs.getInt("c_w_id"), rs.getInt("c_d_id"), tenantId);
			if(response.getStatus() == ClientResponse.SUCCESS && response.getResults()[0].getRowCount() != 0){
				continue;
			}
			voltdbConn.callProcedure("CUSTOMER"+volumnId+".insert", volumnId, rs.getInt("c_id"), rs.getInt("c_d_id"), 
					rs.getInt("c_w_id"), rs.getString("c_first"), rs.getString("c_middle"), rs.getString("c_last"),
					rs.getString("c_street_1"), rs.getString("c_street_2"), rs.getString("c_city"),rs.getString("c_state"),
					rs.getString("c_zip"), rs.getString("c_phone"), rs.getTimestamp("c_since"), 
					rs.getString("c_credit"), rs.getInt("c_credit_lim"), rs.getDouble("c_discount"), rs.getDouble("c_balance"), 
					rs.getDouble("c_ytd_payment"), rs.getInt("c_payment_cnt"), rs.getInt("c_delivery_cnt"), rs.getString("c_data"),
					tenantId, 0, 0);
		}
		//******************load history*****************************//
		rs = stmt.executeQuery("SELECT * FROM history"+tenantId );
		while(rs.next()){ 
			response = voltdbConn.callProcedure("HISTORY"+volumnId+".select", volumnId, rs.getInt("h_c_id"), rs.getInt("h_c_d_id"), rs.getInt("h_c_w_id"), tenantId);
			if(response.getStatus() == ClientResponse.SUCCESS && response.getResults()[0].getRowCount() != 0){
				continue;
			}
			voltdbConn.callProcedure("HISTORY"+volumnId+".insert", volumnId, rs.getInt("h_c_id"), rs.getInt("h_c_d_id"), 
					rs.getInt("h_c_w_id"), rs.getInt("h_d_id"), rs.getInt("h_w_id"), 
					rs.getTimestamp("h_date"), rs.getDouble("h_amount"), rs.getString("h_data"),
					tenantId, 0, 0);
		}
		//*******************load new orders*************************//
		rs = stmt.executeQuery("SELECT * FROM new_orders"+tenantId );
		while(rs.next()){
			response = voltdbConn.callProcedure("NEW_ORDERS"+volumnId+".select", volumnId, rs.getInt("no_w_id"), rs.getInt("no_d_id"), rs.getInt("no_o_id"), tenantId);
			if(response.getStatus() == ClientResponse.SUCCESS && response.getResults()[0].getRowCount() != 0){
				continue;
			}			
			voltdbConn.callProcedure("NEW_ORDERS"+volumnId+".insert", volumnId, rs.getInt("no_o_id"), rs.getInt("no_d_id"), 
					rs.getInt("no_w_id"), tenantId, 0, 0);
		}
		//******************load orders******************************//
		rs = stmt.executeQuery("SELECT * FROM orders"+tenantId );
		while(rs.next()){
			response = voltdbConn.callProcedure("ORDERS"+volumnId+".select", volumnId, rs.getInt("o_w_id"), rs.getInt("o_d_id"), rs.getInt("o_id"), tenantId);
			if(response.getStatus() == ClientResponse.SUCCESS && response.getResults()[0].getRowCount() != 0){
				continue;
			}		
			voltdbConn.callProcedure("ORDERS"+volumnId+".insert", volumnId, rs.getInt("o_id"), rs.getInt("o_d_id"),
					rs.getInt("o_w_id"), rs.getInt("o_c_id"),
					rs.getTimestamp("o_entry_d"), 
					rs.getInt("o_carrier_id"), rs.getInt("o_ol_cnt"), rs.getInt("o_all_local"), tenantId, 0, 0);
		}
		//***********************load order line********************//
		rs = stmt.executeQuery("SELECT * FROM order_line"+tenantId );
		while(rs.next()){
			response = voltdbConn.callProcedure("ORDER_LINE"+volumnId+".select", volumnId, rs.getInt("ol_w_id"), rs.getInt("ol_d_id"), rs.getInt("ol_o_id"), rs.getInt("ol_number"), tenantId);
			if(response.getStatus() == ClientResponse.SUCCESS && response.getResults()[0].getRowCount() != 0){
				continue;
			}		
			voltdbConn.callProcedure("ORDER_LINE"+volumnId+".insert", volumnId, rs.getInt("ol_o_id"), rs.getInt("ol_d_id"), 
					rs.getInt("ol_w_id"), rs.getInt("ol_number"), rs.getInt("ol_i_id"), rs.getInt("ol_supply_w_id"),
					rs.getTimestamp("ol_delivery_d"), 
					rs.getInt("ol_quantity"), rs.getDouble("ol_amount"), rs.getString("ol_dist_info"), tenantId, 0, 0);
		}
		//********************load item***************************//
		rs = stmt.executeQuery("SELECT * FROM item"+tenantId );
		while(rs.next()){
			response = voltdbConn.callProcedure("ITEM"+volumnId+".select", volumnId, rs.getInt("i_id"), tenantId);
			if(response.getStatus() == ClientResponse.SUCCESS && response.getResults()[0].getRowCount() != 0){
				continue;
			}		
			voltdbConn.callProcedure("ITEM"+volumnId+".insert", volumnId, rs.getInt("i_id"), rs.getInt("i_im_id"), 
					rs.getString("i_name"), rs.getDouble("i_price"), rs.getString("i_data"), 
					tenantId, 0, 0);
		}
		//*********************load stock*************************//
		rs = stmt.executeQuery("SELECT * FROM stock"+tenantId );
		while(rs.next()){
			response = voltdbConn.callProcedure("STOCK"+volumnId+".select", volumnId, rs.getInt("s_w_id"), rs.getInt("s_i_id"), tenantId);
			if(response.getStatus() == ClientResponse.SUCCESS && response.getResults()[0].getRowCount() != 0){
				continue;
			}		
			voltdbConn.callProcedure("STOCK"+volumnId+".insert", volumnId, rs.getInt("s_i_id"), rs.getInt("s_w_id"),
					rs.getInt("s_quantity"), rs.getString("s_dist_01"), rs.getString("s_dist_02"), rs.getString("s_dist_03"), 
					rs.getString("s_dist_04"), rs.getString("s_dist_05"), rs.getString("s_dist_06"), rs.getString("s_dist_07"), 
					rs.getString("s_dist_08"), rs.getString("s_dist_09"), rs.getString("s_dist_10"), rs.getDouble("s_ytd"), 
					rs.getInt("s_order_cnt"), rs.getInt("s_remote_cnt"), rs.getString("s_data"), tenantId, 0, 0);
		}
//		Main.usingVoltdb[tenantId] = true;
//		Main.partiallyUsingVoltdb[tenantId] = false;
		System.out.println("Tenant "+tenantId+" data transferred from mysql to voltdb !");
	}
	
	public void Voltdb2Mysql(int tenantId, int volumnId){
//		Main.partiallyUsingVoltdb[tenantId] = true;
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
		try {
			cr.join();
			dr.join();
			hr.join();
			nor.join();
			olr.join();
			or.join();
			sr.join();
			wr.join();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
//		Main.usingVoltdb[tenantId] = false;
//		Main.partiallyUsingVoltdb[tenantId] = false;
		System.out.println("Tenant "+tenantId+" data retrived from voltdb back to mysql !");
	}
	
}
