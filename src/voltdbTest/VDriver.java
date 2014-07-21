package voltdbTest;

import java.io.IOException;
import java.util.Random;

import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcCallException;

import utility.Support;

public class VDriver {
	public static boolean IsActive = true;

	public static int min_ware = 1;
	public static int max_ware = 1;
	public static int num_ware = 1;
	public static int num_node = 0;
	public static int arg_offset = 0;
	public static int MAXITEMS = 100000;
	public static int CUST_PER_DIST = 3000;
	public static int DIST_PER_WARE = 10;
	public static int ORD_PER_DIST = 3000;
	public static int MAX_NUM_ITEM = 15;
	public static int MAX_ITEM_LEN = 24;
	public static int TYPE_INT = 0;
	public static int TYPE_STRING = 1;
	public static int TYPE_FLOAT = 2;
	public static int TYPE_DOUBLE = 3;

	public static int CUSTOMER = 0;
	public static int DISTRICT = 1;
	public static int ITEM = 2;
	public static int NEW_ORDERS = 3;
	public static int ORDER_LINE = 4;
	public static int ORDERS = 5;
	public static int STOCK = 6;
	public static int WAREHOUSE = 7;
	public static int HISTORY = 8;
	public static int QUERY_SELECT = 0;
	public static int QUERY_UPDATE = 1;
	public static int QUERY_DELETE = 2;
	public static int QUERYINSERT = 3;
	public static String[] tables = {"CUSTOMER", "DISTRICT", "ITEM", "NEW_ORDERS", "ORDER_LINE", "ORDERS", "STOCK", "WAREHOUSE", "HISTORY"};
	public static String[] querys = {"select", "update", "delete", "insert"};

	public Random ran = new Random();
	// ran = new Random();
	/* listed bellow are all attributes of each table in the database */
	/* warehouse */
	public int w_id;
	public String w_name, w_street_1, w_street_2, w_city, w_state, w_zip;
	public float w_tax;
	public double w_ytd = 3000000.00;
	/* district */
	public int d_id, d_w_id;
	public String d_name, d_street_1, d_street_2, d_city, d_state, d_zip;
	public float d_tax;
	public double d_ytd = 30000.0;
	public int d_next_o_id = 3001;
	/* customer */
	public int c_id, c_d_id, c_w_id;
	public String c_first, c_last, c_street_1, c_street_2, c_city, c_state,
			c_zip, c_phone, c_since, c_credit, c_data;
	public double c_discount;
	public String c_middle = "OE";
	public double c_ytd_payment = 10.0;
	public double c_balance = -10.0;
	public int c_credit_lim = 50000, c_payment_cnt = 1, c_delivery_cnt = 0;
	/* history */
	public int h_c_id, h_c_d_id, h_c_w_id, h_d_id, h_w_id;
	public String h_date, h_data;
	public double h_amount = 10.0;
	/* new_orders */
	public int no_o_id, no_d_id, no_w_id;
	/* orders */
	public int o_id, o_d_id, o_w_id, o_c_id, o_ol_cnt;
	public String o_entry_d;

	int o_carrier_id;
	public int o_all_local = 1;
	/* order_line */
	public int ol_o_id, ol_d_id, ol_w_id, ol_number, ol_i_id, ol_supply_w_id;
	public String ol_delivery_d, ol_dist_info;
	public int ol_quantity = 5;
	public double ol_amount = 0.0;
	/* item */
	public int i_id, i_im_id;
	public String i_name, i_data;
	public double i_price;
	/* stock */
	public int s_i_id, s_w_id, s_quantity;
	public String s_dist_01, s_dist_02, s_dist_03, s_dist_04, s_dist_05,
			s_dist_06, s_dist_07, s_dist_08, s_dist_09, s_dist_10;
	public int s_ytd = 0, s_order_cnt = 0, s_remote_cnt = 0;
	public String s_data;

	Object[] para = new Object[30];
	int paraNumber;
	
	public VDriver(int Id) {
		// int[] paraType = new int[30];
		while (Driver.IsActive) { // initiate parameter
			int seq = Tenant.tenants[Id].sequence.nextSequence();
			if (seq == 32) { //history insert
				para[0] = h_c_id = Support.RandomNumber(1,Driver.CUST_PER_DIST);
				para[1] = h_c_d_id = Support.RandomNumber(1,Driver.DIST_PER_WARE);
				para[2] = h_c_w_id = Support.RandomNumber(min_ware,max_ware);
				para[3] = h_d_id = h_c_d_id;
				para[4] = h_w_id = h_c_w_id;
				para[5] = h_date = Support.getTimeStamp();
				para[6] = h_amount = 10.0;
				para[7] = h_data = Support.MakeAlphaString(12,24);
				para[8] = 1; para[9] = 0;
				paraNumber = 10;
				doSQL(Id, seq, paraNumber, para);
				continue;
			}

			int tableId = seq % 8;
			int queryId = seq / 8;
			switch (tableId) {
			case 0: // customer
				c_id = Support.RandomNumber(1, Driver.CUST_PER_DIST);
				c_d_id = Support.RandomNumber(1, Driver.DIST_PER_WARE);
				c_w_id = Support.RandomNumber(min_ware, max_ware);
				c_first = Support.MakeAlphaString(8, 16);
				c_middle = "OE";
				c_last = Support.Lastname(Support.NURand(255, 0, 999));
				c_street_1 = Support.MakeAlphaString(10, 20);
				c_street_2 = Support.MakeAlphaString(10, 20);
				c_city = Support.MakeAlphaString(10, 20);
				c_state = Support.MakeAlphaString(2, 2);
				c_zip = Support.MakeNumberString(9, 9);
				c_phone = Support.MakeNumberString(16, 16);
				c_since = Support.getTimeStamp();
				c_credit = "C0";
				c_credit_lim = 50000;
				c_discount = ran.nextInt(50) / 100.0;
				c_balance = 10.0;
				c_ytd_payment = 10.0;
				c_payment_cnt = 1;
				c_delivery_cnt = 0;
				c_data = Support.MakeAlphaString(300, 500);
				if (queryId == 0 || queryId == 2) {
					para[0] = c_id;
					para[1] = c_d_id;
					para[2] = c_w_id;
					paraNumber = 3;
				} else if (queryId == 1 || queryId == 3) {
					para[0] = c_id;
					para[1] = c_d_id;
					para[2] = c_w_id;
					para[3] = c_first;
					para[4] = c_middle;
					para[5] = c_last;
					para[6] = c_street_1;
					para[7] = c_street_2;
					para[8] = c_city;
					para[9] = c_state;
					para[10] = c_zip;
					para[11] = c_phone;
					para[12] = c_since;
					para[13] = c_credit;
					para[14] = c_credit_lim;
					para[15] = c_discount;
					para[16] = c_balance;
					para[17] = c_ytd_payment;
					para[18] = c_payment_cnt;
					para[19] = c_delivery_cnt;
					para[20] = c_data;
					if(queryId == 1){
						para[21] = 0;
						para[22] = 1;
						para[23] = c_id;
						para[24] = c_d_id;
						para[25] = c_w_id;
						paraNumber = 26;
					}else{ //
						para[21] = 1; para[22] = 0;
						paraNumber = 23;
					}
				}
				break;
			case 1:// district
				d_id = Support.RandomNumber(1, Driver.DIST_PER_WARE);
				d_w_id = Support.RandomNumber(min_ware, max_ware);
				d_name = Support.MakeAlphaString(6, 10);
				d_street_1 = Support.MakeAlphaString(10, 20);
				d_street_2 = Support.MakeAlphaString(10, 20);
				d_city = Support.MakeAlphaString(10, 20);
				d_state = Support.MakeAlphaString(2, 2);
				d_zip = Support.MakeNumberString(9, 9);
				d_tax = (float) (ran.nextInt(20) / 100.0);
				d_ytd = 30000.0;
				d_next_o_id = 3001;
				if (queryId == 0 || queryId == 2) {
					para[0] = d_w_id;
					para[1] = d_id;
					paraNumber = 2;
				} else if(queryId == 1 || queryId == 3){
					para[0] = d_id;
					para[1] = d_w_id;
					para[2] = d_name;
					para[3] = d_street_1;
					para[4] = d_street_2;
					para[5] = d_city;
					para[6] = d_state;
					para[7] = d_zip;
					para[8] = d_tax;
					para[9] = d_ytd;
					para[10] = d_next_o_id;
					if(queryId == 1){
						para[11] = 0; para[12] = 1;
						para[13] = d_w_id;
						para[14] = d_id;
						paraNumber = 15;
					}else{
						para[11] = 1; para[12] = 0;
						paraNumber = 13;
					}
				}
				break;
			case 2:// item
				i_id = Support.RandomNumber(1, MAXITEMS);
				i_im_id = ran.nextInt(9999) + 1;
				i_name = Support.MakeAlphaString(14, 24);
				i_price = (ran.nextInt(9900) + 100) / 100.0;
				i_data = Support.MakeAlphaString(26, 50);
				if (queryId == 0 || queryId == 2) {
					para[0] = i_id;
					paraNumber = 1;
				} else if(queryId == 1 || queryId == 3) {
					para[0] =  i_id;
					para[1] = i_im_id;
					para[2] = i_name;
					para[3] = i_price;
					para[4] = i_data;
					if(queryId == 1){
						para[5] = 0; para[6] = 1;
						para[7] = i_id;
						paraNumber = 8;
					}else{
						para[5] = 1; para[6] = 0;
						paraNumber = 7;
					}
				}
				break;
			case 3:// new_orders
				no_o_id = Support.RandomNumber(1, Driver.ORD_PER_DIST);
				no_d_id = Support.RandomNumber(1, Driver.DIST_PER_WARE);
				no_w_id = Support.RandomNumber(min_ware, max_ware);
				if (queryId == 0 || queryId == 2) {
					para[0] = no_w_id;
					para[1] = no_d_id;
					para[2] = no_o_id;
					paraNumber = 3;
				} else if(queryId == 1 || queryId == 3) {
					para[0] = no_o_id;
					para[1] = no_d_id;
					para[2] = no_w_id;
					if(queryId == 1){
						para[3] = 0; para[4] = 1;
						para[5] = no_w_id;
						para[6] = no_d_id;
						para[7] = no_o_id;
						paraNumber = 8;
					}else{
						para[3] = 1; para[4] = 0;
						paraNumber = 5;
					}
				}
				break;
			case 4:// order_line
				ol_o_id = Support.RandomNumber(1, Driver.ORD_PER_DIST);
				ol_d_id = Support.RandomNumber(1, Driver.DIST_PER_WARE);
				ol_w_id = Support.RandomNumber(min_ware, max_ware);
				ol_number = Support.RandomNumber(1, 15);
				ol_i_id = Support.RandomNumber(1, Driver.MAXITEMS);
				ol_supply_w_id = Support.RandomNumber(min_ware, max_ware);
				ol_delivery_d = Support.getTimeStamp();
				ol_quantity = 5;
				ol_amount = 0.0;
				ol_dist_info = Support.MakeAlphaString(24, 24);
				if (queryId == 0 || queryId == 2) {
					para[0] = ol_w_id;
					para[1] = ol_d_id;
					para[2] = ol_o_id;
					para[3] = ol_number;
					paraNumber = 4;
				} else if(queryId == 1 || queryId == 3) {
					para[0] = ol_o_id;
					para[1] = ol_d_id;
					para[2] = ol_w_id;
					para[3] = ol_number;
					para[4] = ol_i_id;
					para[5] = ol_supply_w_id;
					para[6] = ol_delivery_d;
					para[7] = ol_quantity;
					para[8] = ol_amount;
					para[9] = ol_dist_info;
					if(queryId == 1){
						para[10] = 0; para[11] = 1;
						para[12] = ol_w_id;
						para[13] = ol_d_id;
						para[14] = ol_o_id;
						para[15] = ol_number;
						paraNumber = 16;
					}else{
						para[10] = 1; para[11] = 0;
						paraNumber = 12;
					}
				}
				break;
			case 5:// orders
				o_id = Support.RandomNumber(1, Driver.ORD_PER_DIST);
				o_d_id = Support.RandomNumber(1, Driver.DIST_PER_WARE);
				o_w_id = Support.RandomNumber(min_ware, max_ware);
				o_c_id = Support.RandomNumber(1, Driver.ORD_PER_DIST);
				o_entry_d = Support.MakeAlphaString(24, 24);
				o_carrier_id = Support.RandomNumber(1, 10);
				o_ol_cnt = Support.RandomNumber(5, 15);
				o_all_local = ran.nextInt(100) == 1 ? 0 : 1;
				if (queryId == 0 || queryId == 2) {
					para[0] = o_id;
					paraNumber = 1;
				} else if(queryId == 1 || queryId == 3) {
					para[0] = o_id;
					para[1] = o_d_id;
					para[2] = o_w_id;
					para[3] = o_c_id;
					para[4] = o_entry_d;
					para[5] = o_carrier_id;
					para[6] = o_ol_cnt;
					para[7] = o_all_local;
					if(queryId == 1){
						para[8] = 0; para[9] = 1;
						para[10] = o_id;
						paraNumber = 11;
					}else{
						para[8] = 1; para[9] = 0;
						paraNumber = 10;
					}
				}
				break;
			case 6:// stock
				s_i_id = Support.RandomNumber(1, MAXITEMS);
				s_w_id = Support.RandomNumber(min_ware, max_ware);
				s_quantity = Support.RandomNumber(10, 100);
				s_dist_01 = Support.MakeAlphaString(24, 24);
				s_dist_02 = Support.MakeAlphaString(24, 24);
				s_dist_03 = Support.MakeAlphaString(24, 24);
				s_dist_04 = Support.MakeAlphaString(24, 24);
				s_dist_05 = Support.MakeAlphaString(24, 24);
				s_dist_06 = Support.MakeAlphaString(24, 24);
				s_dist_07 = Support.MakeAlphaString(24, 24);
				s_dist_08 = Support.MakeAlphaString(24, 24);
				s_dist_09 = Support.MakeAlphaString(24, 24);
				s_dist_10 = Support.MakeAlphaString(24, 24);
				s_ytd = 0;
				s_order_cnt = 0;
				s_remote_cnt = 0;
				s_data = Support.MakeAlphaString(26, 50);
				if (queryId == 0 || queryId == 2) {
					para[0] = s_w_id;
					para[1] = s_i_id;
					paraNumber = 2;
				} else if(queryId == 1 || queryId == 3) {
					para[0] = s_i_id;
					para[1] = s_w_id;
					para[2] = s_quantity;
					para[3] = s_dist_01;
					para[4] = s_dist_02;
					para[5] = s_dist_03;
					para[6] = s_dist_04;
					para[7] = s_dist_05;
					para[8] = s_dist_06;
					para[9] = s_dist_07;
					para[10] = s_dist_08;
					para[11] = s_dist_09;
					para[12] = s_dist_10;
					para[13] = s_ytd;
					para[14] = s_order_cnt;
					para[15] = s_remote_cnt;
					para[16] = s_data;
					if(queryId == 1){
						para[17] = 0; para[18] = 1;
						para[19] = s_w_id;
						para[20] = s_i_id;
						paraNumber = 21;
					}else{
						para[17] = 1; para[18] = 0;
						paraNumber = 19;
					}
				}
				break;
			case 7:// warehouse
				w_id = Support.RandomNumber(min_ware, max_ware);
				w_name = Support.MakeAlphaString(6, 10);
				w_street_1 = Support.MakeAlphaString(10, 20);
				w_street_2 = Support.MakeAlphaString(10, 20);
				w_city = Support.MakeAlphaString(10, 20);
				w_state = Support.MakeAlphaString(2, 2);
				w_zip = Support.MakeNumberString(9, 9);
				w_tax = (float) (Support.RandomNumber(10, 20) / 100.0);
				w_ytd = 3000000.00;
				if (queryId == 0 || queryId == 2) {
					para[0] = w_id;
					paraNumber = 1;
				} else if(queryId == 1 || queryId == 3) {
					para[0] = w_id;
					para[1] = w_name;
					para[2] = w_street_1;
					para[3] = w_street_2;
					para[4] = w_city;
					para[5] = w_state;
					para[6] = w_zip;
					para[7] = w_tax;
					para[8] = w_ytd;
					if(queryId == 1){
						para[9] = 0; para[10] = 1;
						para[11] = w_id;
						paraNumber = 12;
					}else{
						para[9] = 1; para[10] = 0;
						paraNumber = 11;
					}
				}
				break;
			default:
			}
			doSQL(Id, seq, paraNumber, para);
		}
	}

	public void doSQL(int threadId, int sqlId, int paraNumber, Object[] para) {
		boolean success = true;
		for(int i=0; i<VMain.MaxTry; i++){
			success = doSQLOnce(threadId, sqlId, paraNumber, para);
			
			if(success && Driver.IsActive && VMain.startCount){
				VMain.queryThisInterval ++;
				return;
			}else if(!success && Driver.IsActive & VMain.startCount){
//				System.out.print(".");
				System.out.println("sql failure! tenant id: "+threadId+", sql id: "+sqlId);
				System.exit(0);
			}else if(!Driver.IsActive){
				return;
			}
		}
	}

	public boolean doSQLOnce(int threadId, int sqlId, int paraNumber, Object[] para) {
		ClientResponse response = null;
		try{
			if(sqlId == 32){
				response = Tenant.tenants[threadId].voltdbConn.callProcedure("HISTORY"+threadId+".insert", para[0], para[1], para[2], para[3], para[4], para[5], para[6], para[7], para[8], para[9], para[10]);
				return true;
			}
			int tableId = sqlId % 8;
			int queryId = sqlId / 8;
			switch(paraNumber){
			case 1:
				response = Tenant.tenants[threadId].voltdbConn.callProcedure(tables[tableId]+threadId+"."+querys[queryId], threadId, para[0]);
				break;
			case 2:
				response = Tenant.tenants[threadId].voltdbConn.callProcedure(tables[tableId]+threadId+"."+querys[queryId], threadId, para[0], para[1]);
				break;
			case 3:
				response = Tenant.tenants[threadId].voltdbConn.callProcedure(tables[tableId]+threadId+"."+querys[queryId], threadId, para[0], para[1], para[2]);
				break;
			case 4:
				response = Tenant.tenants[threadId].voltdbConn.callProcedure(tables[tableId]+threadId+"."+querys[queryId], threadId, para[0], para[1], para[2], para[3]);
				break;
			case 5:
				response = Tenant.tenants[threadId].voltdbConn.callProcedure(tables[tableId]+threadId+"."+querys[queryId], threadId, para[0], para[1], para[2], para[3], para[4]);
				break;
			case 6:
				response = Tenant.tenants[threadId].voltdbConn.callProcedure(tables[tableId]+threadId+"."+querys[queryId], threadId, para[0], para[1], para[2], para[3], para[4], para[5]);
				break;
			case 7:
				response = Tenant.tenants[threadId].voltdbConn.callProcedure(tables[tableId]+threadId+"."+querys[queryId], threadId, para[0], para[1], para[2], para[3], para[4], para[5], para[6]);
				break;
			case 8:
				response = Tenant.tenants[threadId].voltdbConn.callProcedure(tables[tableId]+threadId+"."+querys[queryId], threadId, para[0], para[1], para[2], para[3], para[4], para[5], para[6], para[7]);
				break;
			case 9:
				response = Tenant.tenants[threadId].voltdbConn.callProcedure(tables[tableId]+threadId+"."+querys[queryId], threadId, para[0], para[1], para[2], para[3], para[4], para[5], para[6], para[7], para[8]);
				break;
			case 10:
				response = Tenant.tenants[threadId].voltdbConn.callProcedure(tables[tableId]+threadId+"."+querys[queryId], threadId, para[0], para[1], para[2], para[3], para[4], para[5], para[6], para[7], para[8], para[9]);
				break;
			case 11:
				response = Tenant.tenants[threadId].voltdbConn.callProcedure(tables[tableId]+threadId+"."+querys[queryId], threadId, para[0], para[1], para[2], para[3], para[4], para[5], para[6], para[7], para[8], para[9], para[10]);
				break;
			case 12:
				response = Tenant.tenants[threadId].voltdbConn.callProcedure(tables[tableId]+threadId+"."+querys[queryId], threadId, para[0], para[1], para[2], para[3], para[4], para[5], para[6], para[7], para[8], para[9], para[10], para[11]);
				break;
			case 13:
				response = Tenant.tenants[threadId].voltdbConn.callProcedure(tables[tableId]+threadId+"."+querys[queryId], threadId, para[0], para[1], para[2], para[3], para[4], para[5], para[6], para[7], para[8], para[9], para[10], para[11], para[12]);
				break;
			case 14:
				response = Tenant.tenants[threadId].voltdbConn.callProcedure(tables[tableId]+threadId+"."+querys[queryId], threadId, para[0], para[1], para[2], para[3], para[4], para[5], para[6], para[7], para[8], para[9], para[10], para[11], para[12], para[13]);
				break;
			case 15:
				response = Tenant.tenants[threadId].voltdbConn.callProcedure(tables[tableId]+threadId+"."+querys[queryId], threadId, para[0], para[1], para[2], para[3], para[4], para[5], para[6], para[7], para[8], para[9], para[10], para[11], para[12], para[13], para[14]);
				break;
			case 16:
				response = Tenant.tenants[threadId].voltdbConn.callProcedure(tables[tableId]+threadId+"."+querys[queryId], threadId, para[0], para[1], para[2], para[3], para[4], para[5], para[6], para[7], para[8], para[9], para[10], para[11], para[12], para[13], para[14], para[15]);
				break;
			case 19:
				response = Tenant.tenants[threadId].voltdbConn.callProcedure(tables[tableId]+threadId+"."+querys[queryId], threadId, para[0], para[1], para[2], para[3], para[4], para[5], para[6], para[7], para[8], para[9], para[10], para[11], para[12], para[13], para[14], para[15], para[16], para[17], para[18]);
				break;
			case 21:
				response = Tenant.tenants[threadId].voltdbConn.callProcedure(tables[tableId]+threadId+"."+querys[queryId], threadId, para[0], para[1], para[2], para[3], para[4], para[5], para[6], para[7], para[8], para[9], para[10], para[11], para[12], para[13], para[14], para[15], para[16], para[17], para[18], para[19], para[20]);
				break;
			case 23:
				response = Tenant.tenants[threadId].voltdbConn.callProcedure(tables[tableId]+threadId+"."+querys[queryId], threadId, para[0], para[1], para[2], para[3], para[4], para[5], para[6], para[7], para[8], para[9], para[10], para[11], para[12], para[13], para[14], para[15], para[16], para[17], para[18], para[19], para[20], para[21], para[22]);
				break;
			case 26:
				response = Tenant.tenants[threadId].voltdbConn.callProcedure(tables[tableId]+threadId+"."+querys[queryId], threadId, para[0], para[1], para[2], para[3], para[4], para[5], para[6], para[7], para[8], para[9], para[10], para[11], para[12], para[13], para[14], para[15], para[16], para[17], para[18], para[19], para[20], para[21], para[22], para[23], para[24], para[25]);
				break;
				default:
			}
			
			if(response.getStatus() != ClientResponse.SUCCESS){
				System.out.println("response failed");
				return false;
			}
			return true;
//			long rets = response.getResults()[0].asScalarLong();
//			if(rets == 0  )
//				return false;
//			else return true;
		}catch(IOException | ProcCallException e){
			e.printStackTrace();
			return false;
		}
	}
	
}
