package mysqlTest;

import java.sql.SQLException;
import java.util.Random;

import utility.IdMatch;
import utility.Support;

public class Driver {
	public static boolean IsActive = true;

	public static int min_ware = 1;
	public static int max_ware = 1;
	public static int num_ware = 1;
	public static int num_node = 0;
	public static int arg_offset = 0;
	public static int MAXITEMS = 5000;
	public static int CUST_PER_DIST = 50;
	public static int DIST_PER_WARE = 5;
	public static int ORD_PER_DIST = 50;
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
	public static String[] select25 = {
		"c_id, c_d_id, c_w_id, c_first, c_middle, c_last, c_street_1, c_street_2, c_city, c_state, c_zip, c_phone, c_since, c_credit, c_credit_lim, c_discount, c_balance, c_ytd_payment, c_payment_cnt, c_delivery_cnt",
		"d_id, d_street_1, d_next_o_id",
		"i_name",
		"no_w_id",
		"ol_o_id, ol_d_id, ol_delivery_d",
		"o_id, o_w_id",
		"s_quantity, s_dist_01, s_data",
		"w_street_1, w_tax",
		"h_date, h_amount"
	};
	public static String[] select50 = {
		"*",
		"d_street_1, d_street_2, d_zip",
		"i_data",
		"no_o_id",
		"ol_d_id, ol_dist_info",
		"o_d_id, o_w_id, o_entry_d",
		"s_i_id, s_w_id, s_ytd, s_dist_01, s_dist_02, s_dist_03, s_dist_04, s_dist_05, s_dist_06",
		"w_street_1, w_street_2, w_ytd",
		"h_data"
	};
	public static String[] select75 = {
		"*",
		"d_name, d_street_1, d_street_2, d_city, d_tax",
		"i_id, i_im_id, i_price, i_data",
		"no_o_id, no_d_id",
		"ol_o_id, ol_w_id, ol_delivery_d, ol_dist_info",
		"o_id, o_d_id, o_c_id, o_entry_d",
		"s_i_id, s_w_id, s_quantity, s_ytd, s_order_cnt, s_dist_01, s_dist_02, s_dist_03, s_dist_04, s_dist_05, s_dist_06, s_dist_07, s_dist_08, s_dist_09, s_dist_10",
		"w_street_1, w_street_2, w_city, w_tax, w_ytd",
		"h_c_d_id, h_c_w_id, h_date, h_data"
	};

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
	Object[] paratmp = new Object[30];
	Object[] PK = new Object[5];
	int paraNumber, paratmpNumber, PKNumber;
	
	public Driver(int Id) {
		// int[] paraType = new int[30];
		while (Driver.IsActive) { // initiate parameter
			int seq = 0, tableId = 0, queryId = 0;
			if(MMain.ReturnData == 100){
				seq = Tenant.tenants[Id].sequence.nextSequence();
				tableId = seq / 4;
				queryId = seq % 4;
				if(queryId == 2 || queryId == 3){
					queryId = 1;
					seq = tableId * 4 + queryId;
				}
			}else{
				queryId = 0;
				if(MMain.ReturnData == 25){
					tableId = Support.RandomNumber(1, 8);
				}else{
					tableId = Support.RandomNumber(0, 8);
				}				
				seq = tableId * 4;
			}
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
					para[1] = c_w_id;
					para[2] = c_d_id;
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
					para[21] = c_id;
					para[22] = c_w_id;
					para[23] = c_d_id;
					if(queryId == 1) paraNumber = 24;
					else paraNumber = 21;
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
					para[11] = d_w_id;
					para[12] = d_id;
					if (queryId == 1) {
						paraNumber = 13;
					} else {
						paraNumber = 11;
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
					para[5] = i_id;
					if (queryId == 1) {
						paraNumber = 6;
					} else {
						paraNumber = 5;
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
					para[3] = no_w_id;
					para[4] = no_d_id;
					para[5] = no_o_id;
					if (queryId == 1) {
						paraNumber = 6;
					} else {
						paraNumber = 3;
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
					para[10] = ol_w_id;
					para[11] = ol_d_id;
					para[12] = ol_o_id;
					para[13] = ol_number;
					if (queryId == 1) {
						paraNumber = 14;
					} else {
						paraNumber = 10;
					}
				}
				break;
			case 5:// orders
				o_id = Support.RandomNumber(1, Driver.ORD_PER_DIST);
				o_d_id = Support.RandomNumber(1, Driver.DIST_PER_WARE);
				o_w_id = Support.RandomNumber(min_ware, max_ware);
				o_c_id = Support.RandomNumber(1, Driver.ORD_PER_DIST);
				o_entry_d = Support.getTimeStamp();
				o_carrier_id = Support.RandomNumber(1, 10);
				o_ol_cnt = Support.RandomNumber(5, 15);
				o_all_local = ran.nextInt(100) == 1 ? 0 : 1;
				if (queryId == 0 || queryId == 2) {
					para[0] = o_w_id;
					para[1] = o_d_id;
					para[2] = o_id;
					paraNumber = 3;
				} else if(queryId == 1 || queryId == 3) {
					para[0] = o_id;
					para[1] = o_d_id;
					para[2] = o_w_id;
					para[3] = o_c_id;
					para[4] = o_entry_d;
					para[5] = o_carrier_id;
					para[6] = o_ol_cnt;
					para[7] = o_all_local;
					para[8] = o_w_id;
					para[9] = o_d_id;
					para[10] = o_id;
					if (queryId == 1) {
						paraNumber = 11;
					} else {
						paraNumber = 8;
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
					para[17] = s_w_id;
					para[18] = s_i_id;
					if (queryId == 1) {
						paraNumber = 19;
					} else {
						paraNumber = 17;
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
					para[9] = w_id;
					if (queryId == 1) {
						paraNumber = 10;
					} else {
						paraNumber = 9;
					}
				}
				break;
			case 8: //history
				h_c_id = Support.RandomNumber(1,Driver.CUST_PER_DIST);
				h_c_d_id = Support.RandomNumber(1,Driver.DIST_PER_WARE);
				h_c_w_id = Support.RandomNumber(min_ware,max_ware);
				h_d_id = h_c_d_id;
				h_w_id = h_c_w_id;
				h_date = Support.getTimeStamp();
				h_amount = 10.0;
				h_data = Support.MakeAlphaString(12,24);
				if(queryId == 0 || queryId == 2){
					para[0] = h_c_id;
					para[1] = h_c_d_id;
					para[2] = h_c_w_id;
					paraNumber = 3;
				}else if(queryId == 1 || queryId == 3){
					para[0] = h_c_id;
					para[1] = h_c_d_id;
					para[2] = h_c_w_id;
					para[3] = h_d_id;
					para[4] = h_w_id;
					para[5] = h_date;
					para[6] = h_amount;
					para[7] = h_data;
					para[8] = h_c_id;
					para[9] = h_c_d_id;
					para[10] = h_c_w_id;
					if (queryId == 1) {
						paraNumber = 11;
					} else {
						paraNumber = 8;
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
		int tenantId = ran.nextInt(MMain.tenantPerThread) + threadId*MMain.tenantPerThread;
		for(int i=0; i<MMain.MaxTry; i++){
			switch(MMain.ReturnData){
			case 25: 
				success = doSQLOnce_25(threadId, IdMatch.id2TableIndex(tenantId), sqlId, para, paraNumber);
				break;
			case 50:
				success = doSQLOnce_50(threadId, IdMatch.id2TableIndex(tenantId), sqlId, para, paraNumber);
				break;
			case 75:
				success = doSQLOnce_75(threadId, IdMatch.id2TableIndex(tenantId), sqlId, para, paraNumber);
				break;
			case 100:
				success = doSQLOnce(threadId, IdMatch.id2TableIndex(tenantId), sqlId, para, paraNumber);
				break;
				default:
			}
			
			if(success){
				MMain.queryThisInterval ++;
				break;
			}else {
				MMain.retryThisInterval ++;
//				System.out.println("sql failure! tenant id: "+threadId+", table id: "+tenantId+", sql id: "+sqlId+". retrying...");
				System.out.print(".");
				if(MMain.retryThisInterval % 100 == 0)
					System.out.println();
				if(sqlId % 4 == 3){
					sqlId -= 2;
				}
//				System.exit(0);
			}
		}
		if(success == false){
			MMain.retryThisInterval --;
		}
	}

	public boolean doSQLOnce(int threadId, int tableIndex, int sqlId, Object[] para, int paraNumber) {
		boolean success = true;
		try{
			int tableId = sqlId / 4;
			int queryId = sqlId % 4;
			switch(tableId){
			case 0: //customer
				if(queryId == 0){//select
					Tenant.tenants[threadId].stmt.execute("SELECT * FROM customer"+tableIndex+" WHERE c_id = "+para[0]+" AND c_w_id = "+para[1]+" AND c_d_id = "+para[2]);
				}else if(queryId == 1){//update
					Tenant.tenants[threadId].stmt.execute("UPDATE customer"+tableIndex+" SET c_id = "+para[0]+", c_d_id = "+para[1]+",c_w_id = "+para[2]+", c_first = '"+para[3]+"', c_middle = '"+para[4]+"', c_last = '"+para[5]+"', c_street_1 = '"+para[6]+"', c_street_2 = '"+para[7]+"',c_city = '"+para[8]+"',"
				+ "c_state = '"+para[9]+"',c_zip = '"+para[10]+"', c_phone = '"+para[11]+"',c_since = '"+para[12]+"', c_credit = '"+para[13]+"', c_credit_lim = "+para[14]+", c_discount = "+para[15]+", c_balance = "+para[16]+", c_ytd_payment = "+para[17]+",c_payment_cnt = "+para[18]+", c_delivery_cnt = "+para[19]+", c_data = '"+para[20]+"' "
				+ "WHERE c_id = "+para[21]+" AND c_w_id = "+para[22]+" AND c_d_id = "+para[23]);
				}else if(queryId == 2){//delete
					Tenant.tenants[threadId].stmt.execute("DELETE FROM customer"+tableIndex+" WHERE c_id = "+para[0]+" AND c_w_id = "+para[1]+" AND c_d_id = "+para[2]);
				}else if(queryId == 3){//insert
					Tenant.tenants[threadId].stmt.execute("INSERT INTO customer"+tableIndex+" VALUES ("+para[0]+","+para[1]+","+para[2]+",'"+para[3]+"','"+para[4]+"','"+para[5]+"','"+para[6]+"','"+para[7]+"','"+para[8]+"','"+para[9]+"','"+para[10]+"','"+para[11]+"','"+para[12]+"','"+para[13]+"',"+para[14]+","+para[15]+","+para[16]+","+para[17]+","+para[18]+","+para[19]+",'"+para[20]+"')");
				}
				break;
			case 1:
				if(queryId == 0){//select
					Tenant.tenants[threadId].stmt.execute("SELECT * FROM district"+tableIndex+" WHERE d_w_id = "+para[0]+" AND d_id = "+para[1]+"");
				}else if(queryId == 1){//update
					Tenant.tenants[threadId].stmt.execute("UPDATE district"+tableIndex+" SET d_id = "+para[0]+", d_w_id = "+para[1]+", d_name = '"+para[2]+"', d_street_1 = '"+para[3]+"', d_street_2 = '"+para[4]+"', d_city = '"+para[5]+"', d_state = '"+para[6]+"', d_zip = '"+para[7]+"', d_tax = "+para[8]+", d_ytd = "+para[9]+", d_next_o_id = "+para[10]+" WHERE d_w_id = "+para[11]+" AND d_id = "+para[12]+"");
				}else if(queryId == 2){//delete
					Tenant.tenants[threadId].stmt.execute("DELETE FROM district"+tableIndex+" WHERE d_w_id = "+para[0]+" AND d_id = "+para[1]+"");
				}else if(queryId == 3){//insert
					Tenant.tenants[threadId].stmt.execute("INSERT INTO district"+tableIndex+" VALUES ("+para[0]+","+para[1]+",'"+para[2]+"','"+para[3]+"','"+para[4]+"','"+para[5]+"','"+para[6]+"','"+para[7]+"',"+para[8]+","+para[9]+","+para[10]+")");
				}
				break;
			case 2:
				if(queryId == 0){//select
					Tenant.tenants[threadId].stmt.execute("SELECT * FROM item"+tableIndex+" WHERE i_id = "+para[0]+"");
				}else if(queryId == 1){//update
					Tenant.tenants[threadId].stmt.execute("UPDATE item"+tableIndex+" SET i_id = "+para[0]+", i_im_id = "+para[1]+", i_name = '"+para[2]+"', i_price = "+para[3]+", i_data = '"+para[4]+"' WHERE i_id = "+para[5]+"");
				}else if(queryId == 2){//delete
					Tenant.tenants[threadId].stmt.execute("DELETE FROM item"+tableIndex+" WHERE i_id = "+para[0]+"");
				}else if(queryId == 3){//insert
					Tenant.tenants[threadId].stmt.execute("INSERT INTO item"+tableIndex+" VALUES ("+para[0]+","+para[1]+",'"+para[2]+"',"+para[3]+",'"+para[4]+"')");
				}
				break;
			case 3:
				if(queryId == 0){//select
					Tenant.tenants[threadId].stmt.execute("SELECT * FROM new_orders"+tableIndex+" WHERE no_w_id = "+para[0]+" AND no_d_id = "+para[1]+" AND no_o_id = "+para[2]+"");
				}else if(queryId == 1){//update
					Tenant.tenants[threadId].stmt.execute("UPDATE new_orders"+tableIndex+" SET no_o_id = "+para[0]+",no_d_id = "+para[1]+",no_w_id = "+para[2]+" WHERE no_w_id = "+para[3]+" AND no_d_id = "+para[4]+" AND no_o_id = "+para[5]+"");
				}else if(queryId == 2){//delete
					Tenant.tenants[threadId].stmt.execute("DELETE FROM new_orders"+tableIndex+" WHERE no_w_id = "+para[0]+" AND no_d_id = "+para[1]+" AND no_o_id = "+para[2]+"");
				}else if(queryId == 3){//insert
					Tenant.tenants[threadId].stmt.execute("INSERT INTO new_orders"+tableIndex+" VALUES ("+para[0]+","+para[1]+","+para[2]+")");
				}
				break;
			case 4:
				if(queryId == 0){//select
					Tenant.tenants[threadId].stmt.execute("SELECT * FROM order_line"+tableIndex+" WHERE ol_w_id = "+para[0]+" AND ol_d_id = "+para[1]+" AND ol_o_id = "+para[2]+" AND ol_number = "+para[3]+"");
				}else if(queryId == 1){//update
					Tenant.tenants[threadId].stmt.execute("UPDATE order_line"+tableIndex+" SET ol_o_id = "+para[0]+", ol_d_id = "+para[1]+",ol_w_id = "+para[2]+",ol_number = "+para[3]+",ol_i_id = "+para[4]+", ol_supply_w_id = "+para[5]+",ol_delivery_d = '"+para[6]+"', ol_quantity = "+para[7]+", ol_amount = "+para[8]+", ol_dist_info = '"+para[9]+"' WHERE ol_w_id = "+para[10]+" AND ol_d_id = "+para[11]+" AND ol_o_id = "+para[12]+" AND ol_number = "+para[13]+"");
				}else if(queryId == 2){//delete
					Tenant.tenants[threadId].stmt.execute("DELETE FROM order_line"+tableIndex+" WHERE ol_w_id = "+para[0]+" AND ol_d_id = "+para[1]+" AND ol_o_id = "+para[2]+" AND ol_number = "+para[3]+"");
				}else if(queryId == 3){//insert
					Tenant.tenants[threadId].stmt.execute("INSERT INTO order_line"+tableIndex+" VALUES ("+para[0]+","+para[1]+","+para[2]+","+para[3]+","+para[4]+","+para[5]+",'"+para[6]+"',"+para[7]+","+para[8]+",'"+para[9]+"')");
				}
				break;
			case 5:
				if(queryId == 0){//select
					Tenant.tenants[threadId].stmt.execute("SELECT * FROM orders"+tableIndex+" WHERE o_w_id = "+para[0]+" AND o_d_id = "+para[1]+" AND o_id = "+para[2]+"");
				}else if(queryId == 1){//update
					Tenant.tenants[threadId].stmt.execute("UPDATE orders"+tableIndex+" SET o_id = "+para[0]+", o_d_id = "+para[1]+", o_w_id = "+para[2]+",o_c_id = "+para[3]+",o_entry_d = '"+para[4]+"',o_carrier_id = "+para[5]+",o_ol_cnt = "+para[6]+", o_all_local = "+para[7]+" WHERE o_w_id = "+para[8]+" AND o_d_id = "+para[9]+" AND o_id = "+para[10]+"");
				}else if(queryId == 2){//delete
					Tenant.tenants[threadId].stmt.execute("DELETE FROM orders"+tableIndex+" WHERE o_w_id = "+para[0]+" AND o_d_id = "+para[1]+" AND o_id = "+para[2]+"");
				}else if(queryId == 3){//insert
					Tenant.tenants[threadId].stmt.execute("INSERT INTO orders"+tableIndex+" VALUES ("+para[0]+","+para[1]+","+para[2]+","+para[3]+",'"+para[4]+"',"+para[5]+","+para[6]+","+para[7]+")");
				}
				break;
			case 6:
				if(queryId == 0){//select
					Tenant.tenants[threadId].stmt.execute("SELECT * FROM stock"+tableIndex+" WHERE s_w_id = "+para[0]+" AND s_i_id = "+para[1]+"");
				}else if(queryId == 1){//update
					Tenant.tenants[threadId].stmt.execute("UPDATE stock"+tableIndex+" SET s_i_id = "+para[0]+", s_w_id = "+para[1]+", s_quantity = "+para[2]+", s_dist_01 = '"+para[3]+"', s_dist_02 = '"+para[4]+"',s_dist_03 = '"+para[5]+"',s_dist_04 = '"+para[6]+"', s_dist_05 = '"+para[7]+"', s_dist_06 = '"+para[8]+"', s_dist_07 = '"+para[9]+"', s_dist_08 = '"+para[10]+"', s_dist_09 = '"+para[11]+"', s_dist_10 = '"+para[12]+"', s_ytd = "+para[13]+", s_order_cnt = "+para[14]+", s_remote_cnt = "+para[15]+",s_data = '"+para[16]+"' WHERE s_w_id = "+para[17]+" AND s_i_id = "+para[18]+"");
				}else if(queryId == 2){//delete
					Tenant.tenants[threadId].stmt.execute("DELETE FROM stock"+tableIndex+" WHERE s_w_id = "+para[0]+" AND s_i_id = "+para[1]+"");
				}else if(queryId == 3){//insert
					Tenant.tenants[threadId].stmt.execute("INSERT INTO stock"+tableIndex+" VALUES ("+para[0]+","+para[1]+","+para[2]+",'"+para[3]+"','"+para[4]+"','"+para[5]+"','"+para[6]+"','"+para[7]+"','"+para[8]+"','"+para[9]+"','"+para[10]+"','"+para[11]+"','"+para[12]+"',"+para[13]+","+para[14]+","+para[15]+",'"+para[16]+"')");
				}
				break;
			case 7:
				if(queryId == 0){//select
					Tenant.tenants[threadId].stmt.execute("SELECT * FROM warehouse"+tableIndex+" WHERE w_id = "+para[0]+"");
				}else if(queryId == 1){//update
					Tenant.tenants[threadId].stmt.execute("UPDATE warehouse"+tableIndex+" SET w_id = "+para[0]+",	w_name = '"+para[1]+"',w_street_1 = '"+para[2]+"',w_street_2 = '"+para[3]+"',w_city = '"+para[4]+"',w_state = '"+para[5]+"',w_zip = '"+para[6]+"',w_tax = "+para[7]+",	w_ytd = "+para[8]+" WHERE w_id = "+para[9]+"");
				}else if(queryId == 2){//delete
					Tenant.tenants[threadId].stmt.execute("DELETE FROM warehouse"+tableIndex+" WHERE w_id = "+para[0]+"");
				}else if(queryId == 3){//insert
					Tenant.tenants[threadId].stmt.execute("INSERT INTO warehouse"+tableIndex+" VALUES ("+para[0]+",'"+para[1]+"','"+para[2]+"','"+para[3]+"','"+para[4]+"','"+para[5]+"','"+para[6]+"',"+para[7]+","+para[8]+")");
				}
				break;
			case 8:
				if(queryId == 0){//select
					Tenant.tenants[threadId].stmt.execute("SELECT * FROM history"+tableIndex+" WHERE h_c_id = "+para[0]+" AND h_c_d_id = "+para[1]+" AND h_c_w_id = "+para[2]+"");
				}else if(queryId == 1){//update
					Tenant.tenants[threadId].stmt.execute("UPDATE history"+tableIndex+" SET h_c_id = "+para[0]+", h_c_d_id = "+para[1]+", h_c_w_id = "+para[2]+",h_d_id = "+para[3]+",h_w_id = "+para[4]+",h_date = '"+para[5]+"',h_amount = "+para[6]+",h_data = '"+para[7]+"' WHERE h_c_id = "+para[8]+" AND h_c_d_id = "+para[9]+" AND h_c_w_id = "+para[10]+"");
				}else if(queryId == 2){//delete
					Tenant.tenants[threadId].stmt.execute("DELETE FROM history"+tableIndex+" WHERE h_c_id = "+para[0]+" AND h_c_d_id = "+para[1]+" AND h_c_w_id = "+para[2]+"");
				}else if(queryId == 3){//insert
					Tenant.tenants[threadId].stmt.execute("INSERT INTO history"+tableIndex+" VALUES ("+para[0]+","+para[1]+","+para[2]+","+para[3]+","+para[4]+",'"+para[5]+"',"+para[6]+",'"+para[7]+"')");
				}
				break;
			}
			return success;
		}catch(SQLException e){
//			e.printStackTrace();
//			System.out.println("sql failure! tenant id: "+threadId+", table id: "+tableIndex+", sql id: "+sqlId+".");
			return false;
		}
	}
	
	public boolean doSQLOnce_25(int threadId, int tableIndex, int sqlId, Object[] para, int paraNumber) {
		boolean success = true;
		try{
			int tableId = sqlId / 4;
			int queryId = 0;
			switch(tableId){
			case 0: //customer
				if(queryId == 0){//select
					Tenant.tenants[threadId].stmt.execute("SELECT "+select25[tableId]+" FROM customer"+tableIndex+" WHERE c_id = "+para[0]+" AND c_w_id = "+para[1]+" AND c_d_id = "+para[2]);
				}else if(queryId == 1){//update
					Tenant.tenants[threadId].stmt.execute("UPDATE customer"+tableIndex+" SET c_id = "+para[0]+", c_d_id = "+para[1]+",c_w_id = "+para[2]+", c_first = '"+para[3]+"', c_middle = '"+para[4]+"', c_last = '"+para[5]+"', c_street_1 = '"+para[6]+"', c_street_2 = '"+para[7]+"',c_city = '"+para[8]+"',"
				+ "c_state = '"+para[9]+"',c_zip = '"+para[10]+"', c_phone = '"+para[11]+"',c_since = '"+para[12]+"', c_credit = '"+para[13]+"', c_credit_lim = "+para[14]+", c_discount = "+para[15]+", c_balance = "+para[16]+", c_ytd_payment = "+para[17]+",c_payment_cnt = "+para[18]+", c_delivery_cnt = "+para[19]+", c_data = '"+para[20]+"' "
				+ "WHERE c_id = "+para[21]+" AND c_w_id = "+para[22]+" AND c_d_id = "+para[23]);
				}
				break;
			case 1:
				if(queryId == 0){//select
					Tenant.tenants[threadId].stmt.execute("SELECT "+select25[tableId]+" FROM district"+tableIndex+" WHERE d_w_id = "+para[0]+" AND d_id = "+para[1]+"");
				}else if(queryId == 1){//update
					Tenant.tenants[threadId].stmt.execute("UPDATE district"+tableIndex+" SET d_id = "+para[0]+", d_w_id = "+para[1]+", d_name = '"+para[2]+"', d_street_1 = '"+para[3]+"', d_street_2 = '"+para[4]+"', d_city = '"+para[5]+"', d_state = '"+para[6]+"', d_zip = '"+para[7]+"', d_tax = "+para[8]+", d_ytd = "+para[9]+", d_next_o_id = "+para[10]+" WHERE d_w_id = "+para[11]+" AND d_id = "+para[12]+"");
				}
				break;
			case 2:
				if(queryId == 0){//select
					Tenant.tenants[threadId].stmt.execute("SELECT "+select25[tableId]+" FROM item"+tableIndex+" WHERE i_id = "+para[0]+"");
				}else if(queryId == 1){//update
					Tenant.tenants[threadId].stmt.execute("UPDATE item"+tableIndex+" SET i_id = "+para[0]+", i_im_id = "+para[1]+", i_name = '"+para[2]+"', i_price = "+para[3]+", i_data = '"+para[4]+"' WHERE i_id = "+para[5]+"");
				}
				break;
			case 3:
				if(queryId == 0){//select
					Tenant.tenants[threadId].stmt.execute("SELECT "+select25[tableId]+" FROM new_orders"+tableIndex+" WHERE no_w_id = "+para[0]+" AND no_d_id = "+para[1]+" AND no_o_id = "+para[2]+"");
				}else if(queryId == 1){//update
					Tenant.tenants[threadId].stmt.execute("UPDATE new_orders"+tableIndex+" SET no_o_id = "+para[0]+",no_d_id = "+para[1]+",no_w_id = "+para[2]+" WHERE no_w_id = "+para[3]+" AND no_d_id = "+para[4]+" AND no_o_id = "+para[5]+"");
				}
				break;
			case 4:
				if(queryId == 0){//select
					Tenant.tenants[threadId].stmt.execute("SELECT "+select25[tableId]+" FROM order_line"+tableIndex+" WHERE ol_w_id = "+para[0]+" AND ol_d_id = "+para[1]+" AND ol_o_id = "+para[2]+" AND ol_number = "+para[3]+"");
				}else if(queryId == 1){//update
					Tenant.tenants[threadId].stmt.execute("UPDATE order_line"+tableIndex+" SET ol_o_id = "+para[0]+", ol_d_id = "+para[1]+",ol_w_id = "+para[2]+",ol_number = "+para[3]+",ol_i_id = "+para[4]+", ol_supply_w_id = "+para[5]+",ol_delivery_d = '"+para[6]+"', ol_quantity = "+para[7]+", ol_amount = "+para[8]+", ol_dist_info = '"+para[9]+"' WHERE ol_w_id = "+para[10]+" AND ol_d_id = "+para[11]+" AND ol_o_id = "+para[12]+" AND ol_number = "+para[13]+"");
				}
				break;
			case 5:
				if(queryId == 0){//select
					Tenant.tenants[threadId].stmt.execute("SELECT "+select25[tableId]+" FROM orders"+tableIndex+" WHERE o_w_id = "+para[0]+" AND o_d_id = "+para[1]+" AND o_id = "+para[2]+"");
				}else if(queryId == 1){//update
					Tenant.tenants[threadId].stmt.execute("UPDATE orders"+tableIndex+" SET o_id = "+para[0]+", o_d_id = "+para[1]+", o_w_id = "+para[2]+",o_c_id = "+para[3]+",o_entry_d = '"+para[4]+"',o_carrier_id = "+para[5]+",o_ol_cnt = "+para[6]+", o_all_local = "+para[7]+" WHERE o_w_id = "+para[8]+" AND o_d_id = "+para[9]+" AND o_id = "+para[10]+"");
				}
				break;
			case 6:
				if(queryId == 0){//select
					Tenant.tenants[threadId].stmt.execute("SELECT "+select25[tableId]+" FROM stock"+tableIndex+" WHERE s_w_id = "+para[0]+" AND s_i_id = "+para[1]+"");
				}else if(queryId == 1){//update
					Tenant.tenants[threadId].stmt.execute("UPDATE stock"+tableIndex+" SET s_i_id = "+para[0]+", s_w_id = "+para[1]+", s_quantity = "+para[2]+", s_dist_01 = '"+para[3]+"', s_dist_02 = '"+para[4]+"',s_dist_03 = '"+para[5]+"',s_dist_04 = '"+para[6]+"', s_dist_05 = '"+para[7]+"', s_dist_06 = '"+para[8]+"', s_dist_07 = '"+para[9]+"', s_dist_08 = '"+para[10]+"', s_dist_09 = '"+para[11]+"', s_dist_10 = '"+para[12]+"', s_ytd = "+para[13]+", s_order_cnt = "+para[14]+", s_remote_cnt = "+para[15]+",s_data = '"+para[16]+"' WHERE s_w_id = "+para[17]+" AND s_i_id = "+para[18]+"");
				}
				break;
			case 7:
				if(queryId == 0){//select
					Tenant.tenants[threadId].stmt.execute("SELECT "+select25[tableId]+" FROM warehouse"+tableIndex+" WHERE w_id = "+para[0]+"");
				}else if(queryId == 1){//update
					Tenant.tenants[threadId].stmt.execute("UPDATE warehouse"+tableIndex+" SET w_id = "+para[0]+",	w_name = '"+para[1]+"',w_street_1 = '"+para[2]+"',w_street_2 = '"+para[3]+"',w_city = '"+para[4]+"',w_state = '"+para[5]+"',w_zip = '"+para[6]+"',w_tax = "+para[7]+",	w_ytd = "+para[8]+" WHERE w_id = "+para[9]+"");
				}
				break;
			case 8:
				if(queryId == 0){//select
					Tenant.tenants[threadId].stmt.execute("SELECT "+select25[tableId]+" FROM history"+tableIndex+" WHERE h_c_id = "+para[0]+" AND h_c_d_id = "+para[1]+" AND h_c_w_id = "+para[2]+"");
				}else if(queryId == 1){//update
					Tenant.tenants[threadId].stmt.execute("UPDATE history"+tableIndex+" SET h_c_id = "+para[0]+", h_c_d_id = "+para[1]+", h_c_w_id = "+para[2]+",h_d_id = "+para[3]+",h_w_id = "+para[4]+",h_date = '"+para[5]+"',h_amount = "+para[6]+",h_data = '"+para[7]+"' WHERE h_c_id = "+para[8]+" AND h_c_d_id = "+para[9]+" AND h_c_w_id = "+para[10]+"");
				}
				break;
			}
			return success;
		}catch(SQLException e){
//			e.printStackTrace();
			return false;
		}
	}
	
	public boolean doSQLOnce_50(int threadId, int tableIndex, int sqlId, Object[] para, int paraNumber) {
		boolean success = true;
		try{
			int tableId = sqlId / 4;
			int queryId = 0;
			switch(tableId){
			case 0: //customer
				if(queryId == 0){//select
					Tenant.tenants[threadId].stmt.execute("SELECT "+select50[tableId]+" FROM customer"+tableIndex+" WHERE c_id = "+para[0]+" AND c_w_id = "+para[1]+" AND c_d_id = "+para[2]);
				}else if(queryId == 1){//update
					Tenant.tenants[threadId].stmt.execute("UPDATE customer"+tableIndex+" SET c_id = "+para[0]+", c_d_id = "+para[1]+",c_w_id = "+para[2]+", c_first = '"+para[3]+"', c_middle = '"+para[4]+"', c_last = '"+para[5]+"', c_street_1 = '"+para[6]+"', c_street_2 = '"+para[7]+"',c_city = '"+para[8]+"',"
				+ "c_state = '"+para[9]+"',c_zip = '"+para[10]+"', c_phone = '"+para[11]+"',c_since = '"+para[12]+"', c_credit = '"+para[13]+"', c_credit_lim = "+para[14]+", c_discount = "+para[15]+", c_balance = "+para[16]+", c_ytd_payment = "+para[17]+",c_payment_cnt = "+para[18]+", c_delivery_cnt = "+para[19]+", c_data = '"+para[20]+"' "
				+ "WHERE c_id = "+para[21]+" AND c_w_id = "+para[22]+" AND c_d_id = "+para[23]);
				}
				break;
			case 1:
				if(queryId == 0){//select
					Tenant.tenants[threadId].stmt.execute("SELECT "+select50[tableId]+" FROM district"+tableIndex+" WHERE d_w_id = "+para[0]+" AND d_id = "+para[1]+"");
				}else if(queryId == 1){//update
					Tenant.tenants[threadId].stmt.execute("UPDATE district"+tableIndex+" SET d_id = "+para[0]+", d_w_id = "+para[1]+", d_name = '"+para[2]+"', d_street_1 = '"+para[3]+"', d_street_2 = '"+para[4]+"', d_city = '"+para[5]+"', d_state = '"+para[6]+"', d_zip = '"+para[7]+"', d_tax = "+para[8]+", d_ytd = "+para[9]+", d_next_o_id = "+para[10]+" WHERE d_w_id = "+para[11]+" AND d_id = "+para[12]+"");
				}
				break;
			case 2:
				if(queryId == 0){//select
					Tenant.tenants[threadId].stmt.execute("SELECT "+select50[tableId]+" FROM item"+tableIndex+" WHERE i_id = "+para[0]+"");
				}else if(queryId == 1){//update
					Tenant.tenants[threadId].stmt.execute("UPDATE item"+tableIndex+" SET i_id = "+para[0]+", i_im_id = "+para[1]+", i_name = '"+para[2]+"', i_price = "+para[3]+", i_data = '"+para[4]+"' WHERE i_id = "+para[5]+"");
				}
				break;
			case 3:
				if(queryId == 0){//select
					Tenant.tenants[threadId].stmt.execute("SELECT "+select50[tableId]+" FROM new_orders"+tableIndex+" WHERE no_w_id = "+para[0]+" AND no_d_id = "+para[1]+" AND no_o_id = "+para[2]+"");
				}else if(queryId == 1){//update
					Tenant.tenants[threadId].stmt.execute("UPDATE new_orders"+tableIndex+" SET no_o_id = "+para[0]+",no_d_id = "+para[1]+",no_w_id = "+para[2]+" WHERE no_w_id = "+para[3]+" AND no_d_id = "+para[4]+" AND no_o_id = "+para[5]+"");
				}
				break;
			case 4:
				if(queryId == 0){//select
					Tenant.tenants[threadId].stmt.execute("SELECT "+select50[tableId]+" FROM order_line"+tableIndex+" WHERE ol_w_id = "+para[0]+" AND ol_d_id = "+para[1]+" AND ol_o_id = "+para[2]+" AND ol_number = "+para[3]+"");
				}else if(queryId == 1){//update
					Tenant.tenants[threadId].stmt.execute("UPDATE order_line"+tableIndex+" SET ol_o_id = "+para[0]+", ol_d_id = "+para[1]+",ol_w_id = "+para[2]+",ol_number = "+para[3]+",ol_i_id = "+para[4]+", ol_supply_w_id = "+para[5]+",ol_delivery_d = '"+para[6]+"', ol_quantity = "+para[7]+", ol_amount = "+para[8]+", ol_dist_info = '"+para[9]+"' WHERE ol_w_id = "+para[10]+" AND ol_d_id = "+para[11]+" AND ol_o_id = "+para[12]+" AND ol_number = "+para[13]+"");
				}
				break;
			case 5:
				if(queryId == 0){//select
					Tenant.tenants[threadId].stmt.execute("SELECT "+select50[tableId]+" FROM orders"+tableIndex+" WHERE o_w_id = "+para[0]+" AND o_d_id = "+para[1]+" AND o_id = "+para[2]+"");
				}else if(queryId == 1){//update
					Tenant.tenants[threadId].stmt.execute("UPDATE orders"+tableIndex+" SET o_id = "+para[0]+", o_d_id = "+para[1]+", o_w_id = "+para[2]+",o_c_id = "+para[3]+",o_entry_d = '"+para[4]+"',o_carrier_id = "+para[5]+",o_ol_cnt = "+para[6]+", o_all_local = "+para[7]+" WHERE o_w_id = "+para[8]+" AND o_d_id = "+para[9]+" AND o_id = "+para[10]+"");
				}
				break;
			case 6:
				if(queryId == 0){//select
					Tenant.tenants[threadId].stmt.execute("SELECT "+select50[tableId]+" FROM stock"+tableIndex+" WHERE s_w_id = "+para[0]+" AND s_i_id = "+para[1]+"");
				}else if(queryId == 1){//update
					Tenant.tenants[threadId].stmt.execute("UPDATE stock"+tableIndex+" SET s_i_id = "+para[0]+", s_w_id = "+para[1]+", s_quantity = "+para[2]+", s_dist_01 = '"+para[3]+"', s_dist_02 = '"+para[4]+"',s_dist_03 = '"+para[5]+"',s_dist_04 = '"+para[6]+"', s_dist_05 = '"+para[7]+"', s_dist_06 = '"+para[8]+"', s_dist_07 = '"+para[9]+"', s_dist_08 = '"+para[10]+"', s_dist_09 = '"+para[11]+"', s_dist_10 = '"+para[12]+"', s_ytd = "+para[13]+", s_order_cnt = "+para[14]+", s_remote_cnt = "+para[15]+",s_data = '"+para[16]+"' WHERE s_w_id = "+para[17]+" AND s_i_id = "+para[18]+"");
				}
				break;
			case 7:
				if(queryId == 0){//select
					Tenant.tenants[threadId].stmt.execute("SELECT "+select50[tableId]+" FROM warehouse"+tableIndex+" WHERE w_id = "+para[0]+"");
				}else if(queryId == 1){//update
					Tenant.tenants[threadId].stmt.execute("UPDATE warehouse"+tableIndex+" SET w_id = "+para[0]+",	w_name = '"+para[1]+"',w_street_1 = '"+para[2]+"',w_street_2 = '"+para[3]+"',w_city = '"+para[4]+"',w_state = '"+para[5]+"',w_zip = '"+para[6]+"',w_tax = "+para[7]+",	w_ytd = "+para[8]+" WHERE w_id = "+para[9]+"");
				}
				break;
			case 8:
				if(queryId == 0){//select
					Tenant.tenants[threadId].stmt.execute("SELECT "+select50[tableId]+" FROM history"+tableIndex+" WHERE h_c_id = "+para[0]+" AND h_c_d_id = "+para[1]+" AND h_c_w_id = "+para[2]+"");
				}else if(queryId == 1){//update
					Tenant.tenants[threadId].stmt.execute("UPDATE history"+tableIndex+" SET h_c_id = "+para[0]+", h_c_d_id = "+para[1]+", h_c_w_id = "+para[2]+",h_d_id = "+para[3]+",h_w_id = "+para[4]+",h_date = '"+para[5]+"',h_amount = "+para[6]+",h_data = '"+para[7]+"' WHERE h_c_id = "+para[8]+" AND h_c_d_id = "+para[9]+" AND h_c_w_id = "+para[10]+"");
				}
				break;
			}
			return success;
		}catch(SQLException e){
			e.printStackTrace();
//			System.out.println("sql failure! tenant id: "+threadId+", table id: "+tableIndex+", sql id: "+sqlId+".");
			return false;
		}
	}
	
	public boolean doSQLOnce_75(int threadId, int tableIndex, int sqlId, Object[] para, int paraNumber) {
		boolean success = true;
		try{
			int tableId = sqlId / 4;
			int queryId = 0;
			switch(tableId){
			case 0: //customer
				if(queryId == 0){//select
					Tenant.tenants[threadId].stmt.execute("SELECT "+select75[tableId]+" FROM customer"+tableIndex+" WHERE c_id = "+para[0]+" AND c_w_id = "+para[1]+" AND c_d_id = "+para[2]);
				}else if(queryId == 1){//update
					Tenant.tenants[threadId].stmt.execute("UPDATE customer"+tableIndex+" SET c_id = "+para[0]+", c_d_id = "+para[1]+",c_w_id = "+para[2]+", c_first = '"+para[3]+"', c_middle = '"+para[4]+"', c_last = '"+para[5]+"', c_street_1 = '"+para[6]+"', c_street_2 = '"+para[7]+"',c_city = '"+para[8]+"',"
				+ "c_state = '"+para[9]+"',c_zip = '"+para[10]+"', c_phone = '"+para[11]+"',c_since = '"+para[12]+"', c_credit = '"+para[13]+"', c_credit_lim = "+para[14]+", c_discount = "+para[15]+", c_balance = "+para[16]+", c_ytd_payment = "+para[17]+",c_payment_cnt = "+para[18]+", c_delivery_cnt = "+para[19]+", c_data = '"+para[20]+"' "
				+ "WHERE c_id = "+para[21]+" AND c_w_id = "+para[22]+" AND c_d_id = "+para[23]);
				}
				break;
			case 1:
				if(queryId == 0){//select
					Tenant.tenants[threadId].stmt.execute("SELECT "+select75[tableId]+" FROM district"+tableIndex+" WHERE d_w_id = "+para[0]+" AND d_id = "+para[1]+"");
				}else if(queryId == 1){//update
					Tenant.tenants[threadId].stmt.execute("UPDATE district"+tableIndex+" SET d_id = "+para[0]+", d_w_id = "+para[1]+", d_name = '"+para[2]+"', d_street_1 = '"+para[3]+"', d_street_2 = '"+para[4]+"', d_city = '"+para[5]+"', d_state = '"+para[6]+"', d_zip = '"+para[7]+"', d_tax = "+para[8]+", d_ytd = "+para[9]+", d_next_o_id = "+para[10]+" WHERE d_w_id = "+para[11]+" AND d_id = "+para[12]+"");
				}
				break;
			case 2:
				if(queryId == 0){//select
					Tenant.tenants[threadId].stmt.execute("SELECT "+select75[tableId]+" FROM item"+tableIndex+" WHERE i_id = "+para[0]+"");
				}else if(queryId == 1){//update
					Tenant.tenants[threadId].stmt.execute("UPDATE item"+tableIndex+" SET i_id = "+para[0]+", i_im_id = "+para[1]+", i_name = '"+para[2]+"', i_price = "+para[3]+", i_data = '"+para[4]+"' WHERE i_id = "+para[5]+"");
				}
				break;
			case 3:
				if(queryId == 0){//select
					Tenant.tenants[threadId].stmt.execute("SELECT "+select75[tableId]+" FROM new_orders"+tableIndex+" WHERE no_w_id = "+para[0]+" AND no_d_id = "+para[1]+" AND no_o_id = "+para[2]+"");
				}else if(queryId == 1){//update
					Tenant.tenants[threadId].stmt.execute("UPDATE new_orders"+tableIndex+" SET no_o_id = "+para[0]+",no_d_id = "+para[1]+",no_w_id = "+para[2]+" WHERE no_w_id = "+para[3]+" AND no_d_id = "+para[4]+" AND no_o_id = "+para[5]+"");
				}
				break;
			case 4:
				if(queryId == 0){//select
					Tenant.tenants[threadId].stmt.execute("SELECT "+select75[tableId]+" FROM order_line"+tableIndex+" WHERE ol_w_id = "+para[0]+" AND ol_d_id = "+para[1]+" AND ol_o_id = "+para[2]+" AND ol_number = "+para[3]+"");
				}else if(queryId == 1){//update
					Tenant.tenants[threadId].stmt.execute("UPDATE order_line"+tableIndex+" SET ol_o_id = "+para[0]+", ol_d_id = "+para[1]+",ol_w_id = "+para[2]+",ol_number = "+para[3]+",ol_i_id = "+para[4]+", ol_supply_w_id = "+para[5]+",ol_delivery_d = '"+para[6]+"', ol_quantity = "+para[7]+", ol_amount = "+para[8]+", ol_dist_info = '"+para[9]+"' WHERE ol_w_id = "+para[10]+" AND ol_d_id = "+para[11]+" AND ol_o_id = "+para[12]+" AND ol_number = "+para[13]+"");
				}
				break;
			case 5:
				if(queryId == 0){//select
					Tenant.tenants[threadId].stmt.execute("SELECT "+select75[tableId]+" FROM orders"+tableIndex+" WHERE o_w_id = "+para[0]+" AND o_d_id = "+para[1]+" AND o_id = "+para[2]+"");
				}else if(queryId == 1){//update
					Tenant.tenants[threadId].stmt.execute("UPDATE orders"+tableIndex+" SET o_id = "+para[0]+", o_d_id = "+para[1]+", o_w_id = "+para[2]+",o_c_id = "+para[3]+",o_entry_d = '"+para[4]+"',o_carrier_id = "+para[5]+",o_ol_cnt = "+para[6]+", o_all_local = "+para[7]+" WHERE o_w_id = "+para[8]+" AND o_d_id = "+para[9]+" AND o_id = "+para[10]+"");
				}
				break;
			case 6:
				if(queryId == 0){//select
					Tenant.tenants[threadId].stmt.execute("SELECT "+select75[tableId]+" FROM stock"+tableIndex+" WHERE s_w_id = "+para[0]+" AND s_i_id = "+para[1]+"");
				}else if(queryId == 1){//update
					Tenant.tenants[threadId].stmt.execute("UPDATE stock"+tableIndex+" SET s_i_id = "+para[0]+", s_w_id = "+para[1]+", s_quantity = "+para[2]+", s_dist_01 = '"+para[3]+"', s_dist_02 = '"+para[4]+"',s_dist_03 = '"+para[5]+"',s_dist_04 = '"+para[6]+"', s_dist_05 = '"+para[7]+"', s_dist_06 = '"+para[8]+"', s_dist_07 = '"+para[9]+"', s_dist_08 = '"+para[10]+"', s_dist_09 = '"+para[11]+"', s_dist_10 = '"+para[12]+"', s_ytd = "+para[13]+", s_order_cnt = "+para[14]+", s_remote_cnt = "+para[15]+",s_data = '"+para[16]+"' WHERE s_w_id = "+para[17]+" AND s_i_id = "+para[18]+"");
				}
				break;
			case 7:
				if(queryId == 0){//select
					Tenant.tenants[threadId].stmt.execute("SELECT "+select75[tableId]+" FROM warehouse"+tableIndex+" WHERE w_id = "+para[0]+"");
				}else if(queryId == 1){//update
					Tenant.tenants[threadId].stmt.execute("UPDATE warehouse"+tableIndex+" SET w_id = "+para[0]+",	w_name = '"+para[1]+"',w_street_1 = '"+para[2]+"',w_street_2 = '"+para[3]+"',w_city = '"+para[4]+"',w_state = '"+para[5]+"',w_zip = '"+para[6]+"',w_tax = "+para[7]+",	w_ytd = "+para[8]+" WHERE w_id = "+para[9]+"");
				}
				break;
			case 8:
				if(queryId == 0){//select
					Tenant.tenants[threadId].stmt.execute("SELECT "+select75[tableId]+" FROM history"+tableIndex+" WHERE h_c_id = "+para[0]+" AND h_c_d_id = "+para[1]+" AND h_c_w_id = "+para[2]+"");
				}else if(queryId == 1){//update
					Tenant.tenants[threadId].stmt.execute("UPDATE history"+tableIndex+" SET h_c_id = "+para[0]+", h_c_d_id = "+para[1]+", h_c_w_id = "+para[2]+",h_d_id = "+para[3]+",h_w_id = "+para[4]+",h_date = '"+para[5]+"',h_amount = "+para[6]+",h_data = '"+para[7]+"' WHERE h_c_id = "+para[8]+" AND h_c_d_id = "+para[9]+" AND h_c_w_id = "+para[10]+"");
				}
				break;
			}
			return success;
		}catch(SQLException e){
			e.printStackTrace();
//			System.out.println("sql failure! tenant id: "+threadId+", table id: "+tableIndex+", sql id: "+sqlId+".");
			return false;
		}
	}
	
}
