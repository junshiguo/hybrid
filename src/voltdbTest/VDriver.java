package voltdbTest;

import java.util.Random;

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
	public static int QUERYiNSERT = 3;

	public Random ran = new Random();
	//ran = new Random();
	/* listed bellow are all attributes of each table in the database */
	/* warehouse */
	public int w_id;
	public String w_name, w_street_1, w_street_2, w_city, w_state,
			w_zip;
	public float w_tax;
	public String w_ytd = "3000000.00";
	/* district */
	public int d_id, d_w_id;
	public String d_name, d_street_1, d_street_2, d_city, d_state,
			d_zip;
	public  float d_tax;
	public  String d_ytd = "30000.0";
	public  String d_next_o_id = "3001L";
	/* customer */
	public  int c_id, c_d_id, c_w_id;
	public  String c_first, c_last, c_street_1, c_street_2, c_city,
			c_state, c_zip, c_phone, c_since, c_credit, c_data;
	public  double c_discount;
	public  String c_middle = "OE";
	public double c_ytd_payment = 10.0;
	public double c_balance = -10.0;
	public int c_credit_lim = 50000, c_payment_cnt = 1,
			c_delivery_cnt = 0;
	/* history */
	public int h_c_id, h_c_d_id, h_c_w_id, h_d_id, h_w_id;
	public String h_date, h_data;
	public double h_amount = 10.0;
	/* new_orders */
	public int no_o_id, no_d_id, no_w_id;
	/* orders */
	public int o_id, o_d_id, o_w_id, o_c_id, o_ol_cnt;
	public String o_entry_d, o_carrier_id;
	public int o_all_local = 1;
	/* order_line */
	public int ol_o_id, ol_d_id, ol_w_id, ol_number, ol_i_id,
			ol_supply_w_id;
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
	
	public VDriver(int Id) {
		Object[] para = new Object[25];
		int[] paraType = new int[25];
		Object[] tmpPara = new Object[25];
		int[] tmpParaType = new int[25];
		while (Driver.IsActive) {
			int seq = Tenant.tenants[Id].sequence.nextSequence();
			if(seq == 32){
				
				continue;
			}
			/**
			 * public static int CUSTOMER = 0;
	public static int DISTRICT = 1;
	public static int ITEM = 2;
	public static int NEW_ORDERS = 3;
	public static int ORDER_LINE = 4;
	public static int ORDERS = 5;
	public static int STOCK = 6;
	public static int WAREHOUSE = 7;
	public static int HISTORY = 8;
			 */
			int tableId = seq % 8;
			int queryId = seq / 8;
			switch(tableId){
			case 0: //customer
				c_id = Support.RandomNumber(1,Driver.CUST_PER_DIST);
				c_d_id = Support.RandomNumber(1,Driver.DIST_PER_WARE);
				c_w_id = Support.RandomNumber(min_ware,max_ware);
				c_first = Support.MakeAlphaString(8, 16);
				c_middle = "OE";
				c_last = Support.Lastname(Support.NURand(255,0,999));
				c_street_1 = Support.MakeAlphaString(10, 20);
				c_street_2 = Support.MakeAlphaString(10, 20);
				c_city = Support.MakeAlphaString(10, 20);
				c_state = Support.MakeAlphaString(2, 2);
				c_zip = Support.MakeNumberString(9, 9);
				c_phone = Support.MakeNumberString(16, 16);
				c_since = Support.getTimeStamp();
				c_credit = "C0";
				c_credit_lim = 50000;
				c_discount = ran.nextInt(50)/100.0;
				c_balance = 10.0;
				c_ytd_payment=10.0;
				c_payment_cnt = 1;
				c_delivery_cnt = 0;
				c_data = Support.MakeAlphaString(300, 500);
			}
			switch(queryId){
			case 0://select
			case 1://update
			case 2://delete
			case 3://insert
			}
			
			
		}
	}
	
}
