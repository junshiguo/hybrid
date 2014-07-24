package hybridTest;

import java.util.Random;

import utility.Support;

public class Driver {
	public static boolean IsActive = true;

	public static int min_ware = 1;
	public static int max_ware = 1;
	public static int num_ware = 1;
	public static int num_node = 0;
	public static int arg_offset = 0;
	/*********************associated with datasize***********************************/
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
	int[] paraType = new int[30];
	int paraNumber, PKNumber;
	
	public void initiatePara(int seq) {
		while (Driver.IsActive) { // initiate parameter
			int tableId = seq % 9;
			switch (tableId) {
			case 0: // customer
				para[0] = c_id = Support.RandomNumber(1, Driver.CUST_PER_DIST);		paraType[0] = Driver.TYPE_INT;
				para[1] = c_d_id = Support.RandomNumber(1, Driver.DIST_PER_WARE);	paraType[1] = Driver.TYPE_INT;
				para[2] = c_w_id = Support.RandomNumber(min_ware, max_ware);				paraType[2] = Driver.TYPE_INT;
				para[3] = c_first = Support.MakeAlphaString(8, 16);										paraType[3] = Driver.TYPE_STRING;
				para[4] = c_middle = "OE";		paraType[4] = Driver.TYPE_STRING;
				para[5] = c_last = Support.Lastname(Support.NURand(255, 0, 999)); paraType[5] = Driver.TYPE_STRING;
				para[6] = c_street_1 = Support.MakeAlphaString(10, 20);		paraType[6] = Driver.TYPE_STRING;
				para[7] = c_street_2 = Support.MakeAlphaString(10, 20);		paraType[7] = Driver.TYPE_STRING;
				para[8] = c_city = Support.MakeAlphaString(10, 20);				paraType[8] = Driver.TYPE_STRING;
				para[9] = c_state = Support.MakeAlphaString(2, 2);					paraType[9] = Driver.TYPE_STRING;
				para[10] = c_zip = Support.MakeNumberString(9, 9);					paraType[10] = Driver.TYPE_STRING;
				para[11] = c_phone = Support.MakeNumberString(16, 16);		paraType[11] = Driver.TYPE_STRING;
				para[12] = c_since = Support.getTimeStamp();									paraType[12] = Driver.TYPE_STRING;
				para[13] = c_credit = "C0";															paraType[13] = Driver.TYPE_STRING;
				para[14] = c_credit_lim = 50000;												paraType[14] = Driver.TYPE_INT;
				para[15] = c_discount = ran.nextInt(50) / 100.0;	paraType[15] = Driver.TYPE_DOUBLE;
				para[16] = c_balance = 10.0;							paraType[16] = Driver.TYPE_DOUBLE;
				para[17] = c_ytd_payment = 10.0;				paraType[17] = Driver.TYPE_DOUBLE;
				para[18] = c_payment_cnt = 1;						paraType[18] = Driver.TYPE_INT;
				para[19] = c_delivery_cnt = 0;					paraType[19] = Driver.TYPE_INT;
				para[20] = c_data = Support.MakeAlphaString(300, 500);		paraType[20] = Driver.TYPE_STRING;
				para[21] = 0;							paraType[21] = Driver.TYPE_INT;
				para[22] = 0;							paraType[22] = Driver.TYPE_INT;
				para[23] = c_id;					paraType[23] = Driver.TYPE_INT;
				para[24] = c_w_id;			paraType[24] = Driver.TYPE_INT;
				para[25] = c_d_id;			paraType[25] = Driver.TYPE_INT;
				paraNumber = 26;
				PKNumber = 3;
				break;
			case 1:// district
				para[0] = d_id = Support.RandomNumber(1, Driver.DIST_PER_WARE);
				para[1] = d_w_id = Support.RandomNumber(min_ware, max_ware);
				para[2] = d_name = Support.MakeAlphaString(6, 10);
				para[3] = d_street_1 = Support.MakeAlphaString(10, 20);
				para[4] = d_street_2 = Support.MakeAlphaString(10, 20);
				para[5] = d_city = Support.MakeAlphaString(10, 20);
				para[6] = d_state = Support.MakeAlphaString(2, 2);
				para[7] = d_zip = Support.MakeNumberString(9, 9);
				para[8] = d_tax = (float) (ran.nextInt(20) / 100.0);
				para[9] = d_ytd = 30000.0;
				para[10] = d_next_o_id = 3001;
				para[11] = 0;
				para[12] = 1;
				para[13] = d_w_id;
				para[14] = d_id;
				paraType[0] = Driver.TYPE_INT;
				paraType[1] = Driver.TYPE_INT;
				paraType[2] = Driver.TYPE_STRING;
				paraType[3] = Driver.TYPE_STRING;
				paraType[4] = Driver.TYPE_STRING;
				paraType[5] = Driver.TYPE_STRING;
				paraType[6] = Driver.TYPE_STRING;
				paraType[7] = Driver.TYPE_STRING;
				paraType[8] = Driver.TYPE_DOUBLE;
				paraType[9] = Driver.TYPE_DOUBLE;
				paraType[10] = Driver.TYPE_INT;
				paraType[11] = Driver.TYPE_INT;
				paraType[12] = Driver.TYPE_INT;
				paraType[13] = Driver.TYPE_INT;
				paraType[14] = Driver.TYPE_INT;
				paraNumber = 15;
				PKNumber = 2;
				break;
			case 2:// item
				para[0] = i_id = Support.RandomNumber(1, MAXITEMS);
				para[1] = i_im_id = ran.nextInt(9999) + 1;
				para[2] = i_name = Support.MakeAlphaString(14, 24);
				para[3] = i_price = (ran.nextInt(9900) + 100) / 100.0;
				para[4] = i_data = Support.MakeAlphaString(26, 50);
				para[5] = 0;
				para[6] = 1;
				para[7] = i_id;
				paraType[0] = Driver.TYPE_INT;
				paraType[1] = Driver.TYPE_INT;
				paraType[2] = Driver.TYPE_STRING;
				paraType[3] = Driver.TYPE_DOUBLE;
				paraType[4] = Driver.TYPE_STRING;
				paraType[5] = Driver.TYPE_INT;
				paraType[6] = Driver.TYPE_INT;
				paraType[7] = Driver.TYPE_INT;
				paraNumber = 8;
				PKNumber = 1;
				break;
			case 3:// new_orders
				para[0] = no_o_id = Support.RandomNumber(1, Driver.ORD_PER_DIST);
				para[1] = no_d_id = Support.RandomNumber(1, Driver.DIST_PER_WARE);
				para[2] = no_w_id = Support.RandomNumber(min_ware, max_ware);
				para[3] = 0;
				para[4] = 1;
				para[5] = no_w_id;
				para[6] = no_d_id;
				para[7] = no_o_id;
				paraType[0] = Driver.TYPE_INT;
				paraType[1] = Driver.TYPE_INT;
				paraType[2] = Driver.TYPE_INT;
				paraType[3] = Driver.TYPE_INT;
				paraType[4] = Driver.TYPE_INT;
				paraType[5] = Driver.TYPE_INT;
				paraType[6] = Driver.TYPE_INT;
				paraType[7] = Driver.TYPE_INT;
				paraNumber = 8;
				PKNumber = 3;
				break;
			case 4:// order_line
				para[0] = ol_o_id = Support.RandomNumber(1, Driver.ORD_PER_DIST);
				para[1] = ol_d_id = Support.RandomNumber(1, Driver.DIST_PER_WARE);
				para[2] = ol_w_id = Support.RandomNumber(min_ware, max_ware);
				para[3] = ol_number = Support.RandomNumber(1, 15);
				para[4] = ol_i_id = Support.RandomNumber(1, Driver.MAXITEMS);
				para[5] = ol_supply_w_id = Support.RandomNumber(min_ware, max_ware);
				para[6] = ol_delivery_d = Support.getTimeStamp();
				para[7] = ol_quantity = 5;
				para[8] = ol_amount = 0.0;
				para[9] = ol_dist_info = Support.MakeAlphaString(24, 24);
				para[10] = 0;
				para[11] = 1;
				para[12] = ol_w_id;
				para[13] = ol_d_id;
				para[14] = ol_o_id;
				para[15] = ol_number;
				paraType[0] = Driver.TYPE_INT;
				paraType[1] = Driver.TYPE_INT;
				paraType[2] = Driver.TYPE_INT;
				paraType[3] = Driver.TYPE_INT;
				paraType[4] = Driver.TYPE_INT;
				paraType[5] = Driver.TYPE_INT;
				paraType[6] = Driver.TYPE_STRING;
				paraType[7] = Driver.TYPE_INT;
				paraType[8] = Driver.TYPE_DOUBLE;
				paraType[9] = Driver.TYPE_STRING;
				paraType[10] = Driver.TYPE_INT;
				paraType[11] = Driver.TYPE_INT;
				paraType[12] = Driver.TYPE_INT;
				paraType[13] = Driver.TYPE_INT;
				paraType[14] = Driver.TYPE_INT;
				paraType[15] = Driver.TYPE_INT;
				paraNumber = 16;
				PKNumber = 4;
				break;
			case 5:// orders
				para[0] = o_id = Support.RandomNumber(1, Driver.ORD_PER_DIST);
				para[1] = o_d_id = Support.RandomNumber(1, Driver.DIST_PER_WARE);
				para[2] = o_w_id = Support.RandomNumber(min_ware, max_ware);
				para[3] = o_c_id = Support.RandomNumber(1, Driver.ORD_PER_DIST);
				para[4] = o_entry_d = Support.getTimeStamp();
				para[5] = o_carrier_id = Support.RandomNumber(1, 10);
				para[6] = o_ol_cnt = Support.RandomNumber(5, 15);
				para[7] = o_all_local = ran.nextInt(100) == 1 ? 0 : 1;
				para[8] = 0;
				para[9] = 1;
				para[10] = o_w_id;
				para[11] = o_d_id;
				para[12] = o_id;
				paraType[0] = Driver.TYPE_INT;
				paraType[1] = Driver.TYPE_INT;
				paraType[2] = Driver.TYPE_INT;
				paraType[3] = Driver.TYPE_INT;
				paraType[4] = Driver.TYPE_STRING;
				paraType[5] = Driver.TYPE_INT;
				paraType[6] = Driver.TYPE_INT;
				paraType[7] = Driver.TYPE_INT;
				paraType[8] = Driver.TYPE_INT;
				paraType[9] = Driver.TYPE_INT;
				paraType[10] = Driver.TYPE_INT;
				paraType[11] = Driver.TYPE_INT;
				paraType[12] = Driver.TYPE_INT;
				paraNumber = 13;
				PKNumber = 3;
				break;
			case 6:// stock
				para[0] = s_i_id = Support.RandomNumber(1, MAXITEMS);
				para[1] = s_w_id = Support.RandomNumber(min_ware, max_ware);
				para[2] = s_quantity = Support.RandomNumber(10, 100);
				para[3] = s_dist_01 = Support.MakeAlphaString(24, 24);
				para[4] = s_dist_02 = Support.MakeAlphaString(24, 24);
				para[5] = s_dist_03 = Support.MakeAlphaString(24, 24);
				para[6] = s_dist_04 = Support.MakeAlphaString(24, 24);
				para[7] = s_dist_05 = Support.MakeAlphaString(24, 24);
				para[8] = s_dist_06 = Support.MakeAlphaString(24, 24);
				para[9] = s_dist_07 = Support.MakeAlphaString(24, 24);
				para[10] = s_dist_08 = Support.MakeAlphaString(24, 24);
				para[11] = s_dist_09 = Support.MakeAlphaString(24, 24);
				para[12] = s_dist_10 = Support.MakeAlphaString(24, 24);
				para[13] = s_ytd = 0;
				para[14] = s_order_cnt = 0;
				para[15] = s_remote_cnt = 0;
				para[16] = s_data = Support.MakeAlphaString(26, 50);
				para[17] = 0;
				para[18] = 1;
				para[19] = s_w_id;
				para[20] = s_i_id;
				paraType[0] = Driver.TYPE_INT;
				paraType[1] = Driver.TYPE_INT;
				paraType[2] = Driver.TYPE_INT;
				paraType[3] = Driver.TYPE_STRING;
				paraType[4] = Driver.TYPE_STRING;
				paraType[5] = Driver.TYPE_STRING;
				paraType[6] = Driver.TYPE_STRING;
				paraType[7] = Driver.TYPE_STRING;
				paraType[8] = Driver.TYPE_STRING;
				paraType[9] = Driver.TYPE_STRING;
				paraType[10] = Driver.TYPE_STRING;
				paraType[11] = Driver.TYPE_STRING;
				paraType[12] = Driver.TYPE_STRING;
				paraType[13] = Driver.TYPE_INT;
				paraType[14] = Driver.TYPE_INT;
				paraType[15] = Driver.TYPE_INT;
				paraType[16] = Driver.TYPE_STRING;
				paraType[17] = Driver.TYPE_INT;
				paraType[18] = Driver.TYPE_INT;
				paraType[19] = Driver.TYPE_INT;
				paraType[20] = Driver.TYPE_INT;
				paraNumber = 21;
				PKNumber = 2;
				break;
			case 7:// warehouse
				para[0] = w_id = Support.RandomNumber(min_ware, max_ware);
				para[1] = w_name = Support.MakeAlphaString(6, 10);
				para[2] = w_street_1 = Support.MakeAlphaString(10, 20);
				para[3] = w_street_2 = Support.MakeAlphaString(10, 20);
				para[4] = w_city = Support.MakeAlphaString(10, 20);
				para[5] = w_state = Support.MakeAlphaString(2, 2);
				para[6] = w_zip = Support.MakeNumberString(9, 9);
				para[7] = w_tax = (float) (Support.RandomNumber(10, 20) / 100.0);
				para[8] = w_ytd = 3000000.00;
				para[9] = 0;
				para[10] = 1;
				para[11] = w_id;
				paraType[0] = Driver.TYPE_INT;
				paraType[1] = Driver.TYPE_STRING;
				paraType[2] = Driver.TYPE_STRING;
				paraType[3] = Driver.TYPE_STRING;
				paraType[4] = Driver.TYPE_STRING;
				paraType[5] = Driver.TYPE_STRING;
				paraType[6] = Driver.TYPE_STRING;
				paraType[7] = Driver.TYPE_FLOAT;
				paraType[8] = Driver.TYPE_DOUBLE;
				paraType[9] = Driver.TYPE_INT;
				paraType[10] = Driver.TYPE_INT;
				paraType[11] = Driver.TYPE_INT;
				paraNumber = 12;
				PKNumber = 1;
				break;
			case 8: //history
				para[0] = h_c_id = Support.RandomNumber(1,Driver.CUST_PER_DIST);
				para[1] = h_c_d_id = Support.RandomNumber(1,Driver.DIST_PER_WARE);
				para[2] = h_c_w_id = Support.RandomNumber(min_ware,max_ware);
				para[3] = h_d_id = h_c_d_id;
				para[4] = h_w_id = h_c_w_id;
				para[5] = h_date = Support.getTimeStamp();
				para[6] = h_amount = 10.0;
				para[7] = h_data = Support.MakeAlphaString(12,24);
				para[8] = 0;
				para[9] = 1;
				para[10] = h_c_id;
				para[11] = h_c_d_id;
				para[12] = h_c_w_id;
				paraType[0] = Driver.TYPE_INT;
				paraType[1] = Driver.TYPE_INT;
				paraType[2] = Driver.TYPE_INT;
				paraType[3] = Driver.TYPE_INT;
				paraType[4] = Driver.TYPE_INT;
				paraType[5] = Driver.TYPE_STRING;
				paraType[6] = Driver.TYPE_DOUBLE;
				paraType[7] = Driver.TYPE_STRING;
				paraType[8] = Driver.TYPE_INT;
				paraType[9] = Driver.TYPE_INT;
				paraType[10] = Driver.TYPE_INT;
				paraType[11] = Driver.TYPE_INT;
				paraType[12] = Driver.TYPE_INT;
				paraNumber = 13;
				PKNumber = 3;
				break;
			default:
			}
		}
	}

}