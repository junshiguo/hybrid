package voltdbTest;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.Random;

import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcCallException;

import utility.Support;

public class Driver {
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
	public  String c_middle = "OE", c_ytd_payment = "10.0";
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

	public Driver(int Id) {
		Object[] para = new Object[10];
		int[] paraType = new int[10];
		while (Driver.IsActive) {
			int seq;
			switch (seq = Tenant.tenants[Id].sequence.nextSequence()) {
			case 0:
				w_id = Support.RandomNumber(min_ware, max_ware);
				d_id = Support.RandomNumber(1, Driver.DIST_PER_WARE);
				para[0] = d_id; paraType[0] = Driver.TYPE_INT;
				para[1] = w_id;	paraType[1] = Driver.TYPE_INT;
				doSQL(Id, 0, 2, para, paraType);
				break;
			case 1:
				i_id = Support.RandomNumber(1, MAXITEMS);
				para[0] = i_id; paraType[0] = Driver.TYPE_INT;
				doSQL(Id, 1, 1, para, paraType);
				break;
			case 2:
				s_i_id = Support.RandomNumber(1, MAXITEMS);
				s_w_id = Support.RandomNumber(min_ware, max_ware);
				para[0] = s_i_id; paraType[0] = Driver.TYPE_INT;
				para[1] = s_w_id; paraType[1] = Driver.TYPE_INT;
				doSQL(Id, 2, 2, para, paraType);
				break;
			case 3:
				w_id = Support.RandomNumber(min_ware, max_ware);
				para[0] = w_id; paraType[0] = Driver.TYPE_INT;
				doSQL(Id, 3, 1, para, paraType);
				break;
			case 4:
				w_id = Support.RandomNumber(min_ware,max_ware);
				d_id = Support.RandomNumber(1,Driver.DIST_PER_WARE);
				para[0] = w_id; paraType[0] = Driver.TYPE_INT;
				para[1] = d_id; paraType[1] = Driver.TYPE_INT;
				doSQL(Id, 4, 2, para, paraType);
				break;
			case 5:
				c_w_id = Support.RandomNumber(min_ware,max_ware);
				c_d_id = Support.RandomNumber(1,Driver.DIST_PER_WARE);
				c_last = Support.Lastname(Support.NURand(255,0,999));
				para[0] = c_w_id; paraType[0] = Driver.TYPE_INT;
				para[1] = c_d_id; paraType[1] = Driver.TYPE_INT;
				para[2] = c_last; paraType[2] = Driver.TYPE_STRING;
				doSQL(Id, 5, 3, para, paraType);
				break;
			case 6:
				c_w_id = Support.RandomNumber(min_ware,max_ware);
				c_d_id = Support.RandomNumber(1,Driver.DIST_PER_WARE);
				c_last = Support.Lastname(Support.NURand(255,0,999));
				para[0] = c_w_id; paraType[0] = Driver.TYPE_INT;
				para[1] = c_d_id; paraType[1] = Driver.TYPE_INT;
				para[2] = c_last; paraType[2] = Driver.TYPE_STRING;
				doSQL(Id, 6, 3, para, paraType);
				break;
			case 7:
				c_w_id = Support.RandomNumber(min_ware,max_ware);
				c_d_id = Support.RandomNumber(1,Driver.DIST_PER_WARE);
				c_id = Support.RandomNumber(1,Driver.CUST_PER_DIST);
				para[0] = c_w_id; paraType[0] = Driver.TYPE_INT;
				para[1] = c_d_id; paraType[1] = Driver.TYPE_INT;
				para[2] = c_id; paraType[2] = Driver.TYPE_INT;
				doSQL(Id, 7, 3, para, paraType);
				break;
			case 8:
				c_w_id = Support.RandomNumber(min_ware,max_ware);
				c_d_id = Support.RandomNumber(1,Driver.DIST_PER_WARE);
				c_id = Support.RandomNumber(1,Driver.CUST_PER_DIST);
				para[0] = c_w_id; paraType[0] = Driver.TYPE_INT;
				para[1] = c_d_id; paraType[1] = Driver.TYPE_INT;
				para[2] = c_id; paraType[2] = Driver.TYPE_INT;
				doSQL(Id, 8, 3, para, paraType);
				break;
			case 9:
				c_w_id = Support.RandomNumber(min_ware,max_ware);
				c_d_id = Support.RandomNumber(1,Driver.DIST_PER_WARE);
				c_id = Support.RandomNumber(1,Driver.CUST_PER_DIST);
				if(c_id < 1000)	c_last = Support.Lastname(c_id-1);
				else c_last = Support.Lastname(Support.NURand(255,0,999));
				para[0] = c_w_id; 	paraType[0] = Driver.TYPE_INT;
				para[1] = c_d_id; 	paraType[1] = Driver.TYPE_INT;
				para[2] = c_last; 	paraType[2] = Driver.TYPE_STRING;
				doSQL(Id, 9, 3, para, paraType);
				break;
			case 10:
				c_w_id = Support.RandomNumber(min_ware,max_ware);
				c_d_id = Support.RandomNumber(1,Driver.DIST_PER_WARE);
				c_id = Support.RandomNumber(1,Driver.CUST_PER_DIST);
				if(c_id < 1000)	c_last = Support.Lastname(c_id-1);
				else c_last = Support.Lastname(Support.NURand(255,0,999));
				para[0] = c_w_id; 	paraType[0] = Driver.TYPE_INT;
				para[1] = c_d_id; 	paraType[1] = Driver.TYPE_INT;
				para[2] = c_last; 	paraType[2] = Driver.TYPE_STRING;
				doSQL(Id, 10, 3, para, paraType);
				break;
			case 11:
				c_w_id = Support.RandomNumber(min_ware,max_ware); 
				c_d_id = Support.RandomNumber(1,Driver.DIST_PER_WARE);
				c_id = Support.RandomNumber(1,Driver.CUST_PER_DIST);
				para[0] = c_w_id; 	paraType[0] = Driver.TYPE_INT;
				para[1] = c_d_id; 	paraType[1] = Driver.TYPE_INT;
				para[2] = c_id; 	paraType[2] = Driver.TYPE_INT;
				doSQL(Id, 11, 3, para, paraType);
				break;
			case 12:
				ol_w_id = Support.RandomNumber(min_ware,max_ware);
				ol_d_id = Support.RandomNumber(1,Driver.DIST_PER_WARE);
				ol_o_id = Support.RandomNumber(1,Driver.ORD_PER_DIST);
				para[0] = ol_w_id;	paraType[0] = Driver.TYPE_INT;
				para[1] = ol_d_id;	paraType[1] = Driver.TYPE_INT;
				para[2] = ol_o_id;	paraType[2] = Driver.TYPE_INT;
				doSQL(Id, 12, 3, para, paraType);
				break;
			case 13:
				no_d_id = Support.RandomNumber(1,Driver.DIST_PER_WARE);
				no_w_id = Support.RandomNumber(min_ware,max_ware);
				para[0] = no_d_id;	paraType[0] = Driver.TYPE_INT;
				para[1] = no_w_id;	paraType[1] = Driver.TYPE_INT;
				doSQL(Id, 13, 2, para, paraType);
				break;
			case 14:
				o_id = Support.RandomNumber(1,Driver.ORD_PER_DIST);
				o_d_id = Support.RandomNumber(1,Driver.DIST_PER_WARE);
				o_w_id = Support.RandomNumber(min_ware,max_ware);
				para[0] = o_id;		paraType[0] = Driver.TYPE_INT;
				para[1] = o_d_id;	paraType[1] = Driver.TYPE_INT;
				para[2] = o_w_id;	paraType[2] = Driver.TYPE_INT;
				doSQL(Id, 14, 3, para, paraType);
				break;
			case 15:
				ol_o_id = Support.RandomNumber(1,Driver.ORD_PER_DIST);
				ol_d_id = Support.RandomNumber(1,Driver.DIST_PER_WARE);
				ol_w_id = Support.RandomNumber(min_ware,max_ware);
				para[0] = ol_o_id;	paraType[0] = Driver.TYPE_INT;
				para[1] = ol_d_id;	paraType[1] = Driver.TYPE_INT;
				para[2] = ol_w_id;	paraType[2] = Driver.TYPE_INT;
				doSQL(Id, 15, 3, para, paraType);
				break;
			case 16:
				d_id = Support.RandomNumber(1,Driver.DIST_PER_WARE);
				d_w_id = Support.RandomNumber(min_ware,max_ware);
				para[0] = d_id;		paraType[0] = Driver.TYPE_INT;
				para[1] = d_w_id;	paraType[1] = Driver.TYPE_INT;
				doSQL(Id, 16, 2, para, paraType);
				break;
			case 17:
				ol_w_id = Support.RandomNumber(min_ware,max_ware);
				ol_d_id = Support.RandomNumber(1,Driver.DIST_PER_WARE);
				ol_o_id = Support.RandomNumber(1,Driver.ORD_PER_DIST);
				para[0] = ol_w_id; paraType[0] = Driver.TYPE_INT;
				para[1] = ol_d_id; paraType[1] = Driver.TYPE_INT;
				para[2] = ol_o_id; paraType[2] = Driver.TYPE_INT;
				para[3] = ol_o_id; paraType[3] = Driver.TYPE_INT;
				doSQL(Id, 17, 4, para, paraType);
				break;
			case 18:
				s_w_id = Support.RandomNumber(min_ware,max_ware);
				s_i_id = Support.RandomNumber(1,MAXITEMS);
				s_quantity = Support.RandomNumber(10,100);
				para[0] = s_w_id; 		paraType[0] = Driver.TYPE_INT;
				para[1] = s_i_id;	 	paraType[1] = Driver.TYPE_INT;
				para[2] = s_quantity; 	paraType[2] = Driver.TYPE_INT;
				doSQL(Id, 18, 3, para, paraType);
				break;
			case 19:
				w_id = Support.RandomNumber(min_ware,max_ware);
				c_d_id = Support.RandomNumber(1,Driver.DIST_PER_WARE);
				c_id = Support.RandomNumber(1,Driver.CUST_PER_DIST);
				para[0] = w_id; 	paraType[0] = Driver.TYPE_INT;
				para[1] = c_d_id;	paraType[1] = Driver.TYPE_INT;
				para[2] = c_id; 	paraType[2] = Driver.TYPE_INT;
				doSQL(Id, 19, 3, para, paraType);
				break;
			case 20:
				o_w_id = Support.RandomNumber(min_ware,max_ware);
				o_d_id = Support.RandomNumber(1,Driver.DIST_PER_WARE);
				o_c_id = Support.RandomNumber(1,Driver.ORD_PER_DIST);
				para[0] = para[3] = o_w_id; paraType[0] = paraType[3] = Driver.TYPE_INT;
				para[1] = para[4] = o_d_id; paraType[1] = paraType[4] = Driver.TYPE_INT;
				para[2] = para[5] = o_c_id; paraType[2] = paraType[5] = Driver.TYPE_INT;
				doSQL(Id, 20, 6, para, paraType);
				break;
			case 21:
				o_id = Support.RandomNumber(1,Driver.ORD_PER_DIST); 
				o_d_id = Support.RandomNumber(1,Driver.DIST_PER_WARE);
				o_w_id = Support.RandomNumber(min_ware,max_ware);
				o_c_id = Support.RandomNumber(1,Driver.ORD_PER_DIST);
				//o_c_id = Support.NURand(1023,1,Driver.CUST_PER_DIST)+1;
				java.util.Date  date=new java.util.Date();
				o_entry_d = new Timestamp(date.getTime()).toString();
				int ooo = o_entry_d.lastIndexOf(".");
				if(ooo != -1) o_entry_d.substring(0, ooo);
				o_ol_cnt = Support.RandomNumber(5,15);
				o_all_local = ran.nextInt(100)==1?0:1;
				para[0] = o_id;		paraType[0] = Driver.TYPE_INT;
				para[1] = o_d_id;	paraType[1] = Driver.TYPE_INT;
				para[2] = o_w_id;	paraType[2] = Driver.TYPE_INT;
				para[3] = o_c_id;	paraType[3] = Driver.TYPE_INT;
				para[4] = o_entry_d;	paraType[4] = Driver.TYPE_STRING;
				para[5] = o_ol_cnt;	paraType[5] = Driver.TYPE_INT;
				para[6] = o_all_local;	paraType[6] = Driver.TYPE_INT;
				doSQL(Id, 21, 7, para, paraType);
				break;
			case 22:
				no_o_id = Support.RandomNumber(1,Driver.ORD_PER_DIST);
				no_d_id = Support.RandomNumber(1,Driver.DIST_PER_WARE);
				no_w_id = Support.RandomNumber(min_ware,max_ware);
				para[0] = no_o_id;	paraType[0] = Driver.TYPE_INT;
				para[1] = no_d_id;	paraType[1] = Driver.TYPE_INT;
				para[2] = no_w_id;	paraType[2] = Driver.TYPE_INT;
				doSQL(Id, 22, 3, para, paraType);
				break;
			case 23:
				ol_o_id = Support.RandomNumber(1,Driver.ORD_PER_DIST);
				ol_d_id = Support.RandomNumber(1,Driver.DIST_PER_WARE);
				ol_w_id = Support.RandomNumber(min_ware,max_ware);
				ol_number = Support.RandomNumber(1,15);
				ol_i_id = Support.RandomNumber(1,Driver.MAXITEMS);
				ol_supply_w_id = Support.RandomNumber(min_ware,max_ware);
				ol_quantity = 5;
				ol_amount = 0.0;
				ol_dist_info = Support.MakeAlphaString(24,24);
				para[0] = ol_o_id;		paraType[0] = Driver.TYPE_INT;
				para[1] = ol_d_id;		paraType[1] = Driver.TYPE_INT;
				para[2] = ol_w_id;		paraType[2] = Driver.TYPE_INT;
				para[3] = ol_number;	paraType[3] = Driver.TYPE_INT;
				para[4] = ol_i_id;		paraType[4] = Driver.TYPE_INT;
				para[5] = ol_supply_w_id;	paraType[5] = Driver.TYPE_INT;
				para[6] = ol_quantity;	paraType[6] = Driver.TYPE_INT;
				para[7] = ol_amount;	paraType[7] = Driver.TYPE_DOUBLE;
				para[8] = ol_dist_info;	paraType[8] = Driver.TYPE_STRING;
				doSQL(Id, 23, 9, para, paraType);
				break;
			case 24:
				h_c_d_id = Support.RandomNumber(1,Driver.DIST_PER_WARE);
				h_c_w_id = Support.RandomNumber(min_ware,max_ware);
				h_c_id = Support.RandomNumber(1,Driver.CUST_PER_DIST);
				h_d_id = h_c_d_id;
				h_w_id = h_c_w_id;
				java.util.Date  date1=new java.util.Date();
				h_date = new Timestamp(date1.getTime()).toString();
				int hhh = h_date.lastIndexOf(".");
				if(hhh != -1) h_date = h_date.substring(0, hhh);
				h_amount = 10.0;
				h_data = Support.MakeAlphaString(12,24);
				para[0] = h_c_d_id;		paraType[0] = Driver.TYPE_INT;
				para[1] = h_c_w_id;		paraType[1] = Driver.TYPE_INT;
				para[2] = h_c_id;		paraType[2] = Driver.TYPE_INT;
				para[3] = h_d_id;		paraType[3] = Driver.TYPE_INT;
				para[4] = h_w_id;		paraType[4] = Driver.TYPE_INT;
				para[5] = h_date;		paraType[5] = Driver.TYPE_STRING;
				para[6] = h_amount;		paraType[6] = Driver.TYPE_DOUBLE;
				para[7] = h_data;		paraType[7] = Driver.TYPE_STRING;
				doSQL(Id, 24, 8, para, paraType);
				break;
			case 25:
				d_id = Support.RandomNumber(1,Driver.DIST_PER_WARE);
				d_w_id = Support.RandomNumber(min_ware,max_ware);
				para[0] = d_id;	paraType[0] = Driver.TYPE_INT;
				para[1] = d_w_id;	paraType[1] = Driver.TYPE_INT;
				doSQL(Id, 25, 2, para, paraType);
				break;
			case 26:
				s_quantity = Support.RandomNumber(10,100);
				s_i_id = Support.RandomNumber(1,MAXITEMS);
				s_w_id = Support.RandomNumber(min_ware,max_ware);
				para[0] = s_quantity;	paraType[0] = Driver.TYPE_INT;
				para[1] = s_i_id;		paraType[1] = Driver.TYPE_INT;
				para[2] = s_w_id;		paraType[2] = Driver.TYPE_INT;
				doSQL(Id, 26, 3, para, paraType);
				break;
			case 27:
				h_amount = 10.0;
				w_id = Support.RandomNumber(min_ware,max_ware);
				para[0] = h_amount;	paraType[0] = Driver.TYPE_DOUBLE;
				para[1] = w_id;		paraType[1] = Driver.TYPE_INT;
				doSQL(Id, 27, 2, para, paraType);
				break;
			case 28:
				h_amount = 10.0;
				d_w_id = Support.RandomNumber(min_ware,max_ware);
				d_id = Support.RandomNumber(1,Driver.DIST_PER_WARE);
				para[0] = h_amount;	paraType[0] = Driver.TYPE_DOUBLE;
				para[1] = d_w_id;	paraType[1] = Driver.TYPE_INT;
				para[2] = d_id;		paraType[2] = Driver.TYPE_INT;
				doSQL(Id, 28, 3, para, paraType);
				break;
			case 29:
				c_balance = 10.0;
				c_data = Support.MakeAlphaString(300,500);
				c_w_id = Support.RandomNumber(min_ware,max_ware);
				c_d_id = Support.RandomNumber(1,Driver.DIST_PER_WARE);
				c_id = Support.RandomNumber(1,Driver.CUST_PER_DIST);
				para[0] = c_balance;	paraType[0] = Driver.TYPE_DOUBLE;
				para[1] = c_data;	paraType[1] = Driver.TYPE_STRING;
				para[2] = c_w_id;	paraType[2] = Driver.TYPE_INT;
				para[3] = c_d_id;	paraType[3] = Driver.TYPE_INT;
				para[4] = c_id;		paraType[4]	= Driver.TYPE_INT;
				doSQL(Id, 29, 5, para, paraType);
				break;
			case 30:
				c_balance = 10.0;
				c_w_id = Support.RandomNumber(min_ware,max_ware);
				c_d_id = Support.RandomNumber(1,Driver.DIST_PER_WARE);
				c_id = Support.RandomNumber(1, Driver.CUST_PER_DIST);
				para[0] = c_balance;	paraType[0] = Driver.TYPE_DOUBLE;
				para[1] = c_w_id;	paraType[1] = Driver.TYPE_INT;
				para[2] = c_d_id;	paraType[2] = Driver.TYPE_INT;
				para[3] = c_id;		paraType[3] = Driver.TYPE_INT;
				doSQL(Id, 30, 4, para, paraType);
				break;
			case 31:
				int o_carrier_id = Support.RandomNumber(1,10);
				o_id = Support.RandomNumber(1,Driver.ORD_PER_DIST);
				o_d_id = Support.RandomNumber(1,Driver.DIST_PER_WARE);
				o_w_id = Support.RandomNumber(min_ware,max_ware);
				para[0] = o_carrier_id;	paraType[0] = Driver.TYPE_INT;
				para[1] = o_id;			paraType[1] = Driver.TYPE_INT;
				para[2] = o_d_id;		paraType[2] = Driver.TYPE_INT;
				para[3] = o_w_id;		paraType[3] = Driver.TYPE_INT;
				doSQL(Id, 31, 4, para, paraType);
				break;
			case 32:
				java.util.Date  date2=new java.util.Date();
				ol_delivery_d = new Timestamp(date2.getTime()).toString();
				int lll = ol_delivery_d.lastIndexOf(".");
				if(lll != -1) ol_delivery_d = ol_delivery_d.substring(0, lll);
				ol_o_id = Support.RandomNumber(1,Driver.ORD_PER_DIST);
				ol_d_id = Support.RandomNumber(1,Driver.DIST_PER_WARE);
				ol_w_id = Support.RandomNumber(min_ware,max_ware);
				para[0] = ol_delivery_d;	paraType[0] = Driver.TYPE_STRING;
				para[1] = ol_o_id;			paraType[1] = Driver.TYPE_INT;
				para[2] = ol_d_id;		paraType[2] = Driver.TYPE_INT;
				para[3] = ol_w_id;		paraType[3] = Driver.TYPE_INT;
				doSQL(Id, 32, 4, para, paraType);
				break;
			case 33:
				c_balance = ran.nextDouble();
				c_id = Support.RandomNumber(1,Driver.CUST_PER_DIST);
				c_d_id = Support.RandomNumber(1,Driver.DIST_PER_WARE);
				c_w_id = Support.RandomNumber(min_ware,max_ware);
				para[0] = c_balance;	paraType[0] = Driver.TYPE_DOUBLE;
				para[1] = c_id;			paraType[1] = Driver.TYPE_INT;
				para[2] = c_d_id;		paraType[2] = Driver.TYPE_INT;
				para[3] = c_w_id;		paraType[3] = Driver.TYPE_INT;
				doSQL(Id, 33, 4, para, paraType);
				break;
			case 34:
				no_o_id = Support.RandomNumber(1,Driver.ORD_PER_DIST);
				no_d_id = Support.RandomNumber(1,Driver.DIST_PER_WARE);
				no_w_id = Support.RandomNumber(min_ware,max_ware);
				para[0] = no_o_id;	paraType[0] = Driver.TYPE_INT;
				para[1] = no_d_id;	paraType[1] = Driver.TYPE_INT;
				para[2] = no_w_id;	paraType[2] = Driver.TYPE_INT;
				doSQL(Id, 34, 3, para, paraType);
				break;
				default:
					System.out.println("unknown error! "+seq);
			}
		}
	}
	
	public void doSQL(int threadId, int sqlId, int paraNumber, Object[] para,
			int[] paraType) {
		boolean success = true;
		for(int i=0; i<Main.MaxTry; i++){
			success = doSQLOnce(threadId, sqlId, paraNumber, para, paraType);
			
			if(success && Driver.IsActive && Main.startCount){
				Main.queryThisInterval ++;
				return;
			}else if(!success && Driver.IsActive & Main.startCount){
//				System.out.print(".");
				System.out.println("sql failure! tenant id: "+threadId+", sql id: "+sqlId);
				System.exit(0);
			}else if(!Driver.IsActive){
				return;
			}
		}
	}

	public boolean doSQLOnce(int threadId, int sqlId, int paraNumber, Object[] para,
			int[] paraType) {
		ClientResponse response = null;
		try{
			switch(paraNumber){
			case 1:
				response = Tenant.tenants[threadId].voltdbConn.callProcedure("Procedure"+sqlId, threadId, para[0]);
				break;
			case 2:
				response = Tenant.tenants[threadId].voltdbConn.callProcedure("Procedure"+sqlId, threadId, para[0], para[1]);
				break;
			case 3:
				response = Tenant.tenants[threadId].voltdbConn.callProcedure("Procedure"+sqlId, threadId, para[0], para[1], para[2]);
				break;
			case 4:
				response = Tenant.tenants[threadId].voltdbConn.callProcedure("Procedure"+sqlId, threadId, para[0], para[1], para[2], para[3]);
				break;
			case 5:
				response = Tenant.tenants[threadId].voltdbConn.callProcedure("Procedure"+sqlId, threadId, para[0], para[1], para[2], para[3], para[4]);
				break;
			case 6:
				response = Tenant.tenants[threadId].voltdbConn.callProcedure("Procedure"+sqlId, threadId, para[0], para[1], para[2], para[3], para[4], para[5]);
				break;
			case 7:
				response = Tenant.tenants[threadId].voltdbConn.callProcedure("Procedure"+sqlId, threadId, para[0], para[1], para[2], para[3], para[4], para[5], para[6]);
				break;
			case 8:
				response = Tenant.tenants[threadId].voltdbConn.callProcedure("Procedure"+sqlId, threadId, para[0], para[1], para[2], para[3], para[4], para[5], para[6], para[7]);
				break;
			case 9:
				response = Tenant.tenants[threadId].voltdbConn.callProcedure("Procedure"+sqlId, threadId, para[0], para[1], para[2], para[3], para[4], para[5], para[6], para[7], para[8]);
				break;
			case 10:
				response = Tenant.tenants[threadId].voltdbConn.callProcedure("Procedure"+sqlId, threadId, para[0], para[1], para[2], para[3], para[4], para[5], para[6], para[7], para[8], para[9]);
				break;
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
