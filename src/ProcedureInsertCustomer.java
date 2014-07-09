

import org.voltdb.*;
import org.voltdb.types.TimestampType;


public class ProcedureInsertCustomer extends VoltProcedure {
	public final SQLStmt sqlInsert = new SQLStmt("INSERT INTO customer (c_id, c_d_id, c_w_id ,c_first ,c_middle ,"
			+ "c_last ,c_street_1 ,c_street_2 ,c_city ,c_state ,c_zip , c_phone, c_since, c_credit, c_credit_lim, "
			+ "c_discount, c_balance, c_ytd_payment, c_payment_cnt, c_delivery_cnt, c_data, "
			+ "tenant_id, is_insert, is_update) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)");
	
	public long run(int tenant_id, int c_id, int c_d_id, int c_w_id, String c_first, String c_middle,
			String c_last, String c_street_1, String c_street_2, String c_city, String c_state, 
			String c_zip, String c_phone, TimestampType c_since, String c_credit, int c_credit_lim, 
			double c_discount, double c_balance, double c_ytd_payment, int c_payment_cnt, int c_delivery_cnt, 
			String c_data, int is_insert, int is_update) throws VoltAbortException {
		voltQueueSQL(sqlInsert, c_id, c_d_id, c_w_id ,c_first ,c_middle ,
			c_last ,c_street_1 ,c_street_2 ,c_city ,c_state ,c_zip , c_phone, c_since, c_credit, c_credit_lim, 
			c_discount, c_balance, c_ytd_payment, c_payment_cnt, 
			c_delivery_cnt, c_data, tenant_id, is_insert, is_update);
		VoltTable[] result = voltExecuteSQL();
		if(result[0].asScalarLong() == 0)
			return 0;
		return 1;
	}

}
