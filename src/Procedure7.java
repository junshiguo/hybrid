

import org.voltdb.*;

public class Procedure7 extends VoltProcedure {
	public final SQLStmt sql = new SQLStmt("SELECT c_first, c_middle, c_last, c_street_1, c_street_2, c_city, c_state, c_zip, c_phone, c_credit, c_credit_lim, c_discount, c_balance, c_since "
			+ "FROM customer WHERE tenant_id = ? AND c_w_id = ? AND c_d_id = ? AND c_id = ?");
	
	public long run(int tenant_id, int c_w_id, int c_d_id, int c_id) throws VoltAbortException {
		voltQueueSQL(sql, tenant_id, c_w_id, c_d_id, c_id);
		VoltTable[] results = voltExecuteSQL();
		if(results[0].getRowCount() == 0)
			return 0;
		return 1;
	}

}
