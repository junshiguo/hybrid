

import org.voltdb.*;

public class Procedure9 extends VoltProcedure {
	public final SQLStmt sql = new SQLStmt("SELECT count(c_id) FROM customer WHERE tenant_id = ? AND c_w_id = ? AND c_d_id = ? AND c_last = ?");

	public long run(int tenant_id, int c_w_id, int c_d_id, String c_last) throws VoltAbortException {
		voltQueueSQL(sql, tenant_id, c_w_id, c_d_id, c_last);
		VoltTable[] results = voltExecuteSQL();
		if(results[0].getRowCount() == 0 || results[0].fetchRow(0).getLong(0) == 0)
			return 0;
		return 1;
	}
	
}
