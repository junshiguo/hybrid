

import org.voltdb.*;

public class Procedure15 extends VoltProcedure {
	public final SQLStmt sql = new SQLStmt("SELECT SUM(ol_amount) FROM order_line "
			+ "WHERE tenant_id = ? AND ol_o_id = ? AND ol_d_id = ? AND ol_w_id = ?");
	//We do not care the results, just return true or false. Anyway, this SELECT will be practice on both voltdb and mysql.
	public long run(int tenant_id, int ol_o_id, int ol_d_id, int ol_w_id) throws VoltAbortException {
		voltQueueSQL(sql, tenant_id, ol_o_id, ol_d_id, ol_w_id);
		VoltTable[] results = voltExecuteSQL();
		if(results[0].getRowCount() == 0)
			return 0;
		return 1;
	}

}
