

import org.voltdb.*;

public class Procedure0 extends VoltProcedure {
	public final SQLStmt sql = new SQLStmt("SELECT d_next_o_id, d_tax FROM district WHERE tenant_id = ? AND d_id = ? AND d_w_id = ?");

	public long run(int tenant_id, int d_id, int d_w_id) throws VoltAbortException {
		voltQueueSQL(sql,tenant_id, d_id, d_w_id);
		VoltTable[] results =  voltExecuteSQL();
		if(results[0].getRowCount() == 0)
			return 0;
		return 1;
	}
}
