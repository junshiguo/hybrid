

import org.voltdb.*;

public class Procedure34 extends VoltProcedure {
	public final SQLStmt sql = new SQLStmt("DELETE FROM new_orders "
			+ "WHERE tenant_id = ? AND no_o_id = ? AND no_d_id = ? AND no_w_id = ?");
	
	public long run(int tenant_id, int no_o_id, int no_d_id, int no_w_id) throws VoltAbortException {
		voltQueueSQL(sql, tenant_id, no_o_id, no_d_id, no_w_id);
		voltExecuteSQL();
		return 0;
	}

}
