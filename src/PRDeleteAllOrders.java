

import org.voltdb.*;

public class PRDeleteAllOrders extends VoltProcedure {
	public final SQLStmt sql = new SQLStmt("DELETE FROM orders WHERE tenant_id = ?");
	
	public VoltTable[] run(int tenant_id) throws VoltAbortException {
		voltQueueSQL(sql, tenant_id);
		return voltExecuteSQL();
	}
	
}
