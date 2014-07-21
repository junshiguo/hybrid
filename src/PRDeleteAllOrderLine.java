

import org.voltdb.*;

public class PRDeleteAllOrderLine extends VoltProcedure {
	public final SQLStmt sql = new SQLStmt("DELETE FROM order_line WHERE tenant_id = ?");
	
	public VoltTable[] run(int tenant_id) throws VoltAbortException {
		voltQueueSQL(sql, tenant_id);
		return voltExecuteSQL();
	}
	
}
