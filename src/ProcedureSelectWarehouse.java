

import org.voltdb.*;

public class ProcedureSelectWarehouse extends VoltProcedure {
	public final SQLStmt sql = new SQLStmt("SELECT * FROM warehouse WHERE tenant_id = ? AND w_id = ? ORDER BY w_name");

	public long run(int tenant_id, int w_id) throws VoltAbortException {
		voltQueueSQL(sql, tenant_id, w_id);
		VoltTable[] result = voltExecuteSQL();
		if(result[0].getRowCount() == 0)
			return 0;
		return 1;
	}
	
}
