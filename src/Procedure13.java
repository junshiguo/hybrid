

import org.voltdb.*;

public class Procedure13 extends VoltProcedure {
	public final SQLStmt sql = new SQLStmt("SELECT COALESCE(MIN(no_o_id),0) FROM new_orders "
			+ "WHERE tenant_id = ? AND no_d_id = ? AND no_w_id = ?");
	
	public long run(int tenant_id, int no_d_id, int no_w_id) throws VoltAbortException{
		voltQueueSQL(sql, tenant_id, no_d_id, no_w_id);
		VoltTable[] results = voltExecuteSQL();
		if(results[0].getRowCount() == 0 || (results[0].fetchRow(0).getLong(0)) == 0)
			return 0;
		return 1;
	}

}
