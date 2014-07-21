


import org.voltdb.*;

public class Procedure14 extends VoltProcedure {
	public final SQLStmt sql = new SQLStmt("SELECT o_c_id FROM orders "
			+ "WHERE tenant_id = ? AND o_id = ? AND o_d_id = ? AND o_w_id = ?");
	
	public long run(int tenant_id, int o_id, int o_d_id, int o_w_id) throws VoltAbortException {
		voltQueueSQL(sql, tenant_id, o_id, o_d_id, o_w_id);
		VoltTable[] results = voltExecuteSQL();
		if(results[0].getRowCount() == 0)
			return 0;
		return 1;
	}

}
