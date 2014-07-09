

import org.voltdb.*;

public class Procedure20 extends VoltProcedure {
	public final SQLStmt sql = new SQLStmt("SELECT o_id, o_entry_d, COALESCE(o_carrier_id,0) FROM orders "
			+ "WHERE tenant_id = ? AND o_w_id = ? AND o_d_id = ? AND o_c_id = ? "
			+ "AND o_id = ?");
	public final SQLStmt sql0 = new SQLStmt("SELECT MAX(o_id) FROM orders "
			+ "WHERE tenant_id = ? AND o_w_id = ? AND o_d_id = ? AND o_c_id = ?");
	
	public long run(int tenant_id, int o_w_id, int o_d_id, int o_c_id, int o_w_id1, int o_d_id1, int o_c_id1) throws VoltAbortException{
		voltQueueSQL(sql0, tenant_id, o_w_id1, o_d_id1, o_c_id1);
		VoltTable[] results = voltExecuteSQL();
		if(results[0].getRowCount() == 0)
			return 0;
		int o_id = (int) results[0].fetchRow(0).getLong(0);
		voltQueueSQL(sql, tenant_id, o_w_id, o_d_id, o_c_id, o_id);
		results = voltExecuteSQL();
		if(results[0].getRowCount() == 0)
			return 0;
		return 1;
	}

}
