


import org.voltdb.*;

public class Procedure10 extends VoltProcedure {
	public final SQLStmt sql = new SQLStmt("SELECT c_balance, c_first, c_middle, c_last FROM customer "
			+ "WHERE tenant_id = ? AND c_w_id = ? AND c_d_id = ? AND c_last = ? ORDER BY c_first");
	
	public long run(int tenant_id, int c_w_id, int c_d_id, String c_last) throws VoltAbortException{
		voltQueueSQL(sql, tenant_id, c_w_id, c_d_id, c_last);
		VoltTable[] results = voltExecuteSQL();
		if(results[0].getRowCount() == 0)
			return 0;
		return 1;
	}

}
