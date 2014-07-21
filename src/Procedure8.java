


import org.voltdb.*;

public class Procedure8 extends VoltProcedure {
	public final SQLStmt sql = new SQLStmt("SELECT c_data FROM customer WHERE tenant_id = ? AND c_w_id = ? AND c_d_id = ? AND c_id = ?");

	public long run(int tenant_id, int c_w_id, int c_d_id, int c_id) throws VoltAbortException{
		voltQueueSQL(sql, tenant_id, c_w_id, c_d_id, c_id);
		VoltTable[] results = voltExecuteSQL();
		if(results[0].getRowCount() == 0)
			return 0;
		return 1;
	}
	
}
