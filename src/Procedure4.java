

import org.voltdb.*;

public class Procedure4 extends VoltProcedure {
	public final SQLStmt sql = new SQLStmt("SELECT d_street_1, d_street_2, d_city, d_state, d_zip, d_name "
			+ "FROM district WHERE tenant_id = ? AND d_w_id = ? AND d_id = ?");
	
	public long run(int tenant_id, int d_w_id, int d_id) throws VoltAbortException {
		voltQueueSQL(sql, tenant_id, d_w_id, d_id);
		VoltTable[] results = voltExecuteSQL();
		if(results[0].getRowCount() == 0)
			return 0;
		return 1;
	}

}
