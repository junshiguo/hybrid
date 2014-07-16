

import org.voltdb.*;

public class Procedure30 extends VoltProcedure {
	public final SQLStmt sql = new SQLStmt("UPDATE customer SET c_balance = ? , is_update = 1 "
			+ "WHERE tenant_id = ? AND c_w_id = ? AND c_d_id = ? AND c_id = ?");
	
	public long run(int tenant_id, double c_balance, int c_w_id, int c_d_id, int c_id) throws VoltAbortException{
		voltQueueSQL(sql, c_balance, tenant_id, c_w_id, c_d_id, c_id);
		VoltTable[] result = voltExecuteSQL();
		if(result[0].asScalarLong() == 0)
			return 0;
		return 1;
	}

}
