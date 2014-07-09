

import org.voltdb.*;

public class Procedure25 extends VoltProcedure {
	public final SQLStmt sql = new SQLStmt("UPDATE district SET d_next_o_id = d_next_o_id + 1 "
			+ "WHERE tenant_id = ? AND d_id = ? AND d_w_id = ?");
	
	public long run(int tenant_id, int d_id, int d_w_id) throws VoltAbortException{
		voltQueueSQL(sql, tenant_id, d_id, d_w_id);
		VoltTable[] reuslt = voltExecuteSQL();
		if(reuslt[0].asScalarLong() == 0)
			return 0;
		return 1;
	}

}
