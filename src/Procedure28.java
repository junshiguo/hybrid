


import java.math.BigDecimal;

import org.voltdb.*;

public class Procedure28 extends VoltProcedure {
	public final SQLStmt sql = new SQLStmt("UPDATE district SET d_ytd = d_ytd + ? , is_update = 1 "
			+ "WHERE tenant_id = ? AND d_w_id = ? AND d_id = ?");

	public long run(int tenant_id, double h_amount, int d_w_id, int d_id) throws VoltAbortException{
		voltQueueSQL(sql, new BigDecimal(h_amount), tenant_id, d_w_id, d_id);
		VoltTable[] result = voltExecuteSQL();
		if(result[0].asScalarLong() == 0)
			return 0;
		return 1;
	}

}
