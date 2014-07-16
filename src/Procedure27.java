

import java.math.BigDecimal;

import org.voltdb.*;

public class Procedure27 extends VoltProcedure {
	public final SQLStmt sql = new SQLStmt("UPDATE warehouse SET w_ytd = w_ytd + ? , is_update = 1 WHERE tenant_id = ? AND w_id = ?");

	public long run(int tenant_id, double h_amount, int w_id) throws VoltAbortException{
		voltQueueSQL(sql, new BigDecimal(h_amount), tenant_id, w_id);
		VoltTable[] result = voltExecuteSQL();
		if(result[0].asScalarLong() == 0)
			return 0;
		return 1;
	}
	
}
