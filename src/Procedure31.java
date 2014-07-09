

import org.voltdb.*;

public class Procedure31 extends VoltProcedure {
	public final SQLStmt sql = new SQLStmt("UPDATE orders SET o_carrier_id = ? "
			+ "WHERE tenant_id = ? AND o_id = ? AND o_d_id = ? AND o_w_id = ?");
	
	public long run(int tenant_id, int o_carrier_id, int o_id, int o_d_id, int o_w_id) throws VoltAbortException {
		voltQueueSQL(sql, o_carrier_id, tenant_id, o_id, o_d_id, o_w_id);
		VoltTable[] result = voltExecuteSQL();
		if(result[0].asScalarLong() == 0)
			return 0;
		return 1;
	}

}
