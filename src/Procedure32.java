

import org.voltdb.*;
import org.voltdb.types.TimestampType;

public class Procedure32 extends VoltProcedure {
	public final SQLStmt sql = new SQLStmt("UPDATE order_line SET ol_delivery_d = ? "
			+ "WHERE tenant_id = ? AND ol_o_id = ? AND ol_d_id = ? AND ol_w_id = ?");
	
	public long run(int tenant_id, String ol_delivery_d, int ol_o_id, int ol_d_id, int ol_w_id) throws VoltAbortException {
		voltQueueSQL(sql, new TimestampType(ol_delivery_d), tenant_id, ol_o_id, ol_d_id, ol_w_id);
		VoltTable[] result = voltExecuteSQL();
		if(result[0].asScalarLong() == 0)
			return 0;
		return 1;
	}

}
