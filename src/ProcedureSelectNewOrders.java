
import org.voltdb.*;

public class ProcedureSelectNewOrders extends VoltProcedure {
	public final SQLStmt sql = new SQLStmt("SELECT * FROM new_orders WHERE tenant_id = ? AND no_o_id = ? AND no_d_id = ? AND no_w_id = ?");
	
	public long run(int tenant_id, int no_o_id, int no_d_id, int no_w_id) throws VoltAbortException {
		voltQueueSQL(sql, tenant_id, no_o_id, no_d_id, no_w_id);
		VoltTable[] result = voltExecuteSQL();
		if(result[0].getRowCount() == 0)
			return 0;
		return 1;
	}

}
