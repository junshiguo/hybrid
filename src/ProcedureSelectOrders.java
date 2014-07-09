
import org.voltdb.*;

public class ProcedureSelectOrders extends VoltProcedure {
	public final SQLStmt sqlpre = new SQLStmt("SELECT * FROM orders "
			+ "WHERE tenant_id = ? AND o_id = ? AND o_d_id = ? AND o_w_id = ? ORDER BY o_entry_d");
	
	public long run(int tenant_id, int o_id, int o_d_id, int o_w_id) throws VoltAbortException {
		voltQueueSQL(sqlpre, tenant_id, o_id, o_d_id, o_w_id);
		VoltTable[] result = voltExecuteSQL();
		if(result[0].getRowCount() == 0)
			return 0;
		return 1;
	}

}
