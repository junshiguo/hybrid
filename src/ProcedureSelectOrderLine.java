
import org.voltdb.*;

public class ProcedureSelectOrderLine extends VoltProcedure {
	public final SQLStmt sqlpre = new SQLStmt("SELECT * FROM order_line "
			+ "WHERE tenant_id = ? AND ol_o_id = ? AND ol_d_id = ? AND ol_w_id = ? ORDER BY ol_dist_info");
	
	public long run(int tenant_id, int ol_o_id, int ol_d_id, int ol_w_id) throws VoltAbortException {
		voltQueueSQL(sqlpre, tenant_id, ol_o_id, ol_d_id, ol_w_id);
		VoltTable[] result = voltExecuteSQL();
		if(result[0].getRowCount() == 0)
			return 0;
		return 1;
	}

}
