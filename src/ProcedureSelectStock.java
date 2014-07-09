
import org.voltdb.*;

public class ProcedureSelectStock extends VoltProcedure {
	public final SQLStmt sql = new SQLStmt("SELECT * FROM stock WHERE tenant_id = ? AND s_i_id = ? AND s_w_id = ? ORDER BY s_data");

	public long run(int tenant_id, int s_i_id, int s_w_id) throws VoltAbortException {
		voltQueueSQL(sql, tenant_id, s_i_id, s_w_id);
		VoltTable[] result = voltExecuteSQL();
		if(result[0].getRowCount() == 0)
			return 0;
		return 1;
	}
	
}
