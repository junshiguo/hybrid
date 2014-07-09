
import org.voltdb.*;

public class ProcedureSelectDistrict extends VoltProcedure {
	public final SQLStmt sql = new SQLStmt("SELECT * FROM district WHERE tenant_id = ? AND d_id = ? AND d_w_id = ? ORDER BY d_name");

	public long run(int tenant_id, int d_id, int d_w_id) throws VoltAbortException{
		voltQueueSQL(sql, tenant_id, d_id, d_w_id);
		VoltTable[] results = voltExecuteSQL();
		if(results[0].getRowCount() == 0)
			return 0;
		return 1;
	}
	
}
