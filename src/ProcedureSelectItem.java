
import org.voltdb.*;

public class ProcedureSelectItem extends VoltProcedure {
	public final SQLStmt sql = new SQLStmt("SELECT * FROM item WHERE tenant_id = ? AND i_id = ?");
	
	public long run(int tenant_id, int i_id) throws VoltAbortException {
		voltQueueSQL(sql, tenant_id, i_id);
		VoltTable[] results = voltExecuteSQL();
		if(results[0].getRowCount() == 0)
			return 0;
		return 1;
	}

}
