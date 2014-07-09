
import org.voltdb.*;

public class ProcedureSelectCustomer extends VoltProcedure {
	public final SQLStmt sqlpre = new SQLStmt("SELECT * FROM customer "
			+ "WHERE tenant_id = ? AND c_w_id = ? AND c_d_id = ? AND c_id = ? ORDER BY c_last");
	
	public long run(int tenant_id, int c_w_id, int c_d_id, int c_id) throws VoltAbortException {
		voltQueueSQL(sqlpre, tenant_id, c_w_id, c_d_id, c_id);
		VoltTable[] result = voltExecuteSQL();
		if(result[0].getRowCount() == 0)
			return 0;
		return 1;
	}

}
