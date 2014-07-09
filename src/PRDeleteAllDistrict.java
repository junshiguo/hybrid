
import org.voltdb.*;

public class PRDeleteAllDistrict extends VoltProcedure {
	public final SQLStmt sql = new SQLStmt("DELETE FROM district WHERE tenant_id = ?");
	
	public VoltTable[] run(int tenant_id) throws VoltAbortException{
		voltQueueSQL(sql, tenant_id);
		return voltExecuteSQL();
	}

}
