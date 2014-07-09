

import org.voltdb.*;

public class Procedure3 extends VoltProcedure {
	public final SQLStmt sql = new SQLStmt("SELECT w_street_1, w_street_2, w_city, w_state, w_zip, w_name "
			+ "FROM warehouse WHERE tenant_id = ? AND w_id = ?");
	
	public long run(int tenant_id, int w_id) throws VoltAbortException{
		voltQueueSQL(sql, tenant_id, w_id);
		VoltTable[] results = voltExecuteSQL();
		if(results[0].getRowCount() == 0)
			return 0;
		return 1;
	}

}
