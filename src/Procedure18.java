


import org.voltdb.*;

public class Procedure18 extends VoltProcedure {
	public final SQLStmt sql = new SQLStmt("SELECT count(*) FROM stock "
			+ "WHERE tenant_id = ? AND s_w_id = ? AND s_i_id = ? AND s_quantity < ?");
	
	public long run(int tenant_id, int s_w_id, int s_i_id, int s_quantity) throws VoltAbortException{
		voltQueueSQL(sql, tenant_id, s_w_id, s_i_id, s_quantity);
		VoltTable[] results = voltExecuteSQL();
		if(results[0].getRowCount() == 0)
			return 0;
		return 1;
	}

}
