


import org.voltdb.*;

public class Procedure2 extends VoltProcedure {
	public final SQLStmt sql = new SQLStmt("SELECT s_quantity, s_data, s_dist_01, s_dist_02, s_dist_03, s_dist_04, " +
				"s_dist_05, s_dist_06, s_dist_07, s_dist_08, s_dist_09, s_dist_10 FROM stock "
				+ "WHERE tenant_id = ? AND s_i_id = ? AND s_w_id = ?");
	
	public long run(int tenant_id, int s_i_id, int s_w_id) throws VoltAbortException {
		voltQueueSQL(sql, tenant_id, s_i_id, s_w_id);
		VoltTable[] results = voltExecuteSQL();
		if(results[0].getRowCount() == 0)
			return 0;
		return 1;
	}

}
