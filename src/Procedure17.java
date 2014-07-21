


import org.voltdb.*;

public class Procedure17 extends VoltProcedure {
	public final SQLStmt sql = new SQLStmt("SELECT DISTINCT ol_i_id FROM order_line "
			+ "WHERE tenant_id = ? AND ol_w_id = ? AND ol_d_id = ? AND ol_o_id < ? AND ol_o_id >= (? - 20)");
	
	public long run(int tenant_id, int ol_w_id, int ol_d_id, int ol_o_id, int ol_o_id1) throws VoltAbortException {
		voltQueueSQL(sql, tenant_id,ol_w_id, ol_d_id, ol_o_id, ol_o_id);
		VoltTable[] results = voltExecuteSQL();
		if(results[0].getRowCount() == 0)
			return 0;
		return 1;
	}

}
