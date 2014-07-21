


import org.voltdb.*;

public class Procedure12 extends VoltProcedure {
	public final SQLStmt sql = new SQLStmt(
			"SELECT ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, ol_delivery_d "
					+ "FROM order_line WHERE tenant_id = ? AND ol_w_id = ? AND ol_d_id = ? AND ol_o_id = ?");

	public long run(int tenant_id, int ol_w_id, int ol_d_id,
			int ol_o_id) throws VoltAbortException {
		voltQueueSQL(sql, tenant_id, ol_w_id, ol_d_id, ol_o_id);
		VoltTable[] results = voltExecuteSQL();
		if (results[0].getRowCount() == 0)
			return 0;
		return 1;
	}

}
