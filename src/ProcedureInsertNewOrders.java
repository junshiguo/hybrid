import org.voltdb.*;

public class ProcedureInsertNewOrders extends VoltProcedure {
	public final SQLStmt sqlInsert = new SQLStmt("INSERT INTO new_orders (no_o_id, no_d_id, no_w_id, tenant_id, is_insert, is_update) "
			+ "VALUES (?,?,?,?,?,?)");
	
	public long run(int tenant_id, int no_o_id, int no_d_id, int no_w_id, int is_insert, int is_update) throws VoltAbortException {
		voltQueueSQL(sqlInsert, no_o_id, no_d_id, no_w_id, tenant_id, is_insert, is_update);
		VoltTable[] result = voltExecuteSQL();
		if(result[0].asScalarLong() == 0)
			return 0;
		return 1;
	}

}
