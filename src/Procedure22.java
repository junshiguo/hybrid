


import org.voltdb.*;

public class Procedure22 extends VoltProcedure {
	public final SQLStmt sql = new SQLStmt("INSERT INTO new_orders (no_o_id, no_d_id, no_w_id, tenant_id, is_insert, is_update) "
			+ "VALUES (?,?,?,?,?,?)");

	public long run(int tenant_id, int no_o_id, int no_d_id, int no_w_id) throws VoltAbortException{
		voltQueueSQL(sql, no_o_id, no_d_id, no_w_id, tenant_id, 1, 0);
		voltExecuteSQL();
		return 1;
	}
	
}
