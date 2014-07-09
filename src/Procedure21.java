

import org.voltdb.*;
import org.voltdb.types.TimestampType;

public class Procedure21 extends VoltProcedure {
	public final SQLStmt sql = new SQLStmt(
			"INSERT INTO orders (o_id, o_d_id, o_w_id, o_c_id, o_entry_d, o_ol_cnt, o_all_local, tenant_id, is_insert, is_update) "
					+ "VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");

	public long run(int tenant_id, int o_id, int o_d_id, int o_w_id, int o_c_id,
			String o_entry_d, int o_ol_cnt, int o_all_local)
			throws VoltAbortException {
		voltQueueSQL(sql, o_id, o_d_id, o_w_id, o_c_id, new TimestampType(o_entry_d), o_ol_cnt, o_all_local, tenant_id, 1, 0);
		voltExecuteSQL();
		return 1;
	}

}
