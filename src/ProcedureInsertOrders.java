

import org.voltdb.*;

public class ProcedureInsertOrders extends VoltProcedure {
	public final SQLStmt sqlInsert = new SQLStmt("INSERT INTO orders (o_id, o_d_id, o_w_id, o_c_id, o_entry_d, "
			+ "o_carrier_id, o_ol_cnt, o_all_local, tenant_id, is_insert, is_update) VALUES (?,?,?,?,?,?,?,?,?,?,?)");

	public long run(int tenant_id, int o_id, int o_d_id, int o_w_id, int o_c_id, String o_entry_d, 
			int o_carrier_id, int o_ol_cnt, int o_all_local, int is_insert, 
			int is_update) throws VoltAbortException {
		voltQueueSQL(sqlInsert, o_id, o_d_id, o_w_id, o_c_id, o_entry_d, 
			o_carrier_id, o_ol_cnt, o_all_local, tenant_id, is_insert, is_update);
		VoltTable[] result = voltExecuteSQL();
		if(result[0].asScalarLong() == 0)
			return 0;
		return 1;
	}
	
}
