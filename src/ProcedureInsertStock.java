


import org.voltdb.*;

public class ProcedureInsertStock extends VoltProcedure {
	public final SQLStmt sqlInsert = new SQLStmt("INSERT INTO stock (s_i_id, s_w_id, s_quantity, s_dist_01, s_dist_02,s_dist_03,s_dist_04, s_dist_05, s_dist_06, "
			+ "s_dist_07, s_dist_08, s_dist_09, s_dist_10, s_ytd, s_order_cnt, s_remote_cnt,s_data, tenant_id, is_insert, is_update) "
			+ "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)");
	
	public long run(int tenant_id, int s_i_id, int s_w_id, int s_quantity, String s_dist_01, String s_dist_02, String s_dist_03,
			String s_dist_04, String s_dist_05, String s_dist_06, String s_dist_07, String s_dist_08,
			String s_dist_09, String s_dist_10, double s_ytd, int s_order_cnt, int s_remote_cnt, String s_data,
			 int is_insert, int is_update) throws VoltAbortException {
		voltQueueSQL(sqlInsert, s_i_id, s_w_id, s_quantity, s_dist_01, s_dist_02,s_dist_03,s_dist_04, s_dist_05,
				s_dist_06, s_dist_07, s_dist_08, s_dist_09, s_dist_10, s_ytd, s_order_cnt, s_remote_cnt,s_data, 
				tenant_id, is_insert, is_update);
		VoltTable[] result = voltExecuteSQL();
		if(result[0].asScalarLong() == 0)
			return 0;
		return 1;
	}

}
