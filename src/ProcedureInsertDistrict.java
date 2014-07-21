


import org.voltdb.*;


public class ProcedureInsertDistrict extends VoltProcedure {
	public final SQLStmt sqlInsert = new SQLStmt("INSERT INTO district (d_id, d_w_id, d_name, d_street_1, d_street_2, d_city, d_state, "
			+ "d_zip, d_tax, d_ytd, d_next_o_id, tenant_id, is_insert, is_update) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)");

	public long run(int tenant_id, int d_id, int d_w_id, String d_name, String d_Street_1, String d_street_2, String d_city, String d_state,
			String d_zip, double d_tax, double d_ytd, int d_next_o_id, int is_insert, int is_update) throws VoltAbortException {
		voltQueueSQL(sqlInsert, d_id, d_w_id, d_name, d_Street_1, d_street_2, d_city, d_state,
			d_zip, d_tax, d_ytd, d_next_o_id, tenant_id, is_insert, is_update);
		VoltTable[] result = voltExecuteSQL();
		if(result[0].asScalarLong() == 0)
			return 0;
		return 1;
	}
	
}
