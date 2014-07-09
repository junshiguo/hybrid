

import org.voltdb.*;

public class ProcedureInsertWarehouse extends VoltProcedure {
	public final SQLStmt sqlInsert = new SQLStmt("INSERT INTO warehouse (w_id, w_name, w_street_1, w_street_2, "
			+ "w_city, w_state, w_zip, w_tax, w_ytd, tenant_id, is_insert, is_update) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)");
	
	public long run( int tenant_id, int w_id, String w_name, String w_street_1, String w_street_2, String w_city, 
			String w_state, String w_zip, double w_tax, double w_ytd, int is_insert, int is_update) 
					throws VoltAbortException {
		voltQueueSQL(sqlInsert, w_id, w_name, w_street_1, w_street_2,
			w_city, w_state, w_zip, w_tax, w_ytd, tenant_id, is_insert, is_update);
		VoltTable[] result = voltExecuteSQL();
		if(result[0].asScalarLong() == 0)
			return 0;
		return 1;
	}

}
