
import org.voltdb.*;

public class ProcedureInsertHistory extends VoltProcedure {
	public final SQLStmt sqlInsert = new SQLStmt("INSERT INTO history ( h_c_id, h_c_d_id, h_c_w_id, h_d_id, h_w_id, h_date, h_amount, h_data, "
			+ "tenant_id, is_insert, is_update) VALUES (?,?,?,?,?,?,?,?,?,?,?) ");
	
	public long run(int tenant_id, int h_c_id, int h_c_d_id, int h_c_w_id, int h_d_id, int h_w_id, String h_date, double h_amount, 
			String h_data, int is_insert, int is_update) throws VoltAbortException {
		voltQueueSQL(sqlInsert, h_c_id, h_c_d_id, h_c_w_id, h_d_id, h_w_id, h_date, h_amount, h_data, tenant_id, is_insert, is_update);
		VoltTable[] result = voltExecuteSQL();
		if(result[0].asScalarLong() == 0)
			return 0;
		return 1;
	}

}
