

import org.voltdb.*;
import org.voltdb.types.TimestampType;

public class Procedure24 extends VoltProcedure {
	public final SQLStmt sql = new SQLStmt(
			"INSERT INTO history(h_c_d_id, h_c_w_id, h_c_id, h_d_id, "
					+ "h_w_id, h_date, h_amount, h_data, tenant_id, is_insert, is_update) "
					+ "VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");

	public long run(int tenant_id, int h_c_d_id, int h_c_w_id, int h_c_id, int h_d_id,
			int h_w_id, String h_date, double h_amount, String h_data) throws VoltAbortException {
		voltQueueSQL(sql, h_c_d_id, h_c_w_id, h_c_id, h_d_id, h_w_id, new TimestampType(h_date), h_amount, h_data, tenant_id, 1, 0);
		voltExecuteSQL();
		return 1;
	}

}
