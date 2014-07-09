import org.voltdb.*;

public class ProcedureInsertItem extends VoltProcedure{
	public final SQLStmt sqlInsert = new SQLStmt("INSERT INTO item (i_id, i_im_id, i_name, i_price, i_data, tenant_id, is_insert, is_update) "
			+ "VALUES (?,?,?,?,?,?,?,?)");
	
	public long run(int tenant_id, int i_id, int i_im_id, String i_name, double i_price, String i_data, int is_insert, int is_update) throws VoltAbortException {
		voltQueueSQL(sqlInsert, i_id, i_im_id, i_name, i_price, i_data, tenant_id, is_insert, is_update);
		VoltTable[] result = voltExecuteSQL();
		if(result[0].asScalarLong() == 0)
			return 0;
		return 1;
	}

}
