


import org.voltdb.*;

public class Procedure26 extends VoltProcedure {
	public final SQLStmt sql = new SQLStmt("UPDATE stock SET s_quantity = ? , is_update = 1 "
			+ "WHERE tenant_id = ? AND s_i_id = ? AND s_w_id = ?");
	
	public long run(int tenant_id, int s_quantity, int s_i_id, int s_w_id) throws VoltAbortException{
		voltQueueSQL(sql,s_quantity, tenant_id, s_i_id, s_w_id);
		VoltTable[] result = voltExecuteSQL();
		if(result[0].asScalarLong() == 0)
			return 0;
		return 1;
	}

}
