


import org.voltdb.*;

public class Procedure19 extends VoltProcedure {
	public final SQLStmt sql = new SQLStmt("SELECT c_discount, c_last, c_credit, w_tax FROM customer, warehouse "
			+ "WHERE customer.tenant_id = warehouse.tenant_id AND warehouse.tenant_id = ? AND w_id = ? AND c_w_id = w_id AND c_d_id = ? AND c_id = ?");
	
	public long run(int tenant_id, int w_id, int c_d_id, int c_id) throws VoltAbortException{
		voltQueueSQL(sql, tenant_id, w_id, c_d_id, c_id);
		VoltTable[] results = voltExecuteSQL();
		if(results[0].getRowCount() == 0)
			return 0;
		return 1;
	}

}
