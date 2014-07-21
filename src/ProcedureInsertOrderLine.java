


import org.voltdb.*;

public class ProcedureInsertOrderLine extends VoltProcedure {
	public final SQLStmt sqlInsert = new SQLStmt("INSERT INTO order_line (ol_o_id, ol_d_id,ol_w_id,"
			+ "ol_number,ol_i_id, ol_supply_w_id,ol_delivery_d, ol_quantity, ol_amount, ol_dist_info,"
			+ "tenant_id, is_insert, is_update) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)");
	
	public long run(int tenant_id, int ol_o_id, int ol_d_id, int ol_w_id, int ol_number, int ol_i_id, 
			int ol_supply_w_id, String ol_delivery_d, int ol_quantity, double ol_amount, String ol_dist_info, 
			 int is_insert, int is_update) throws VoltAbortException {
		voltQueueSQL(sqlInsert, ol_o_id, ol_d_id,ol_w_id, ol_number,ol_i_id, ol_supply_w_id,ol_delivery_d,
				ol_quantity, ol_amount, ol_dist_info, tenant_id, is_insert, is_update);
		VoltTable[] result = voltExecuteSQL();
		if(result[0].asScalarLong() == 0)
			return 0;
		return 1;
	}

}
