

import org.voltdb.*;
/**
 * PRSelectAll is not partitioned and therefore not so efficient.
 * @author guojunshi
 *
 */
public class PRSelectAll extends VoltProcedure {
	public final SQLStmt sql0 = new SQLStmt("SELECT * FROM warehouse WHERE tenant_id = ? AND is_insert = ? AND is_update = ?");
	public final SQLStmt sql1 = new SQLStmt("SELECT * FROM district WHERE tenant_id = ? AND is_insert = ? AND is_update = ?");
	public final SQLStmt sql2 = new SQLStmt("SELECT * FROM customer WHERE tenant_id = ? AND is_insert = ? AND is_update = ?");
	public final SQLStmt sql3 = new SQLStmt("SELECT * FROM history WHERE tenant_id = ? AND is_insert = ? AND is_update = ?");
	public final SQLStmt sql4 = new SQLStmt("SELECT * FROM new_orders WHERE tenant_id = ? AND is_insert = ? AND is_update = ?");
	public final SQLStmt sql5 = new SQLStmt("SELECT * FROM orders WHERE tenant_id = ? AND is_insert = ? AND is_update = ?");
	public final SQLStmt sql6 = new SQLStmt("SELECT * FROM order_line WHERE tenant_id = ? AND is_insert = ? AND is_update = ?");
	public final SQLStmt sql7 = new SQLStmt("SELECT * FROM item WHERE tenant_id = ? AND is_insert = ? AND is_update = ?");
	public final SQLStmt sql8 = new SQLStmt("SELECT * FROM stock WHERE tenant_id = ? AND is_insert = ? AND is_update = ?");
	
	public VoltTable[] run(int table, int tenant_id, int is_insert, int is_update) throws VoltAbortException {
		switch(table){
		case 0:
			voltQueueSQL(sql0, tenant_id, is_insert, is_update);
			break;
		case 1:
			voltQueueSQL(sql1, tenant_id, is_insert, is_update);
			break;
		case 2:
			voltQueueSQL(sql2, tenant_id, is_insert, is_update);
			break;
		case 3:
			voltQueueSQL(sql3, tenant_id, is_insert, is_update);
			break;
		case 4:
			voltQueueSQL(sql4, tenant_id, is_insert, is_update);
			break;
		case 5:
			voltQueueSQL(sql5, tenant_id, is_insert, is_update);
			break;
		case 6:
			voltQueueSQL(sql6, tenant_id, is_insert, is_update);
			break;
		case 7:
			voltQueueSQL(sql7, tenant_id, is_insert, is_update);
			break;
		case 8:
			voltQueueSQL(sql8, tenant_id, is_insert, is_update);
			break;
		}
		return voltExecuteSQL();
	}

}
