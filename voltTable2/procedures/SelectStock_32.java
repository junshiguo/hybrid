import org.voltdb.*;
public class SelectStock_32 extends VoltProcedure {
	public final SQLStmt sql = new SQLStmt("SELECT * FROM stock32 WHERE tenant_id = ? AND is_insert = ? AND is_update = ?");
	public VoltTable[] run(int tenant_id, int is_insert, int is_update) throws VoltAbortException {
		voltQueueSQL(sql, tenant_id, is_insert, is_update);
		return voltExecuteSQL();
	}
}