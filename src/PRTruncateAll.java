
import org.voltdb.*;

public class PRTruncateAll extends VoltProcedure {
	public final SQLStmt sql0 = new SQLStmt("TRUNCATE TABLE warehouse;");
	public final SQLStmt sql1 = new SQLStmt("TRUNCATE TABLE district;");
	public final SQLStmt sql2 = new SQLStmt("TRUNCATE TABLE customer;");
	public final SQLStmt sql3 = new SQLStmt("TRUNCATE TABLE history;");
	public final SQLStmt sql4 = new SQLStmt("TRUNCATE TABLE new_orders;");
	public final SQLStmt sql5 = new SQLStmt("TRUNCATE TABLE orders;");
	public final SQLStmt sql6 = new SQLStmt("TRUNCATE TABLE order_line;");
	public final SQLStmt sql7 = new SQLStmt("TRUNCATE TABLE item;");
	public final SQLStmt sql8 = new SQLStmt("TRUNCATE TABLE stock;");
	
	public VoltTable[] run(int table) throws VoltAbortException {
		switch(table){
		case 0:
			voltQueueSQL(sql0);
			break;
		case 1:
			voltQueueSQL(sql1);
			break;
		case 2:
			voltQueueSQL(sql2);
			break;
		case 3:
			voltQueueSQL(sql3);
			break;
		case 4:
			voltQueueSQL(sql4);
			break;
		case 5:
			voltQueueSQL(sql5);
			break;
		case 6:
			voltQueueSQL(sql6);
			break;
		case 7:
			voltQueueSQL(sql7);
			break;
		case 8:
			voltQueueSQL(sql8);
			break;
		}
		return voltExecuteSQL();
	}

}
