package moldb.cosmic;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class Database {
	static {
		try {
			Class.forName("org.sqlite.JDBC");
		} catch (final ClassNotFoundException e) {
			e.printStackTrace();
		}
	}

	private final Connection conn;

	public Database(final String fileName) throws SQLException {
		conn = DriverManager.getConnection("jdbc:sqlite:" + fileName);
		final Statement s = conn.createStatement();
		s.executeUpdate("pragma synchronous = off");
		s.executeUpdate("pragma count_changes = off");
		s.executeUpdate("pragma journal_mode = memory");
		s.executeUpdate("pragma temp_store = memory");
		conn.setAutoCommit(false);
	}

	public void init() throws IOException, SQLException {
		reset();
		UniProtBasedTables.init(conn);
		MutationTable.init(conn);
		SynonymsTable.init(conn);
		System.out.println("done");
	}

	private void reset() throws SQLException {
		UniProtBasedTables.teardown(conn);
		MutationTable.teardown(conn);
		SynonymsTable.teardown(conn);
		UniProtBasedTables.setup(conn);
		MutationTable.setup(conn);
		SynonymsTable.setup(conn);
	}

	public static void main(final String[] args) throws Exception {
		final Database db = new Database("sample.db");
		db.init();
	}

}