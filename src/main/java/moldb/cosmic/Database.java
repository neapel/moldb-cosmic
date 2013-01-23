package moldb.cosmic;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.log4j.Logger;

public class Database {
	static Logger logger = Logger.getLogger(Database.class);

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

	public Connection getConnection() {
		return conn;
	}

	public void init() throws IOException, SQLException {
		logger.info("Creating Database.");
		UniProtBasedTables.init(conn);
		SynonymsTable.init(conn);
		MutationTable.init(conn);
		System.out.println("done");
	}

	private void reset() throws SQLException {
		UniProtBasedTables.teardown(conn);
		SynonymsTable.teardown(conn);
		MutationTable.teardown(conn);
		UniProtBasedTables.setup(conn);
		SynonymsTable.setup(conn);
		MutationTable.setup(conn);
	}

}