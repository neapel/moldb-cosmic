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

		logger.info("Creating views.");
		final Statement s = conn.createStatement();
		s.executeUpdate("CREATE VIEW IF NOT EXISTS genename AS SELECT DISTINCT s1.name AS a, s2.name AS b, s1.official FROM synonym s1, synonym s2 WHERE s1.official = s2.official");
		s.executeUpdate("CREATE TEMP TABLE IF NOT EXISTS proteinmut AS SELECT m.id AS mid, m.*, p.id AS pid, p.*, n.official AS gene FROM mutation m, genename n, product g, protein p WHERE m.gene = n.a AND n.b = g.gene AND g.protein = p.id");
		conn.commit();
		logger.info("Done.");
	}
}
