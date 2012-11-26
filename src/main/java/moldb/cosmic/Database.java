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
			} catch (ClassNotFoundException e) {
				e.printStackTrace();
			}
	}

	private Connection conn;

	public Database(String fileName) throws SQLException {
			conn = DriverManager.getConnection("jdbc:sqlite:" + fileName);
			Statement s = conn.createStatement();
			s.executeUpdate("pragma synchronous = off");
			s.executeUpdate("pragma count_changes = off");
			s.executeUpdate("pragma journal_mode = memory");
			s.executeUpdate("pragma temp_store = memory");
			conn.setAutoCommit(false);
	}
	
	public void init() throws IOException, SQLException {
		reset();
		System.out.println("coding");
		long start = System.currentTimeMillis();
		MutationTable.read(conn, "WGS_CodingMuts_v60_190712.vcf", true);
		System.out.println("took " + (System.currentTimeMillis() - start) + "ms");
		System.out.println("noncoding");
		start = System.currentTimeMillis();
		MutationTable.read(conn, "WGS_NonCodingVariants_v60_190712.vcf", false);
		System.out.println("took " + (System.currentTimeMillis() - start) + "ms");

		System.out.println("synonyms");
		start = System.currentTimeMillis();
		SynonymsTable.read(conn, "hgnc-proteincoding.csv");
		System.out.println("took " + (System.currentTimeMillis() - start) + "ms");
		
		System.out.println("done");
	}

	private void reset() throws SQLException {
		MutationTable.teardown(conn);
		SynonymsTable.teardown(conn);
		MutationTable.setup(conn);
		SynonymsTable.setup(conn);
	}

	public static void main(String[] args) throws Exception {
		Database db = new Database("sample.db");
		db.init();
	}

}