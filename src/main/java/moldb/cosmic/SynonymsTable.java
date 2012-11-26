package moldb.cosmic;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;

public class SynonymsTable {
	/*
	 * Fields: 0: HGNC ID 1: Approved Symbol 2: Approved Name 3: Status 4:
	 * Previous Symbols (,) 5: Previous Names 6: Synonyms (,) 7: Chromosome 8:
	 * Accession Numbers 9: RefSeq IDs
	 */

	public static void setup(Connection conn) throws SQLException {
		Statement s = conn.createStatement();
		s.executeUpdate("create table if not exists synonym (" +
				"symbol collate nocase, synonym collate nocase)");
	}

	public static void teardown(Connection conn) throws SQLException {
		Statement s = conn.createStatement();
		s.executeUpdate("drop table if exists synonym");
	}

	public static void read(Connection conn, String fileName) throws IOException, SQLException {
		BufferedReader reader = new BufferedReader(new FileReader(fileName));
		reader.readLine(); // header
		PreparedStatement ps = conn.prepareStatement(
				"insert into synonym values(?,?)");
		for (String line = reader.readLine(); line != null; line = reader.readLine()) {
			String[] fields = line.split("\t");
			ps.setString(1, fields[1]); // Approved Symbol
			if (fields[4].length() > 0)
				for (String previous : fields[4].split(",")) {
					ps.setString(2, previous.trim());
					ps.execute();
				}
			if (fields[6].length() > 0)
				for (String synonym : fields[6].split(",")) {
					ps.setString(2, synonym.trim());
					ps.execute();
				}
		}
		reader.close();
		conn.commit();
	}

}
