package moldb.cosmic;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;

public class MutationTable {

	public static void setup(final Connection conn) throws SQLException {
		final Statement s = conn.createStatement();
		s.executeUpdate("create table if not exists mutation ("
				+ "chromosome text collate nocase," + "position integer,"
				+ "id text collate nocase," + "reference text collate nocase,"
				+ "alternative text collate nocase," + "coding integer,"
				+ "gene text collate nocase," + "strand text,"
				+ "count integer)");
	}

	public static void teardown(final Connection conn) throws SQLException {
		final Statement s = conn.createStatement();
		s.executeUpdate("drop table if exists mutation");
	}

	public static void read(final Connection conn, final String fileName,
			final boolean isCoding) throws IOException, SQLException {
		final BufferedReader reader = new BufferedReader(new FileReader(
				fileName));
		final PreparedStatement ps = conn
				.prepareStatement("insert into mutation values(?,?,?,?,?,?,?,?,?)");
		for (String line = reader.readLine(); line != null; line = reader
				.readLine()) {
			if (line.startsWith("#"))
				continue;
			final String[] parts = line.split("\t");
			for (int i = 0; i < parts.length; i++)
				if (parts[i].equals("."))
					parts[i] = null;
			if (parts[7] == null)
				continue;
			ps.clearParameters();
			ps.setString(1, parts[0]); // CHROM
			ps.setString(2, parts[1]); // POS
			ps.setString(3, parts[2]); // ID
			ps.setString(4, parts[3]); // REF
			ps.setString(5, parts[4]); // ALT
			ps.setBoolean(6, isCoding);
			String gene = null, strand = null, count = null;
			for (final String field : parts[7].split(";")) {
				final String[] kv = field.split("=");
				if (kv[0] == "GENE")
					gene = kv[1];
				else if (kv[0] == "STRAND")
					strand = kv[1];
				else if (kv[0] == "CNT")
					count = kv[1];
			}
			if (gene == null || strand == null)
				continue;
			ps.setString(7, gene);
			ps.setString(8, strand);
			ps.setString(9, count);
			ps.executeUpdate();
		}
		reader.close();
		conn.commit();
	}
}
