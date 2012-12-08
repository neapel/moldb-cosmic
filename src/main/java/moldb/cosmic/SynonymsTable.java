package moldb.cosmic;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URL;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

import org.apache.commons.io.FileUtils;

public class SynonymsTable {
	/*
	 * Fields: 0: HGNC ID 1: Approved Symbol 2: Approved Name 3: Status 4:
	 * Previous Symbols (,) 5: Previous Names 6: Synonyms (,) 7: Chromosome 8:
	 * Accession Numbers 9: RefSeq IDs
	 */

	public static void setup(final Connection conn) throws SQLException {
		final Statement s = conn.createStatement();
		s.executeUpdate("create table if not exists synonym (gene REFERENCES gene(name), synonym, PRIMARY KEY (gene, synonym))");
		// + "symbol collate nocase, synonym collate nocase)");
	}

	public static void teardown(final Connection conn) throws SQLException {
		final Statement s = conn.createStatement();
		s.executeUpdate("drop table if exists synonym");
	}

	static void read(final Connection conn, final String fileName)
			throws IOException, SQLException {
		System.out.println("reading " + fileName);
		final BufferedReader reader = new BufferedReader(new FileReader(
				fileName));
		reader.readLine(); // header
		final PreparedStatement ps = conn
				.prepareStatement("insert or ignore into synonym values(?,?)");
		final Statement stmt = conn.createStatement();
		final ResultSet rs = stmt.executeQuery("select name from gene");
		final ArrayList<String> genes = new ArrayList<String>();
		while (rs.next())
			genes.add(rs.getString(1));
		for (String line = reader.readLine(); line != null; line = reader
				.readLine()) {
			final String[] fields = line.split("\t");

			final ArrayList<String> symbols = new ArrayList<String>();
			symbols.add(fields[1]);
			if (fields[4].length() > 0)
				for (final String previous : fields[4].split(","))
					symbols.add(previous.trim());
			if (fields[6].length() > 0)
				for (final String synonym : fields[6].split(","))
					symbols.add(synonym.trim());
			for (final String s : symbols)
				if (genes.contains(s)) {
					ps.setString(1, s);
					for (final String str : symbols) {
						ps.setString(2, str);
						ps.execute();
					}
				}
		}
		reader.close();
		conn.commit();
	}

	static String synonyms_file = "synonyms.csv";
	static String synonyms_url = "http://www.genenames.org/cgi-bin/hgnc_downloads.cgi?title=HGNC+output+data&hgnc_dbtag=on&col=gd_hgnc_id&col=gd_app_sym&col=gd_app_name&col=gd_status&col=gd_prev_sym&col=gd_prev_name&col=gd_aliases&col=gd_pub_chrom_map&col=gd_pub_acc_ids&col=gd_pub_refseq_ids&status=Approved&status_opt=2&where=((gd_pub_chrom_map%20not%20like%20%27%patch%%27%20and%20gd_pub_chrom_map%20not%20like%20%27%ALT_REF%%27)%20or%20gd_pub_chrom_map%20IS%20NULL)%20and%20gd_locus_group%20%3d%20%27protein-coding%20gene%27&order_by=gd_app_sym_sort&format=text&limit=&submit=submit&.cgifields=&.cgifields=chr&.cgifields=status&.cgifields=hgnc_dbtag";

	static void download() throws IOException {
		final File outfile = new File(synonyms_file);
		if (!outfile.exists()) {
			System.out.println("downloading to " + synonyms_file);
			FileUtils.copyURLToFile(new URL(synonyms_url), outfile);
		}
	}

	public static void init(final Connection conn) throws IOException,
			SQLException {
		download();
		read(conn, synonyms_file);
	}
}
