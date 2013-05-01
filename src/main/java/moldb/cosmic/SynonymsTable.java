package moldb.cosmic;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URL;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;

public class SynonymsTable {
	static Logger logger = Logger.getLogger(SynonymsTable.class);

	public static void setup(final Connection conn) throws SQLException {
		final Statement s = conn.createStatement();
		s.executeUpdate("CREATE TABLE IF NOT EXISTS synonym (official, name, kind, PRIMARY KEY (official, name))");
	}

	static void read(final Connection conn, final String fileName)
			throws IOException, SQLException {
		logger.info("Reading synonyms from " + fileName);
		final BufferedReader reader = new BufferedReader(new FileReader(
				fileName));
		reader.readLine(); // header
		
		final PreparedStatement insertSynonym = conn
				.prepareStatement("INSERT OR IGNORE INTO synonym VALUES(?,?,?)");
		for (String line = reader.readLine(); line != null; line = reader
				.readLine()) {
			final String[] fields = line.split("\t");
			final String sym = fields[0].trim(); // approved symbol
			insertSynonym.setString(1, sym);
			insertSynonym.setString(2, sym);
			insertSynonym.setString(3, "refl");
			insertSynonym.executeUpdate();
			if(fields.length > 1) // previous symbols
				for (final String s : fields[1].split(",")) {
					String r = s.trim();
					if(r.length() > 1) {
						insertSynonym.setString(1, sym);
						insertSynonym.setString(2, r);
						insertSynonym.setString(3, "previous");
						insertSynonym.executeUpdate();
					}
				}
			if(fields.length > 2) // aliases
				for (final String s : fields[2].split(",")){
					String r = s.trim();
					if(r.length() > 1) {
						insertSynonym.setString(1, sym);
						insertSynonym.setString(2, r);
						insertSynonym.setString(3, "alias");
						insertSynonym.executeUpdate();
					}
				}
		}
		reader.close();
		conn.commit();
	}

	static String synonyms_file = "synonyms.csv";
	static String synonyms_url = "http://www.genenames.org/cgi-bin/hgnc_downloads?col=gd_app_sym&col=gd_prev_sym&col=gd_aliases&status=Approved&status_opt=2&where=&order_by=gd_hgnc_id&format=text&limit=&submit=submit";

	static void download() throws IOException {
		final File outfile = new File(synonyms_file);
		if (!outfile.exists()) {
			logger.info("Downloading synonyms to " + synonyms_file);
			FileUtils.copyURLToFile(new URL(synonyms_url), outfile);
		} else
			logger.info("Not downloading synonyms, file exists.");
	}

	public static void init(final Connection conn) throws IOException,
			SQLException {
		setup(conn);
		download();
		read(conn, synonyms_file);
	}
}
