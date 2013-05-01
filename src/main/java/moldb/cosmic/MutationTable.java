package moldb.cosmic;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Hashtable;
import java.util.zip.GZIPInputStream;

import org.apache.commons.io.output.CountingOutputStream;
import org.apache.commons.net.ftp.FTP;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;
import org.apache.commons.net.io.CopyStreamException;
import org.apache.log4j.Logger;

public class MutationTable {
	static Logger logger = Logger.getLogger(MutationTable.class);

	public static void setup(final Connection conn) throws SQLException {
		final Statement s = conn.createStatement();
			s.executeUpdate("CREATE TABLE IF NOT EXISTS mutation (id PRIMARY KEY, gene, coding, position, reference, alternative, strand, aa, count)");
	}

	static void read(final Connection conn, final String fileName,
			final boolean isCoding) throws IOException, SQLException {
		logger.info("Reading COSMIC data from " + fileName + " coding="
				+ isCoding);
		final BufferedReader reader = new BufferedReader(new InputStreamReader(
				new GZIPInputStream(new FileInputStream(fileName))));
		final PreparedStatement insertMutation = conn
				.prepareStatement("INSERT OR REPLACE INTO MUTATION VALUES(?,?,?,?,?,?,?,?,?)");
		int lineNumber = 0, inserted = 0;
		for (String line = reader.readLine(); line != null; line = reader
				.readLine()) {
			lineNumber++;
			if (line.startsWith("#"))
				continue;
			final String[] parts = line.split("\t");
			for (int i = 0; i < parts.length; i++)
				if (parts[i].equals("."))
					parts[i] = null;
			if (parts[7] == null)
				continue;
			final String pos = parts[1], id = parts[2], ref = parts[3], alt = parts[4];
			String gene = null, strand = null, count = null, aa = null;
			for (final String field : parts[7].split(";")) {
				final String[] kv = field.split("=");
				if (kv.length != 2)
					continue;
				if (kv[0].equalsIgnoreCase("GENE"))
					gene = kv[1];
				else if (kv[0].equalsIgnoreCase("STRAND"))
					strand = kv[1];
				else if (kv[0].equalsIgnoreCase("CNT"))
					count = kv[1];
				else if (kv[0].equalsIgnoreCase("AA"))
					aa = kv[1];
			}
			insertMutation.setString(1, id);
			insertMutation.setString(2, gene);
			insertMutation.setBoolean(3, isCoding);
			insertMutation.setString(4, pos);
			insertMutation.setString(5, ref);
			insertMutation.setString(6, alt);
			insertMutation.setString(7, strand);
			insertMutation.setString(8, aa);
			insertMutation.setString(9, count);
			inserted++;
			insertMutation.executeUpdate();
		}
		conn.commit();
		reader.close();
		logger.debug("Done reading " + lineNumber + " lines.");
		logger.debug("Inserted " + inserted + " mutations.");
	}

	static String coding_name = "cosmic_coding.vcf.gz";
	static String noncoding_name = "cosmic_noncoding.vcf.gz";

	static void download() throws IOException {
		logger.info("Checking COSMIC data.");
		final FTPClient ftp = new FTPClient();
		ftp.connect("ngs.sanger.ac.uk");
		ftp.login("anonymous", "-anonymous@example.org");
		ftp.changeWorkingDirectory("/production/cosmic/");
		ftp.enterLocalPassiveMode();
		for (final FTPFile ftpfile : ftp.listFiles()) {
			final String ftpname = ftpfile.getName();
			logger.debug("Check file " + ftpname);
			String outname = null;
			if (ftpname.startsWith("CosmicCodingMuts_"))
				outname = coding_name;
			else if (ftpname.startsWith("CosmicNonCodingVariants_"))
				outname = noncoding_name;
			if (outname != null) {
				final long ftpdate = ftpfile.getTimestamp().getTime().getTime();
				final File outfile = new File(outname);
				if (!outfile.exists() || outfile.lastModified() < ftpdate)
					try {
						final FileOutputStream out = new FileOutputStream(
								outfile);
						final CountingOutputStream cos = new CountingOutputStream(
								out);
						logger.info("Downloading " + ftpname + " to " + outname);
						ftp.setFileType(FTP.BINARY_FILE_TYPE);
						Thread t = new Thread() {
							@Override
							public void run() {
								final double mb = ftpfile.getSize() / 1024.0 / 1024.0;
								while (cos.getCount() != ftpfile.getSize()) {
									try {
										Thread.sleep(3000);
									} catch (final InterruptedException e) {
									}
									final double pc = 100.0 * cos.getCount()
											/ ftpfile.getSize();
									logger.debug(String.format(
											"%.1f%% of %.2fMB downloaded.", pc,
											mb));
								}
							};
						};
						t.start();
						ftp.retrieveFile(ftpname, cos);
						t.stop();
						out.close();
						outfile.setLastModified(ftpdate);
					} catch (final CopyStreamException e) {
						logger.error("Error while Copying: "
								+ e.getCause().getMessage(), e.getCause());
						if (outfile.exists())
							outfile.delete();
					}
			}
		}
	}

	public static void init(final Connection conn) throws IOException,
			SQLException {
		setup(conn);
		// Evtl. neue Daten herunterladen
		download();
		read(conn, coding_name, true);
		read(conn, noncoding_name, false);
	}
}
