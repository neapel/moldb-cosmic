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
		s.executeUpdate("create table if not exists mutation (position NOT NULL, cosmid PRIMARY KEY, reference nucleotide NOT NULL, alternative nucleotide NOT NULL, coding, gene REFERENCES gene(name), strand, amino acid notation, count)");
	}

	public static void teardown(final Connection conn) throws SQLException {
		final Statement s = conn.createStatement();
		s.executeUpdate("drop table if exists mutation");
	}

	static void read(final Connection conn, final String fileName,
			final boolean isCoding) throws IOException, SQLException {
		logger.info("Reading COSMIC data from " + fileName + " coding="
				+ isCoding);
		final BufferedReader reader = new BufferedReader(new InputStreamReader(
				new GZIPInputStream(new FileInputStream(fileName))));
		final PreparedStatement ps = conn
				.prepareStatement("insert or replace into mutation values(?,?,?,?,?,?,?,?,?)");
		final PreparedStatement ps2 = conn
				.prepareStatement("update gene set chromosome = ? where name = ?");
		final PreparedStatement ps3 = conn
				.prepareStatement("select gene from synonym where synonym = ?");
		int lineNumber = 0;
		for (String line = reader.readLine(); line != null; line = reader
				.readLine()) {
			lineNumber++;
			// Kompromiss zwischen jedes mal commiten (sehr langsam) und erst am
			// Ende (auch langsam).
			if (lineNumber % 1000 == 0)
				conn.commit();
			if (lineNumber % 10000 == 0)
				logger.debug("Line #" + lineNumber + ": " + line);
			// DEBUG
			if (lineNumber > 20000)
				break;
			if (line.startsWith("#"))
				continue;
			final String[] parts = line.split("\t");
			for (int i = 0; i < parts.length; i++)
				if (parts[i].equals("."))
					parts[i] = null;
			if (parts[7] == null)
				continue;
			ps.clearParameters();
			ps2.setString(1, parts[0]); // CHROM
			ps.setString(1, parts[1]); // POS
			ps.setString(2, parts[2]); // ID
			ps.setString(3, parts[3]); // REF
			ps.setString(4, parts[4]); // ALT
			ps.setBoolean(5, isCoding);
			String gene = null, strand = null, count = null, aa = null;
			for (final String field : parts[7].split(";")) {
				final String[] kv = field.split("=");
				if (kv[0] == "GENE")
					gene = kv[1];
				else if (kv[0] == "STRAND")
					strand = kv[1];
				else if (kv[0] == "CNT")
					count = kv[1];
				else if (kv[0] == "AA")
					aa = kv[1];
			}
			// if (gene == null || strand == null)
			// continue;
			ps3.setString(1, gene);
			final ResultSet rs = ps3.executeQuery();
			if (rs.next()) {
				final String s = rs.getString(1);
				ps2.setString(2, s);
				ps2.execute();
				ps.setString(6, s);
			} else
				continue; // gene not in db
			ps.setString(7, strand);
			ps.setString(8, aa);
			ps.setString(9, count);
			ps.executeUpdate();
		}
		reader.close();
		logger.debug("Done reading.");
		conn.commit();
		logger.debug("Commited.");
	}

	static String coding_name = "cosmic_coding.vcf.gz";
	static String noncoding_name = "cosmic_noncoding.vcf.gz";

	static void download() throws IOException {
		logger.info("Checking COSMIC data.");
		final FTPClient ftp = new FTPClient();
		ftp.connect("ftp.sanger.ac.uk");
		ftp.login("anonymous", "-anonymous@example.org");
		ftp.changeWorkingDirectory("/pub/CGP/cosmic/data_export/");
		ftp.enterLocalPassiveMode();
		for (final FTPFile ftpfile : ftp.listFiles()) {
			final String ftpname = ftpfile.getName();
			logger.debug("Check file " + ftpname);
			String outname = null;
			if (ftpname.startsWith("CosmicCodingMuts"))
				outname = coding_name;
			else if (ftpname.startsWith("CosmicNonCodingVariants"))
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
						new Thread() {
							@Override
							public void run() {
								while (cos.getCount() != ftpfile.getSize()) {
									try {
										Thread.sleep(3000);
									} catch (final InterruptedException e) {
									}
									logger.debug(Math.round(100.0
											* cos.getCount()
											/ ftpfile.getSize())
											+ "% of "
											+ ftpfile.getSize()
											/ 1024 + "kB downloaded.");
								}
							};
						}.start();
						ftp.retrieveFile(ftpname, cos);
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
		download();
		read(conn, coding_name, true);
		read(conn, noncoding_name, false);
	}
}
