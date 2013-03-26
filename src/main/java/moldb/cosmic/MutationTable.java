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
		synchronized (conn) {
			s.executeUpdate("create table if not exists mutation (position NOT NULL, cosmid PRIMARY KEY, reference_nucleotide NOT NULL, alternative_nucleotide, coding, gene REFERENCES gene(name), strand, amino_acid_notation, count)");
		}
	}

	public static void teardown(final Connection conn) throws SQLException {
		final Statement s = conn.createStatement();
		synchronized (conn) {
			s.executeUpdate("drop table if exists mutation");
		}
	}

	static void read(final Connection conn, final String fileName,
			final boolean isCoding) throws IOException, SQLException {
		logger.info("Reading COSMIC data from " + fileName + " coding="
				+ isCoding);
		final BufferedReader reader = new BufferedReader(new InputStreamReader(
				new GZIPInputStream(new FileInputStream(fileName))));
		final Statement s = conn.createStatement();
		final ResultSet rs = s.executeQuery("select * from synonym");
		// wenn die Datenbank das nur mit angezogener Handbremse schafft, muss
		// man das halt selber machen
		final Hashtable<String, String> syn = new Hashtable<String, String>(
				100000);
		while (rs.next())
			syn.put(rs.getString(2), rs.getString(1));
		final PreparedStatement insertMutation = conn
				.prepareStatement("insert or replace into mutation values(?,?,?,?,?,?,?,?,?)");
		final PreparedStatement setChromosome = conn
				.prepareStatement("update gene set chromosome = ? where name = ?");
		int lineNumber = 0, inserted = 0;
		for (String line = reader.readLine(); line != null; line = reader
				.readLine()) {
			lineNumber++;
			// if (lineNumber % 10000 == 0)
			// logger.debug("Line #" + lineNumber + ": " + line);
			if (line.startsWith("#"))
				continue;
			final String[] parts = line.split("\t");
			for (int i = 0; i < parts.length; i++)
				if (parts[i].equals("."))
					parts[i] = null;
			if (parts[7] == null)
				continue;
			insertMutation.clearParameters();
			setChromosome.clearParameters();
			setChromosome.setString(1, parts[0]); // CHROM
			insertMutation.setString(1, parts[1]); // POS
			insertMutation.setString(2, parts[2]); // ID
			insertMutation.setString(3, parts[3]); // REF
			insertMutation.setString(4, parts[4]); // ALT
			insertMutation.setBoolean(5, isCoding);
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

			gene = syn.get(gene);

			if (gene == null)
				continue;

			setChromosome.setString(2, gene);
			insertMutation.setString(6, gene);
			insertMutation.setString(7, strand);
			insertMutation.setString(8, aa);
			insertMutation.setString(9, count);
			synchronized (conn) {
				inserted++;
				setChromosome.executeUpdate();
				insertMutation.executeUpdate();
			}
		}
		reader.close();
		logger.debug("Done reading " + lineNumber + " lines.");
		logger.debug("Inserted " + inserted + " mutations.");

		/*
		 * final PreparedStatement selectGene = conn
		 * .prepareStatement("select gene from synonym where synonym = ?");
		 * selectGene.setString(1, gene); final ResultSet rs =
		 * selectGene.executeQuery(); if (rs.next()) { final String s =
		 * rs.getString(1); setChromosome.setString(2, s); synchronized (conn) {
		 * setChromosome.executeUpdate(); } insertMutation.setString(6, s); }
		 * else { noSynonym++; continue; } }
		 */
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
		setup(conn);
		// Evtl. neue Daten herunterladen
		download();
		read(conn, coding_name, true);
		read(conn, noncoding_name, false);
	}
}
