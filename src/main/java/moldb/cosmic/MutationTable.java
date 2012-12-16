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

import org.apache.commons.net.ftp.FTP;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;

public class MutationTable {

	public static void setup(final Connection conn) throws SQLException {
		final Statement s = conn.createStatement();
		s.executeUpdate("create table if not exists mutation (position NOT NULL, cosmid PRIMARY KEY, reference nucleotide NOT NULL, alternative nucleotide NOT NULL, coding, gene REFERENCES gene(name), strand, amino acid notation, count)");
		/*
		 * + "chromosome text collate nocase," + "position integer," +
		 * "id text collate nocase," + "reference text collate nocase," +
		 * "alternative text collate nocase," + "coding integer," +
		 * "gene text collate nocase," + "strand text," + "count integer)");
		 */
	}

	public static void teardown(final Connection conn) throws SQLException {
		final Statement s = conn.createStatement();
		s.executeUpdate("drop table if exists mutation");
	}

	static void read(final Connection conn, final String fileName,
			final boolean isCoding) throws IOException, SQLException {
		System.out.println("reading " + fileName);
		final BufferedReader reader = new BufferedReader(new InputStreamReader(
				new GZIPInputStream(new FileInputStream(fileName))));
		final PreparedStatement ps = conn
				.prepareStatement("insert into mutation values(?,?,?,?,?,?,?,?,?)");
		final PreparedStatement ps2 = conn
				.prepareStatement("update gene set chromosome = ? where name = ?");
		final PreparedStatement ps3 = conn
				.prepareStatement("select gene from synonym where synonym = ?");
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
		conn.commit();
		/*
		 * final BufferedReader reader = new BufferedReader(new
		 * InputStreamReader( new GZIPInputStream(new
		 * FileInputStream(fileName)))); final PreparedStatement ps = conn
		 * .prepareStatement("insert into mutation values(?,?,?,?,?,?,?,?,?)");
		 * for (String line = reader.readLine(); line != null; line = reader
		 * .readLine()) { if (line.startsWith("#")) continue; final String[]
		 * parts = line.split("\t"); for (int i = 0; i < parts.length; i++) if
		 * (parts[i].equals(".")) parts[i] = null; if (parts[7] == null)
		 * continue; ps.clearParameters(); ps.setString(1, parts[0]); // CHROM
		 * ps.setString(2, parts[1]); // POS ps.setString(3, parts[2]); // ID
		 * ps.setString(4, parts[3]); // REF ps.setString(5, parts[4]); // ALT
		 * ps.setBoolean(6, isCoding); String gene = null, strand = null, count
		 * = null; for (final String field : parts[7].split(";")) { final
		 * String[] kv = field.split("="); if (kv[0] == "GENE") gene = kv[1];
		 * else if (kv[0] == "STRAND") strand = kv[1]; else if (kv[0] == "CNT")
		 * count = kv[1]; } if (gene == null || strand == null) continue;
		 * ps.setString(7, gene); ps.setString(8, strand); ps.setString(9,
		 * count); ps.executeUpdate(); } reader.close(); conn.commit();
		 */
	}

	static String coding_name = "cosmic_coding.vcf.gz";
	static String noncoding_name = "cosmic_noncoding.vcf.gz";

	static void download() throws IOException {
		final FTPClient ftp = new FTPClient();
		ftp.connect("ftp.sanger.ac.uk");
		ftp.login("anonymous", "anonymous@example.org");
		ftp.changeWorkingDirectory("pub/CGP/cosmic/data_export/");
		for (final FTPFile ftpfile : ftp.listFiles()) {
			final String ftpname = ftpfile.getName();
			String outname = null;
			if (ftpname.startsWith("CosmicCodingMuts"))
				outname = coding_name;
			else if (ftpname.startsWith("CosmicNonCodingVariants"))
				outname = noncoding_name;
			if (outname != null) {
				final long ftpdate = ftpfile.getTimestamp().getTime().getTime();
				final File outfile = new File(outname);
				if (!outfile.exists() || outfile.lastModified() < ftpdate) {
					final FileOutputStream out = new FileOutputStream(outfile);
					System.out.println("downloading " + ftpname + " to "
							+ outname);
					ftp.setFileType(FTP.BINARY_FILE_TYPE);
					ftp.retrieveFile(ftpname, out);
					out.close();
					System.out.println("len " + ftpfile.getSize() + "/"
							+ outfile.length());
					outfile.setLastModified(ftpdate);
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
