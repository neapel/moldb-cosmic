package moldb.cosmic;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;

import uk.ac.ebi.kraken.interfaces.uniprot.Gene;
import uk.ac.ebi.kraken.interfaces.uniprot.UniProtEntry;
import uk.ac.ebi.kraken.interfaces.uniprot.comments.AlternativeProductsComment;
import uk.ac.ebi.kraken.interfaces.uniprot.comments.AlternativeProductsIsoform;
import uk.ac.ebi.kraken.interfaces.uniprot.comments.Comment;
import uk.ac.ebi.kraken.interfaces.uniprot.comments.CommentType;
import uk.ac.ebi.kraken.interfaces.uniprot.comments.IsoformId;
import uk.ac.ebi.kraken.interfaces.uniprot.description.FieldType;
import uk.ac.ebi.kraken.uuw.services.remoting.EntryIterator;
import uk.ac.ebi.kraken.uuw.services.remoting.Query;
import uk.ac.ebi.kraken.uuw.services.remoting.UniProtJAPI;
import uk.ac.ebi.kraken.uuw.services.remoting.UniProtQueryBuilder;
import uk.ac.ebi.kraken.uuw.services.remoting.UniProtQueryService;

public class UniProtBasedTables {
	static Logger logger = Logger.getLogger(UniProtBasedTables.class);
	static String path = "uniprot.acc";

	public static List<String> readAccList(final String fileName)
			throws IOException {
		final List<String> accs = new ArrayList<String>();
		final Pattern pat = Pattern.compile("^UNIPROT:(\\S+)$");
		final BufferedReader r = new BufferedReader(new FileReader(fileName));
		for (String line = r.readLine(); line != null; line = r.readLine()) {
			final Matcher m = pat.matcher(line);
			// 26 Zeilen enthalten Duplikate, müssen somit nicht
			// eingelesen werden
			if (m.matches() && !accs.contains(m.group(1)))
				accs.add(m.group(1));
		}
		r.close();
		return accs;
	}

	public static void setup(final Connection conn) throws SQLException {
		final Statement s = conn.createStatement();
		s.executeUpdate("CREATE TABLE IF NOT EXISTS protein (id PRIMARY KEY, name, accession, sequence)");
		s.executeUpdate("CREATE TABLE IF NOT EXISTS product (gene, protein REFERENCES protein(id), PRIMARY KEY(gene, protein))");
		s.executeUpdate("CREATE TABLE IF NOT EXISTS isoform (id PRIMARY KEY, protein REFERENCES protein(id), sequence)");
	}

	static boolean hasProtein(final Connection conn, final String acc) {
		try {
			final PreparedStatement s = conn
					.prepareStatement("SELECT count(*) != 0 FROM protein WHERE accession = ?");
			s.setString(1, acc);
			final ResultSet rs = s.executeQuery();
			rs.next();
			return rs.getBoolean(1);
		} catch (final SQLException e) {
			return false;
		}
	}

	static void read(final Connection conn, final List<String> accs)
			throws SQLException {
		logger.info("Retrieving " + accs.size() + " ACCs from Uniprot.");
		final UniProtQueryService uniProtQueryService = UniProtJAPI.factory
				.getUniProtQueryService();
		final Query query = UniProtQueryBuilder.buildIDListQuery(accs);
		final EntryIterator<UniProtEntry> entries = uniProtQueryService
				.getEntryIterator(query);
		final PreparedStatement protps = conn
				.prepareStatement("INSERT OR REPLACE INTO protein VALUES (?,?,?,?)");
		final PreparedStatement geneps = conn
				.prepareStatement("INSERT OR IGNORE INTO product VALUES (?,?)");
		final PreparedStatement isops = conn
				.prepareStatement("INSERT OR REPLACE INTO isoform VALUES (?,?,?)");
		final List<String> primAcc = new ArrayList<String>();
		for (final UniProtEntry entry : entries) {
			final String recName = entry.getProteinDescription()
					.getRecommendedName().getFieldsByType(FieldType.FULL)
					.get(0).getValue();
			final String acc = entry.getPrimaryUniProtAccession().getValue();
			final String id = entry.getUniProtId().getValue();
			primAcc.add(acc);
			protps.setString(1, id);
			protps.setString(2, recName);
			protps.setString(3, acc);
			protps.setString(4, entry.getSequence().getValue());
			protps.executeUpdate();
			for (final Gene g : entry.getGenes()) {
				geneps.setString(1, g.getGeneName().getValue());
				geneps.setString(2, id);
				geneps.executeUpdate();
			}
			isops.setString(2, id);
			for (final Comment com : entry
					.getComments(CommentType.ALTERNATIVE_PRODUCTS))
				for (final AlternativeProductsIsoform iso : ((AlternativeProductsComment) com)
						.getIsoforms()) {
					isops.setString(3,
							entry.getSplicedSequence(iso.getName().getValue()));
					for (final IsoformId iid : iso.getIds()) {
						isops.setString(1, iid.getValue());
						isops.executeUpdate();
					}
				}
		}
		for (final String s : accs)
			if (!primAcc.contains(s))
				logger.debug("No entry found for: " + s
						+ " (most likely deleted)");
		logger.info("Done.");
		conn.commit();
		logger.info("Commited.");
	}

	static <T> List<List<T>> chunks(final List<T> list, final int size) {
		final List<List<T>> newList = new ArrayList<List<T>>();
		for (int i = 0; i < list.size(); i += size)
			newList.add(new ArrayList<T>(list.subList(i,
					Math.min(i + size, list.size()))));
		return newList;
	}

	public static void init(final Connection conn) throws IOException,
			SQLException {
		setup(conn);
		final List<String> acclist = readAccList(path);

		// ACCs die schon in der Datenbank stehen herausfiltern.
		final List<String> acclist_new = new ArrayList<String>();
		for (final String acc : acclist)
			if (!hasProtein(conn, acc))
				acclist_new.add(acc);

		// zum Debuggen: gar nicht neu laden wenn nicht alle neu sind.
		if (acclist_new.size() != acclist.size())
			return;

		logger.info("Loading " + acclist_new.size() + "/" + acclist.size()
				+ " ACCs from Uniprot, in chunks:");
		// 1024 max length for uniprot queries
		final List<List<String>> acclists = chunks(acclist_new, 1024);
		for (final List<String> sublist : acclists)
			read(conn, sublist);
		logger.info("All done.");
	}
}
