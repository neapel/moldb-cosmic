package moldb.cosmic;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

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
	static String path = "uniprot.acc";

	public static void setup(final Connection conn) throws SQLException {
		final Statement s = conn.createStatement();
		s.executeUpdate("create table if not exists protein (name PRIMARY KEY, uniprotid NOT NULL, accession NOT NULL, sequence NOT NULL)");
		s.executeUpdate("create table if not exists gene (name PRIMARY KEY, coded protein REFERENCES protein(name))");
		s.executeUpdate("create table if not exists isoform (id PRIMARY KEY, protein REFERENCES protein(name), sequence)");
	}

	public static void teardown(final Connection conn) throws SQLException {
		final Statement s = conn.createStatement();
		s.executeUpdate("drop table if exists protein");
		s.executeUpdate("drop table if exists gene");
		s.executeUpdate("drop table if exists isoform");
	}

	static void read(final Connection conn, final List<String> acc)
			throws SQLException {
		final UniProtQueryService uniProtQueryService = UniProtJAPI.factory
				.getUniProtQueryService();
		final Query query = UniProtQueryBuilder.buildIDListQuery(acc);
		final EntryIterator<UniProtEntry> entries = uniProtQueryService
				.getEntryIterator(query);
		final PreparedStatement protps = conn
				.prepareStatement("insert into protein values (?,?,?,?)");
		final PreparedStatement geneps = conn
				.prepareStatement("insert into gene values (?,?)");
		final PreparedStatement isops = conn
				.prepareStatement("insert into isoform values (?,?,?)");
		int i = 0;
		// System.out.println(entries.getResultSize());
		for (final UniProtEntry entry : entries) {
			final String recName = entry.getProteinDescription()
					.getRecommendedName().getFieldsByType(FieldType.FULL)
					.get(0).getValue();
			protps.setString(1, recName);
			protps.setString(2, entry.getUniProtId().getValue());
			protps.setString(3, acc.get(i++));
			protps.setString(4, entry.getSequence().getValue());
			protps.execute();
			for (final Gene g : entry.getGenes()) {
				geneps.setString(1, g.hasGeneName() ? g.getGeneName()
						.getValue() : null);
				geneps.setString(2, recName);
				geneps.execute();
			}
			isops.setString(2, recName);
			for (final Comment com : entry
					.getComments(CommentType.ALTERNATIVE_PRODUCTS))
				for (final AlternativeProductsIsoform iso : ((AlternativeProductsComment) com)
						.getIsoforms()) {
					isops.setString(3,
							entry.getSplicedSequence(iso.getName().getValue()));
					for (final IsoformId id : iso.getIds()) {
						isops.setString(1, id.getValue());
						isops.execute();
					}
				}
		}
		conn.commit();
	}

	static <T> List<T> subList(final List<T> list, int from, final int to) {
		final List<T> newList = new ArrayList<T>();
		while (from < to)
			newList.add(list.get(from++));
		return newList;
	}

	public static void init(final Connection conn) throws IOException,
			SQLException {
		List<String> acclist = Uniprot.readAccList(path);
		// 1024 max length for uniprot queries
		// java fucking sucks > sublist returns non-serializable object
		final List<String> acclist2 = subList(acclist, 1024, acclist.size());
		acclist = subList(acclist, 0, 1024);

		read(conn, acclist);
		read(conn, acclist2);
	}
}
