package moldb.cosmic;

import uk.ac.ebi.kraken.uuw.services.remoting.*;
import uk.ac.ebi.kraken.interfaces.uniprot.*;
import uk.ac.ebi.kraken.interfaces.uniprot.comments.*;
import uk.ac.ebi.kraken.interfaces.uniprot.description.Name;
import uk.ac.ebi.kraken.interfaces.uniprot.genename.GeneNameSynonym;

public class Main {
	public static void main(String[] args) {
		EntryRetrievalService entryRetrievalService = UniProtJAPI.factory
				.getEntryRetrievalService();
		UniProtEntry entry = entryRetrievalService.getUniProtEntry("P42284");
		if (entry != null) {
			System.out.println("entry = " + entry.getUniProtId().getValue());
			System.out.println("primary acc "
					+ entry.getPrimaryUniProtAccession().getValue());

			for (SecondaryUniProtAccession a : entry
					.getSecondaryUniProtAccessions())
				System.out.println("secondary acc " + a.getValue());

			ProteinDescription d = entry.getProteinDescription();
			System.out.println("rec name " + d.getRecommendedName());
			for (Name n : d.getAlternativeNames())
				System.out.println("alt name " + n);
			for (String n : d.getEcNumbers())
				System.out.println("ec# " + n);

			for (Gene g : entry.getGenes()) {
				System.out.println("gene " + g.getGeneName());
				for (GeneNameSynonym s : g.getGeneNameSynonyms())
					System.out.println("  = " + s.getValue());

			}

			for (Comment x : entry
					.getComments(CommentType.ALTERNATIVE_PRODUCTS)) {
				for (AlternativeProductsIsoform k : ((AlternativeProductsComment) x)
						.getIsoforms()) {
					System.out.println("isoform " + k.getName().getValue());
					for (IsoformId e : k.getIds()) {
						System.out.println("  " + e.getValue());
					}
				}

			}

		}
	}
}
