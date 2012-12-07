package moldb.cosmic;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Uniprot {

	public static List<String> readAccList(final String fileName)
			throws IOException {
		final List<String> accs = new ArrayList<String>();
		final Pattern pat = Pattern.compile("^UNIPROT:(\\S+)$");
		final BufferedReader r = new BufferedReader(new FileReader(fileName));
		for (String line = r.readLine(); line != null; line = r.readLine()) {
			final Matcher m = pat.matcher(line);
			// uniprot.acc has duplicates
			if (m.matches() && !accs.contains(m.group(1)))
				accs.add(m.group(1));
		}
		return accs;
	}

	public static void main(final String[] args) throws IOException {
		final List<String> s = readAccList("uniprot.acc");
		System.out.println(s.size() + " accs");
	}
}
