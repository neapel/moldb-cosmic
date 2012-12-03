package moldb.cosmic;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Uniprot {

	public static List<String> readAccList(String fileName) throws IOException {
		List<String> accs = new ArrayList<String>();
		Pattern pat = Pattern.compile("^UNIPROT:(\\S+)$");
		BufferedReader r = new BufferedReader(new FileReader(fileName));
		for (String line = r.readLine(); line != null; line = r.readLine()) {
			Matcher m = pat.matcher(line);
			if (m.matches())
				accs.add(m.group(1));
		}
		return accs;
	}

	public static void main(String[] args) throws IOException {
		List<String> s = readAccList("uniprot.acc");
		System.out.println(s.size() + " accs");
	}
}
