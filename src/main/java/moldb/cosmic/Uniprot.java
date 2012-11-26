package moldb.cosmic;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Uniprot {

	public static List<String> readAccList(String fileName) throws IOException {
		List<String> accs = new ArrayList<>();
		Pattern pat = Pattern.compile("^UNIPROT:(\\S+)$");
		for (String line : Files.readAllLines(FileSystems.getDefault().getPath(fileName),
				Charset.defaultCharset())) {
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
