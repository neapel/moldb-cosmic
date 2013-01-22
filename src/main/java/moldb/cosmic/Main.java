package moldb.cosmic;

import org.apache.log4j.xml.DOMConfigurator;

public class Main {
	public static void main(final String[] args) throws Exception {
		// Logger
		DOMConfigurator.configure("log4j.xml");

		final Database db = new Database("sample.db");
		db.init();
	}
}
