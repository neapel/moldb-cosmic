package moldb.cosmic;

import javax.swing.UIManager;

import org.apache.log4j.xml.DOMConfigurator;

public class Main {
	public static void main(final String[] args) throws Exception {

		// Logger
		DOMConfigurator.configure("log4j.xml");

		final Database db = new Database("sample.db");
		if(args.length < 2)
			db.init();

		try {
			UIManager.setLookAndFeel(UIManager.getSystemLookAndFeelClassName());
		} catch (final Exception e) {
		}
		new Gui(db.getConnection());
	}
}
