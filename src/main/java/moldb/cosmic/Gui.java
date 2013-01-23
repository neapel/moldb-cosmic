package moldb.cosmic;

import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.awt.event.ActionEvent;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Vector;

import javax.swing.AbstractAction;
import javax.swing.Action;
import javax.swing.JButton;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JScrollPane;
import javax.swing.JTable;
import javax.swing.JTextField;

public class Gui extends JFrame {

	public Gui(final Connection conn) {
		super("SQL-Abfrage");

		final JTextField input = new JTextField();
		final JScrollPane scroll = new JScrollPane();

		// SQL-Abfrage ausführen.
		final Action execute = new AbstractAction("Start") {
			@Override
			public void actionPerformed(final ActionEvent evt) {
				try {
					final Statement s = conn.createStatement();
					if (s.execute(input.getText())) { // Daten anzeigen
						final ResultSet r = s.getResultSet();

						// Spaltentitel
						final ResultSetMetaData m = r.getMetaData();
						final int columns = m.getColumnCount();
						final String[] names = new String[columns];
						for (int i = 0; i < columns; i++)
							names[i] = m.getColumnLabel(i + 1);

						// Reihen
						final Vector<String[]> data = new Vector<String[]>();
						while (r.next()) {
							final String[] values = new String[columns];
							for (int i = 0; i < columns; i++)
								values[i] = r.getString(i + 1);
							data.add(values);
						}

						final JTable t = new JTable(
								data.toArray(new Object[data.size()][columns]),
								names);
						t.setShowGrid(false);
						scroll.setViewportView(t);
					} else
						// geänderte Zeilen anzeigen
						scroll.setViewportView(new JLabel(s.getUpdateCount()
								+ " updates"));
				} catch (final SQLException e) {
					// Fehler anzeigen
					scroll.setViewportView(new JLabel(e.toString()));
				}
			}
		};

		// Fenster aufbauen:
		setLayout(new GridBagLayout());
		final GridBagConstraints c = new GridBagConstraints();
		c.fill = GridBagConstraints.BOTH;
		c.insets = new Insets(10, 10, 10, 10);

		// Enter führt Query aus
		input.setAction(execute);
		c.gridx = c.gridy = 0;
		c.weightx = 1;
		c.weighty = 0;
		add(input, c);

		// Oder Button
		final JButton submit = new JButton(execute);
		c.gridx = 1;
		c.gridy = 0;
		c.weightx = 0;
		c.weighty = 0;
		c.insets.left = 0;
		add(submit, c);

		// Scrollbares Fenster für Ergebnis
		c.gridx = 0;
		c.gridy = 1;
		c.weightx = 1;
		c.weighty = 1;
		c.insets = new Insets(0, 0, 0, 0);
		c.gridwidth = 2;
		add(scroll, c);

		pack();
		setSize(800, 500);

		// bei Schließen beenden
		setDefaultCloseOperation(EXIT_ON_CLOSE);
		setVisible(true);
	}
}