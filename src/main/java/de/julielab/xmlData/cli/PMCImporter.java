package de.julielab.xmlData.cli;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.sql.SQLException;
import java.util.Date;

import de.julielab.xmlData.Constants;
import de.julielab.xmlData.dataBase.DataBaseConnector;

public class PMCImporter {
	public static void main(String[] args) throws FileNotFoundException, SQLException {
		if (args.length != 1) {
			System.err.println("Parameters: <pmc root directory>");
		}

		DataBaseConnector dbc = new DataBaseConnector(new FileInputStream(CLI.USER_SCHEME_DEFINITION));
		String dataTable = Constants.DEFAULT_DATA_TABLE_NAME;
		if (!dbc.tableExists(dataTable))
			dbc.createTable(dataTable, "Created by PMCImporter at " + new Date());
		
		File pmcRoot = new File(args[0]);
		File[] pmcSubdirs = pmcRoot.listFiles();

		for (File subdir : pmcSubdirs) {
			File[] xmlFiles = subdir.listFiles();
			for (File f : xmlFiles) {
				System.out.println("Importing file " + f.getAbsolutePath());
				dbc.updateFromXML(f.getAbsolutePath(), dataTable);
			}
		}

	}
}
