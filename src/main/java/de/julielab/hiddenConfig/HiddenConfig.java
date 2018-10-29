package de.julielab.hiddenConfig;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.julielab.xmlData.Constants;

/**
 * This class reads a hidden configuration file in the users home directory. If no such file exists, a new one can be
 * created.
 * 
 * @author hellrich
 * 
 */
public class HiddenConfig {

	private static final int USER = 0;

	private static final int PASS = 1;

	private Logger LOG = LoggerFactory.getLogger(HiddenConfig.class);

	private Map<String, String[]> authentication = new HashMap<String, String[]>();

	private File configFile;

	/**
	 * Reads a hidden config file in the users home directory.
	 */
	public HiddenConfig() {
		String home = System.getProperty("user.home");
		String fileSeparator = System.getProperty("file.separator");
		String fileStr = home + fileSeparator + ".dbcUser"; // hidden file in
															// home directory
		File homeDir = new File(home);
		File homeConfigFile = new File(fileStr);
		String directPath = System.getProperty(Constants.HIDDEN_CONFIG_PATH);

		if (directPath != null)
			configFile = new File(directPath);
		else if (homeDir.exists())
			// Perhaps we have no home (happens sometimes when we want to work
			// on a machine which has not yet connected to our homes).
			configFile = homeConfigFile;
		else {
			LOG.warn("No home directory found and the \"" + Constants.HIDDEN_CONFIG_PATH
					+ "\" is not set; thus, the configuration file is stored in the working directory.");
			configFile = new File(homeConfigFile.getName());
		}
		if (!configFile.exists()) {
			try {
				configFile.createNewFile();
			} catch (IOException e1) {
				LOG.error("Could not create " + configFile.getPath());
			}
		}

		if (!configFile.canRead()) {
			LOG.error("Could not read from " + configFile.getPath());
		}

		FileReader fr = null;
		try {
			fr = new FileReader(configFile);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		BufferedReader br = new BufferedReader(fr);
		try {
			while (br.ready()) {
				String DBConnectionName = br.readLine();
				if (authentication.get(DBConnectionName) != null) {
					LOG.error("You must not have multiple entries for one DBConnections name!");
					throw new IOException();
				} else {
					String[] userAndPass = new String[2];
					userAndPass[USER] = br.readLine();
					userAndPass[PASS] = br.readLine();
					authentication.put(DBConnectionName, userAndPass);
					// there should be an empty line as separator
					if (br.readLine() == null) {
						String msg = "Your .dbcUser file has malformed content.";
						LOG.error(msg);
						throw new IOException("The CoStoSys credentials caching file at "+configFile+" file has malformed content.");
					}
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * @return - The username in the hidden config file
	 * 
	 */
	public String getUsername(String DBConnectionName) {
		return getFromMap(DBConnectionName, USER);
	}

	/**
	 * @return - The password in the hidden config file
	 * 
	 */
	public String getPassword(String DBConnectionName) {
		return getFromMap(DBConnectionName, PASS);
	}

	private String getFromMap(String DBConnectionName, int what) {
		if (authentication.containsKey(DBConnectionName))
			return authentication.get(DBConnectionName)[what];
		else {
			createEntry(DBConnectionName);
			return getFromMap(DBConnectionName, what);
		}
	}

	private void createEntry(String DBConnectionName) {
		BufferedReader in = new BufferedReader(new InputStreamReader(System.in));

		try {
			while (true) {
				System.out.println("This is the interactive credentials entry dialog to access the database with name " + DBConnectionName + ". Do you wish to enter your credentials now - Y/N ?");
				String answer = null;
				try {
					answer = in.readLine();
				} catch (IOException e) {
					LOG.error("Could not read from comandline!?", e);
				}
				if (answer.equalsIgnoreCase("Y"))
					break;
				if (answer.equalsIgnoreCase("N")) {
					System.out.println("Aborting...");
					System.exit(0);
				}
			}
		} catch (Exception e1) {
			LOG.error(
					"An error occurred during the user-interactive dialog to create a new login for the database {}. If you are running into this error with a program that is not meant to be interactive, you have create the login entry beforehand. You can do this by doing something with the DataBaseConnector, e.g. showing all tables with the -t switch. Alternatively, you can directly edit the hidden configuration file. By default, it is found at ${HOME}/.dbcUser. The original error was: ",
					DBConnectionName, e1);
			e1.printStackTrace();
		}

		String[] userAndPass = new String[2];
		System.out.println("Please enter your username for " + DBConnectionName);
		try {
			userAndPass[USER] = in.readLine();
			System.out.println("Please enter your password for " + DBConnectionName);
			userAndPass[PASS] = in.readLine();
			authentication.put(DBConnectionName, userAndPass);
		} catch (IOException e) {
			LOG.error("Could not read from comandline!?", e);
		}

		try {
			if (configFile.exists() && configFile.canWrite()) {
				String lineSeparator = System.getProperty("line.separator");
				FileWriter fw = new FileWriter(configFile, true);
				fw.write(DBConnectionName);
				fw.write(lineSeparator);
				fw.write(userAndPass[USER]);
				fw.write(lineSeparator);
				fw.write(userAndPass[PASS]);
				fw.write(lineSeparator);
				fw.write(lineSeparator); // empty separator line
				fw.close();
			}
		} catch (IOException e) {
			LOG.error("Could not write to hidden file.", e);
		}
	}
}
