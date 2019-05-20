package de.julielab.costosys.configuration;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ximpleware.AutoPilot;
import com.ximpleware.VTDException;
import com.ximpleware.VTDGen;
import com.ximpleware.VTDNav;
import com.ximpleware.XPathParseException;

import de.julielab.costosys.dbconnection.DataBaseConnector;



public abstract class ConfigBase {
	protected static final Logger LOG = LoggerFactory.getLogger(DataBaseConnector.class);
	/**
	 * Inserts the user schemes into the default configuration. This makes all
	 * data available in one place, which is useful for referencing default
	 * values from within a user configuration.
	 * 
	 * @param defaultConfData
	 *            - prepared default configuration file
	 * @param userConfData
	 *            - prepared user specific configuration file
	 * @param xpath
	 *            - path to the part to merge
	 * @return - the merged configuration
	 * @throws VTDException
	 * @throws IOException
	 */
//	protected byte[] mergeConfigData(byte[] defaultConfData,
//			byte[] userConfData, String xpath) throws VTDException, IOException {
//		VTDGen vg = new VTDGen();
//		vg.setDoc(defaultConfData);
//		vg.parse(true);
//		VTDNav vn = vg.getNav();
//		AutoPilot ap = new AutoPilot(vn);
//		ap.selectXPath(xpath);
//		if (ap.evalXPath() != -1) {
//			XMLModifier xm = new XMLModifier(vn);
//			xm.insertAfterElement(userConfData);
//			ByteArrayOutputStream os = new ByteArrayOutputStream();
//			xm.output(os);
//			return os.toByteArray();
//		}
//		return null;
//	}

	/**
	 * Retrieves XML elements (determined by the used path) 
	 * from the configuration.
	 * 
	 * @param confData
	 *            - prepared XML
	 * @param xpath
	 *            - path to the retrieved configuration element
	 * @return - the retrieved element
	 * @throws IOException
	 * @throws VTDException
	 */
	protected byte[] extractConfigData(byte[] confData, String xpath)
			throws IOException, VTDException {
		byte[] configData;
		VTDGen vg = new VTDGen();
		vg.setDoc(confData);
		vg.parse(true);
		VTDNav vn = vg.getNav();
		AutoPilot ap = new AutoPilot(vn);
		ap.selectXPath(xpath);
		StringBuilder sb = new StringBuilder();

		while (ap.evalXPath() != -1) {
			long fragment = vn.getElementFragment();
			int offset = (int) fragment;
			int length = (int) (fragment >> 32);
			sb.append(vn.toRawString(offset, length));
		}

		configData = sb.toString().getBytes();
		return configData;
	}

	/**
	 * Used to determine which of the elements in the configuration file
	 * is set as active.
	 * 
	 * @param data - the configuration file
	 * @param activePath - xpath to the element which defines the active configuration
	 * @return - the name of the active configuration
	 * @throws IOException
	 * @throws VTDException
	 */
	public static String getActiveConfig(byte[] data, String activePath)
			throws IOException, VTDException {
		VTDGen vg = new VTDGen();
		vg.setDoc(data);
		vg.parse(true);
		VTDNav vn = vg.getNav();
		AutoPilot ap = new AutoPilot(vn);
		ap.selectXPath(activePath);
		if (ap.evalXPath() != -1) {
			int textNodeIndex = vn.getText();
			if (textNodeIndex != -1)
				return vn.toRawString(textNodeIndex);
		}
		return null;
	}
	
	/**
	 * Used to get an element by its relative xpath.
	 * 
	 * @param ap - a vtd autopilot
	 * @param xpath - xpath to the element which shall be returned
	 * @return - String representation of the element
	 * @throws XPathParseException
	 */
	protected String stringFromXpath(AutoPilot ap, String xpath)
			throws XPathParseException {
		ap.selectXPath(xpath);
		String string = ap.evalXPathToString();
		ap.resetXPath();
		return string;
	}

}
