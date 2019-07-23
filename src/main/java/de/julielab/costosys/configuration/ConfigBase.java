package de.julielab.costosys.configuration;

import com.ximpleware.*;
import de.julielab.costosys.dbconnection.DataBaseConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;



public abstract class ConfigBase {

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
			throws VTDException {
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
