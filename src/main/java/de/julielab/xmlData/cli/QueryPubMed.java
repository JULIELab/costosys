package de.julielab.xmlData.cli;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLEncoder;
import java.util.ArrayList;

import com.ximpleware.AutoPilot;
import com.ximpleware.EOFException;
import com.ximpleware.EncodingException;
import com.ximpleware.EntityException;
import com.ximpleware.NavException;
import com.ximpleware.ParseException;
import com.ximpleware.VTDGen;
import com.ximpleware.VTDNav;
import com.ximpleware.XPathEvalException;
import com.ximpleware.XPathParseException;

import de.julielab.xml.JulieXMLTools;

public class QueryPubMed {
	private final static String SITE = "http://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi";
	private final static String RETMAX = "100000000"; // 5x the size of PubMed (2011)
	private final static int BUFFERSIZE = 1024;
	private final static String XPATH = "/eSearchResult/IdList/Id";

	/**
	 * Query PubMed via REST-API, returning up to 10e8 matched PMIDs.
	 * Queried terms get expanded, e.g. "Il-1" will match "interleukin-1".
	 * Searches with really many results (e.g. "cancer") need increased heap space!
	 * More details: http://eutils.ncbi.nlm.nih.gov/corehtml/query/static/esearch_help.html 
	 * 
	 * @param query -Query for PubMed as a String
	 * @return - ArrayList, containing PMIDs as Strings
	 */
	public static ArrayList<String> query(String query) {
		ArrayList<String> ids = new ArrayList<String>();
		try {
			StringBuilder queryBuilder = new StringBuilder();
			queryBuilder.append(SITE)
					.append("?term=").append(URLEncoder.encode(query, "UTF-8"))
					.append("&retmax=").append(RETMAX).append("&tool=julie-medline-manager")
					.append("&email=julielab@listserv.uni-jena.de");
			URL url = new URL(queryBuilder.toString());
			InputStream stream = url.openStream();	
			
			VTDGen vg = new VTDGen(); // Parses XML
			vg.setDoc(JulieXMLTools.readStream(stream, BUFFERSIZE));
			vg.parse(true);
			VTDNav vn = vg.getNav(); // Navigates in parsed XML
			AutoPilot ap = new AutoPilot(vn); // Moves through whole XML
			
			ap.selectXPath(XPATH);
			while (ap.evalXPath() != -1) {
				// 32 bits encoding length, 32 bits encoding offset
				long fragment = vn.getContentFragment(); 
				// right 32 bits
				int offset = (int) fragment; 
				// left 32 bits, casts priority is higher than right-shifts
				int length = (int) (fragment >> 32); 
				ids.add(vn.toString(offset, length));
			}
			
		} catch (IOException e) {
			e.printStackTrace();
		} catch (EncodingException e) {
			e.printStackTrace();
		} catch (EOFException e) {
			e.printStackTrace();
		} catch (EntityException e) {
			e.printStackTrace();
		} catch (ParseException e) {
			e.printStackTrace();
		} catch (XPathParseException e) {
			e.printStackTrace();
		} catch (XPathEvalException e) {
			e.printStackTrace();
		} catch (NavException e) {
			e.printStackTrace();
		}
		
		return ids;
	}
}