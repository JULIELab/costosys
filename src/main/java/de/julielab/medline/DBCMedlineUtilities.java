package de.julielab.medline;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Iterator;

public class DBCMedlineUtilities {
	public void writeDocumentsIntoMedlineCitationSet(Iterator<byte[][]> it,
			int indexOfDocumentData) {
		try {
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			baos.write("<MedlineCitationSet>".getBytes());
			while (it.hasNext()) {
				byte[][] next = it.next();
				baos.write(next[indexOfDocumentData]);
				baos.write("\n".getBytes());
			}
			baos.write("</MedlineCitationSet>".getBytes());
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
