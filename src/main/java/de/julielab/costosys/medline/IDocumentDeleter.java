package de.julielab.costosys.medline;

import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;

import java.util.List;

public interface IDocumentDeleter {

	void deleteDocuments(List<String> docIds);

	/**
	 * Passes the XML contents of the <tt>deletion</tt> element.
	 * @param deletionConfiguration The subconfiguration where the <tt>deletion</tt> element for the current deleter is the root.
	 * @throws DocumentDeletionException If configuration fails.
	 */
	void configure(HierarchicalConfiguration<ImmutableNode> deletionConfiguration) throws DocumentDeletionException;

	default boolean isOneOf(String... names) {
		for (int i = 0; i < names.length; i++) {
			String name = names[i];
			if (name.equals(getName()))
				return true;
		}
		return false;
	}

	String getName();

}
