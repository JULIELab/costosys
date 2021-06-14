package de.julielab.costosys.medline;

import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;

import java.util.List;
import java.util.Set;

public interface IDocumentDeleter {

	void deleteDocuments(List<String> docIds);

	void configure(HierarchicalConfiguration<ImmutableNode> deletionConfiguration) throws MedlineDocumentDeletionException;

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
