package de.julielab.costosys.medline;

import de.julielab.costosys.Constants;
import de.julielab.costosys.dbconnection.DataBaseConnector;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class MedlineDataTableDocumentDeleter implements IDocumentDeleter {

	
	private static final Logger log = LoggerFactory.getLogger(MedlineDataTableDocumentDeleter.class);
	
	private DataBaseConnector dbc;


	public void setDbc(DataBaseConnector dbc) {
		this.dbc = dbc;
	}

	@Override
	public void deleteDocuments(List<String> docIds) {
		log.info(
				"Deleting {} documents marked for deletion in update file from table \"{}\".",
				docIds.size(), Constants.DEFAULT_DATA_TABLE_NAME);
		dbc.deleteFromTableSimplePK(Constants.DEFAULT_DATA_TABLE_NAME, docIds);
	}

	@Override
	public void configure(HierarchicalConfiguration<ImmutableNode> deletionConfiguration) {
		
	}

	@Override
	public String getName() {
		return "database";
	}


}
