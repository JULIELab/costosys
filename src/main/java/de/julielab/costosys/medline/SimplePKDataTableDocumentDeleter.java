package de.julielab.costosys.medline;

import de.julielab.costosys.dbconnection.DataBaseConnector;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;

import static de.julielab.costosys.medline.ConfigurationConstants.TABLE;

public class SimplePKDataTableDocumentDeleter implements IDocumentDeleter {


    private static final Logger log = LoggerFactory.getLogger(SimplePKDataTableDocumentDeleter.class);

    private DataBaseConnector dbc;
    private String[] tablesForDeletions;

    public void setDbc(DataBaseConnector dbc) {
        this.dbc = dbc;
    }

    @Override
    public void deleteDocuments(List<String> docIds) {
        for (String table : tablesForDeletions) {
            log.debug(
                    "Deleting {} documents marked for deletion in update file from table \"{}\".",
                    docIds.size(), table);
            dbc.deleteFromTableSimplePK(table, docIds);
        }
    }

    @Override
    public void configure(HierarchicalConfiguration<ImmutableNode> deletionConfiguration) throws DocumentDeletionException {
        if (!deletionConfiguration.containsKey(TABLE))
            throw new DocumentDeletionException("Could not configure deleter '" + getName() + "' because its configuration lacks the '" + TABLE + "' element telling in which tables to remove deleted documents.");
        tablesForDeletions = deletionConfiguration.getStringArray(TABLE);
        log.info("Received the following tables for row deletion: {}", Arrays.toString(tablesForDeletions));
    }

    @Override
    public String getName() {
        return "database";
    }


}
