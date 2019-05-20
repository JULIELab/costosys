package de.julielab.costosys.configuration;

import java.io.IOException;

import org.apache.commons.lang3.StringUtils;

import com.ximpleware.AutoPilot;
import com.ximpleware.EOFException;
import com.ximpleware.EncodingException;
import com.ximpleware.EntityException;
import com.ximpleware.NavException;
import com.ximpleware.ParseException;
import com.ximpleware.PilotException;
import com.ximpleware.VTDException;
import com.ximpleware.VTDGen;
import com.ximpleware.VTDNav;
import com.ximpleware.XPathEvalException;
import com.ximpleware.XPathParseException;

public class DBConfig extends ConfigBase {

    private static final String XPATH_CONF_DB_INFO = "//DBConnectionInformation";
    private static final String XPATH_CONF_ACTIVE_DB_NAME = "//activeDBConnection";
    private static final String XPATH_CONF_ACTIVE_PG_SCHEMA = "//activePostgresSchema";
    private static final String XPATH_CONF_ACTIVE_DATA_PG_SCHEMA = "//activeDataPostgresSchema";

    private static final String XPATH_CONF_DBs = XPATH_CONF_DB_INFO + "/DBConnections";
    private static final String XPATH_CONF_DB = XPATH_CONF_DBs + "/DBConnection";
    private static final String XPATH_CONF_MAX_CON = XPATH_CONF_DB_INFO + "/maxActiveDBConnections";
    private static final String XPATH_CONF_TEMPLATE = XPATH_CONF_DB + "[@name='%s']";
    private static final String XPATH_CONF_URL_TEMPLATE = XPATH_CONF_TEMPLATE;
    private static final String ATTRIBUTE_URL = "url";

    private String url;
    private String activePGSchema;
    private int maxConnections = 5;
    private String activeDatabase;
    private String activeDataPGSchema;

    public DBConfig(byte[] configData) throws VTDException {
        try {
            // Convert the schema name to lower case as Postgres does so.
            activePGSchema = getActiveConfig(configData, XPATH_CONF_ACTIVE_PG_SCHEMA).toLowerCase();
            activeDataPGSchema = getActiveConfig(configData, XPATH_CONF_ACTIVE_DATA_PG_SCHEMA).toLowerCase();
            activeDatabase = getActiveConfig(configData, XPATH_CONF_ACTIVE_DB_NAME);
            buildDatabaseValues(activeDatabase, configData);

            VTDGen vg = new VTDGen();
            vg.setDoc(configData);
            vg.parse(true);
            VTDNav vn = vg.getNav();
            AutoPilot ap = new AutoPilot(vn);
            String maxConnString = stringFromXpath(ap, XPATH_CONF_MAX_CON);
            if (!StringUtils.isBlank(maxConnString))
                maxConnections = Integer.parseInt(maxConnString);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void buildDatabaseValues(String activeDatabase, byte[] mergedConfData)
            throws EncodingException, EOFException, EntityException, ParseException, XPathParseException,
            XPathEvalException, NavException, PilotException {
        VTDGen vg = new VTDGen();
        vg.setDoc(mergedConfData);
        vg.parse(true);
        VTDNav vn = vg.getNav();
        AutoPilot ap = new AutoPilot(vn);

        ap.selectXPath(String.format(XPATH_CONF_URL_TEMPLATE, activeDatabase));
        if (ap.evalXPath() != -1) {
            int attrIndex = vn.getAttrVal(ATTRIBUTE_URL);
            if (attrIndex != -1)
                url = vn.toString(attrIndex);
        } else {
            throw new IllegalArgumentException(
                    "There is no database connection named \"" + activeDatabase + "\" defined in the configuration.");
        }
    }

    /**
     * @return - url of the database
     */
    public String getUrl() {
        return url;
    }

    /**
     * @return The active Postgres Schema.
     */
    public String getActivePGSchema() {
        return activePGSchema;
    }

    /**
     * @param pgSchema
     */
    public void setActivePGSchema(String pgSchema) {
        activePGSchema = pgSchema;
    }

    /**
     * @return The active data Postgres Schema.
     */
    public String getActiveDataPGSchema() {
        return activeDataPGSchema;
    }

    public String getActiveDatabase() {
        return activeDatabase;
    }

    public int getMaxConnections() {
        return maxConnections;
    }

    public void setMaxConnections(int num) {
        maxConnections = num;
    }

    @Override
    public String toString() {
        String str;
        str = "Database URL: " + url + "\n";
        str += "Active Postgres schema: " + activePGSchema + "\n";
        str += "Max open connections: " + maxConnections;
        return str;
    }

}
