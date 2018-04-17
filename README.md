# CoStoSys
The Corpus Storage System (CoStoSys) is a tool and abstraction layer for a PostgreSQL document database.

It has originally been built for Pubmed/Medline documents but is content-agnostic except that the input must be in some form of XML. Configurable XPath expressions are used to extract portions of XML into database table columns. The main usage of tool is currently to store whole medline citation XML data in the database.

The documentation is contained in the project as a docbook at <code>src/docbkx</code>. Run `mvn clean site` to create a PDF an a HTML version of the docbook in <code>target/docbkx</code>.
Please note that the documentation is somewhat out of date. It still presents the main ideas behind the project.

For a rather exhaustive overview of the tool's capabilities, build it by running `mvn clean package` and run `java -jar target/costosys-<version>-cli-assembly.jar` without parameters.

For development, please note that currently multiple tests are ignored. This is due to the fact that they are integration tests and require a running PostgreSQL database. In the past, the JULIE Lab internal database was used which is no longer feasible. We need another solution, e.g. a Dockers image using [testcontainers](https://www.testcontainers.org/).

## Quickstart

NOTE: This is work in progress.

### Configuration file template

    <?xml version="1.0" encoding="UTF-8"?>
    <databaseConnectorConfiguration
            xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
            xmlns="http://julielab.de"
            xsi:schemaLocation="http://julielab.de https://github.com/JULIELab/costosys/blob/v1.0.1/src/main/resources/configuration.xsd">
        <DBSchemaInformation>
            <activePostgresSchema>mySchema</activePostgresSchema>
            <activeTableSchema>myTableSchema</activeTableSchema>
        </DBSchemaInformation>
        <DBConnectionInformation>
            <activeDBConnection></activeDBConnection>
            <maxActiveDBConnections>1</maxActiveDBConnections>
            <DBConnections>
                <DBConnection url="" name="" />
            </DBConnections>
        </DBConnectionInformation>
    </databaseConnectorConfiguration>

This is a template for the configuration file employed by `CoStoSys`. The structure of the file is formally defined
using an XML schema definition found at https://github.com/JULIELab/costosys/blob/master/src/main/resources/configuration.xsd.
Postgres supports schemas which are a kind of namespace. The element `activePostgresSchema` in the configuration means such a namespace schema.
The table schema, on the other hand, denotes the database table layout, i.e. their columns and datatypes. A range of table schema
have been predefined, including

|  name               | description |
|---------------------|------------- |
| medline_2017        | Defines the columns 'pmid' and 'xml'. Import data is expected to be in PubMed XML PubmedArticleSet format where one large XML file contains a bulk of PubMed articles. The individual articles must be located at XPath /PubmedArticleSet/PubmedArticle/MedlineCitation. This format is employed by the downloadable PubMed distribution since 2017. XML data are stored in GZIP format.|
| medline_2016        | Defines the columns 'pmid' and 'xml'. Import data is expected to be in MEDLINE XML MedlineCitationSet format where one large XML file contains a bulk of MEDLINE articles. The individual articles must be located at XPath /MedlineCitationSet/MedlineCitation. This format was employed by the downloadable MEDLINE distribution until 2016. XML data are stored in GZIP format.        |
| pubmed_gzip         | The same as medline_2017.           |
| xmi_text            | Defines the columns 'pmid', 'xmi', 'max_xmi_id' and 'sofa_mapping'. Used by the JeDIS components [jcore-xmi-db-reader](https://github.com/JULIELab/jcore-base/tree/b2128199bd548dd989b0d7c198634ed79670e8c7/jcore-xmi-db-reader) and [jcore-xmi-db-writer](https://github.com/JULIELab/jcore-base/tree/b2128199bd548dd989b0d7c198634ed79670e8c7/jcore-xmi-db-writer) to read and store UIMA annotation graphs in XMI format that were segmented into annotation types with separate storage.|
| xmi_annotation      | Defines the columns 'pmid' and 'xmi'. This table schema is used for the annotation data segmented away from full XMI annotation graphs, see xmi_text.          |
| xmi_text_gzip       | The same as xmi_text but the contents of the xmi column are stored an GZIP format.|
| max_id_addition     | Defines the fields 'pmid', 'xmi' and 'max_xmi_id' but only marks the 'max_xmi_id' column for retrieval. This schema is not supposed to be used for data import but for a table with xmi_text schema for which only the current maximum XMI ID should be retrieved. Technical detail of the JeDIS architecture.|
| xmi_annotation_gzip | The same as xmi_annotation but with the XMI data in GZIP format.|

Custom table schema may be added to the configuration at XPath `/databaseConnectorConfiguration/DBSchemaInformation/tableSchemas`. Refer to docbook documentation and the XML schema for details.