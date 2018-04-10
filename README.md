# CoStoSys
The Corpus Storage System (CoStoSys) is a tool and abstraction layer for a PostgreSQL document database.

It has originally been built for Pubmed/Medline documents but is content-agnostic except that the input must be in some form of XML. Configurable XPath expressions are used to extract portions of XML into database table columns. The main usage of tool is currently to store whole medline citation XML data in the database.

The documentation is contained in the project as a docbook at <code>src/docbkx</code>. Run `mvn clean site` to create a PDF an a HTML version of the docbook in <code>target/docbkx</code>.
Please note that the documentation is somewhat out of date. It still presents the main ideas behind the project.

For a rather exhaustive overview of the tool's capabilities, build it by running `mvn clean package` and run `java -jar target/costosys-<version>-cli-assembly.jar` without parameters.

For development, please note that currently multiple tests are ignored. This is due to the fact that they are integration tests and require a running PostgreSQL database. In the past, the JULIE Lab internal database was used which is no longer feasible. We need another solution, e.g. a Dockers image using [testcontainers](https://www.testcontainers.org/).
