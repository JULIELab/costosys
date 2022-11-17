# Changelog

## v1.6.1 (17/11/2022)
- [**enhancement**] Create a XmiColumnDataInserter [#16](https://github.com/JULIELab/costosys/issues/16)
- [**closed**] Add 'nsAware' attribute to 'tableSchema' [#15](https://github.com/JULIELab/costosys/issues/15)
- [**closed**] Allow update files to be processed again. [#13](https://github.com/JULIELab/costosys/issues/13)
- [**closed**] Make unshared connections available [#10](https://github.com/JULIELab/costosys/issues/10)
- [**closed**] Restructure Medline import/update [#9](https://github.com/JULIELab/costosys/issues/9)
- [**enhancement**] Upgrade Postgres container version to 11.12. [#8](https://github.com/JULIELab/costosys/issues/8)

---

## v1.6.0 (28/09/2021)
- [**closed**] Add a parameter for deleting rows by ID in a given table. [#14](https://github.com/JULIELab/costosys/issues/14)
- [**enhancement**] Let the database document deleter delete from multiple tables [#12](https://github.com/JULIELab/costosys/issues/12)
- [**closed**] Implement JeDIS binary format decoding for CLI queries [#11](https://github.com/JULIELab/costosys/issues/11)
- [**closed**] Add "drop table" functionality [#3](https://github.com/JULIELab/costosys/issues/3)

---

## v1.5.2 (11/06/2021)
- [**enhancement**] Delete reference to removed mirror subset [#2](https://github.com/JULIELab/costosys/issues/2)
- [**enhancement**] Mention database configuration file parameter [#1](https://github.com/JULIELab/costosys/issues/1)

---

## v1.4.0 (03/06/2019)
- [Bug fix for partial resets.](https://github.com/JULIELab/costosys/commit/b9af0b99f86dc73adafd14297e5875c9e2026064) - @khituras
- [Cleaned the code of the `ConfigReader` class.](https://github.com/JULIELab/costosys/commit/4d73d5ce76c8881776cf510f74114aba136d7ea0) - @khituras
- [Removing `System.exit()` calls from the `CLI` class.](https://github.com/JULIELab/costosys/commit/1505d4055bb87c838427a377f1bf094278ddfb45) - @khituras
- [Adding files due to package name refactoring, removing System.exit() from Updater.](https://github.com/JULIELab/costosys/commit/37cf9e863926dd8a6819bb0ed407e295b1a0e087) - @khituras

---

## v1.3.2 (20/05/2019)
Setting the subset table column `host_name` to null when resetting the subset table.
---

## v1.3.1 (23/04/2019)
Some logging at CLI startup about the used schemas has been added.
Also, the timeout for trying to get a connection from the connection pool has been raised from 30s to 60s.
---

## v1.3.0 (28/03/2019)
This is mostly a maintenance release. The new minor revision number stems from the fact that as of this version, CoStoSys required Java11.
---

## v1.2.4 (25/12/2018)
Important bugfix release. The connection concurrency still had issues. As of this version there is no known issue.
---

## v1.2.3 (06/12/2018)
Updates of large collections now need less memory.

Until this version, each file containing documents to be updated was exhaustively read to
avoid duplicate document IDs in the same update batch which can lead
to primary key violation errors. This has now been broken down
in updating batches up to 10k documents.
---

## v1.2.2 (05/12/2018)
This release fixes a bug where a `NullPointerException` would happen when a ZIP archive is read at the last file is a non-XML file.
The bug actually resided in the `julie-xml-tools` dependency and has been fixed there with version `0.4.2`.
---

## v1.2.1 (11/10/2018)
Concurrency issues where observed in v1.2.0 were connections were closed that are still in use in another place. While this can in theory be solved by the calling code, two small adjustments have been made in the hope to avoid the observed errors:

* Iterators obtain connections for the main thread but close them in their own thread. They now close their connections more quickly so they won't be picked up by other code and then get closed so the other code will throw an error.
* When obtaining a connection, now always the newest connection is returned. This ensured more control about which connection is passed to methods following a connection reservation call.
---

## v1.2.0 (20/09/2018)
This release has completely refactored connection management. The recommended method to obtain a connection is now `obtainOrReserveConnection()` which will return an object of type `CoStoSysConnection`. This object is just a small wrapper around the actual `Connection` object and the information whether a new reservation was necessary or an already reserved connection is being reused. This is important when releasing the connection: We don't want to release an obtained connection that is still used by an algorithm in another scope.
To safely release such connections, use the method `releaseConnection(CoStoSysConnection)`. It will only close the connection if the respective connection was newly reserved but the corresponding call to `obtainOrReserveConnection()`. Note that the `CoStoSysConnection` class is `AutoClosable`. Thus, the recommended use is the `try(CoStoSysConnection cstsConn = dbc.obtainOrReserveConnection) {...}` pattern.

Another major update in this release is the MEDLINE update method (`-um` parameter in the CLI). This is not just the already known XML update mechanism but is specifically designed for the PubMed/MEDLINE update files obtained from the NCBI FTP server. A dedicated database table is created that keeps track of which update file has when been applied to the MEDLINE data table in the database. Since the update files also specify deletions of documents, this release also contains the functionality to remove such documents from the database and even an ElasticSearch index. The deletion functionality has not yet been tested but may work just fine.
---

## v1.1.3 (14/08/2018)
This release fixes an issue with concurrency deadlocks when trying to obtain connections by multiple threads. However, it is already known that there are more issues. Beginning with v1.2.0, there will be a connection reservation system that should resolve the issue. The problem is that a lot of methods obtain connections on their own. Even when they release them quickly this can lead to a thread waiting on itself to finish with a connection it holds because it is trying to obtain another one. When two threads are doing this and the connection pool is empty, there is a deadlock.
---

## v1.1.2 (10/08/2018)
Fixed issues introduced in the v1.1.0 release concerning the iterators taking a connection as argument. This could cause NPEs in the case when no connection was given. The intended release 1.1.1 only fixed this incompletly, thus the omission of 1.1.1 in the release list.
---

## v1.1.0 (06/08/2018)
This release adds support for the Postgres XML type. This does just mean that the XML type may now be specified in table schemas, there is no particular support for querying such data built into CoStoSys.

Fixed a bug when delivering a non-default, schema-qualified data table in the configuration wouldn't work properly.
The database iterators can now be given an external connection to use. This may lower the requirements on the number of connections from the pool. For example, querying subset tables did not work with only a single connection in the connection pool. This does work now.
---

## v1.0.7 (25/07/2018)
This is a minor maintanance release. Nicer messages and such.
---

## v1.0.6 (13/06/2018)
Added new methods to create an XMI primary text data or XMI annotation data schema given a primary key.
Updated julie-xml-tools to 0.4.0 which allows to read ZIP archives containing multiple XML files, possible in GZIP format.
---

## v1.0.5 (09/05/2018)
Added a method to query data with a limit parameter.
---

## v1.0.4 (07/05/2018)
Re-introduced the previously removed method but with a non-ambiguous signature.
---

## v1.0.3 (07/05/2018)
Minor API change.

The method `checkTableSchemaCompatibility(String, String...)` was removed. There is also the method `checkTableSchemaCompatibility(String...)` which was ambiguous for calls like `checkTableSchemaCompatibility("schema1", "schema2")`. Since the first method did just call the second, it was removed to simplify things.
---

## v1.0.2 (27/04/2018)
Added convenience methods for better usage of XMI storage tables and other quality of life methods. The data retrieval iterators are now separate classes and have been generalized to read multiple fields. Bug fixes.
---

## v1.0.1 (17/04/2018)
Important: The system has been redubbed from JeDIS to CoStoSys. JeDIS denotes a more general architecture for database supported UIMA processing that includes UIMA components ([jcore-xmi-db-reader](https://github.com/JULIELab/jcore-base/tree/b2128199bd548dd989b0d7c198634ed79670e8c7/jcore-xmi-db-reader) and [jcore-xmi-db-writer](https://github.com/JULIELab/jcore-base/tree/b2128199bd548dd989b0d7c198634ed79670e8c7/jcore-xmi-db-writer)) and annotation graph segmentation using [jcore-xmi-splitter](https://github.com/JULIELab/jcore-dependencies/tree/3c80c3a8f1264276a5b9e256aab049eb6861ab23/jcore-xmi-splitter). The Corpus Storage System (CoStoSys) is the part managing the database.

In this version, primarily some documentation was added. The most important change, besides the renaming, is the new configuration.xsd XMI schema for the configuration file. Check the README for more information. Note that when using CoStoSys from the command line, the configuration file name is now sought case insensitive and may be called dbcconfiguration.xml, costosysconfiguration.xml and costosys.xml.
---

## v1.0.0 (31/01/2018)
The first release of JeDIS, a system to store large XML text corpora in a database. JeDIS offers facilities to create subsets of the main corpus data table without copying any data but just using foreign keys. The system is originally meant to work with the [jcore-db-reader](https://github.com/JULIELab/jcore-base/tree/master/jcore-ace-reader) which uses subsets to synchronize multiple corpus readers and to track progress and errors.