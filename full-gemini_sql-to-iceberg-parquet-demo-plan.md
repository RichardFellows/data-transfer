---
title: SQL to Iceberg Parquet Demo Plan
tags: []
author: gemini
count: 7
exporter: 2.7.1
date: 2025-10-05T21-17-50+0100
url: https://gemini.google.com/app/9310e09cbc70f841
---

# SQL to Iceberg Parquet Demo Plan

## Table of Contents

- [1: i want to implement a small tech demo which will extract data from a S…](#chat-1)
- [2: Start research](#chat-2)
- [3: in this demo \- where would the iceberg metadata be stored? we want to…](#chat-3)
- [4: are there any REST catalogues that would work on a developer machine,…](#chat-4)
- [5: i agree, tabular image seems like the best fit. Can you now help me to…](#chat-5)
- [6: give me a markdown file that contains all of this conversation and res…](#chat-6)
- [7: how can a save this full conversation to a single file?](#chat-7)

### chat-1

> i want to implement a small tech demo which will extract data from a SQL Server db as defined via a T-SQL statement, and have  is saved into a local parquet file in an Iceberg table format. I'm  looking at this blog as the inspiration. https://medium.com/@xorets/writing-iceberg-tables-with-c-a-journey-through-schema-compatibility-b169c846f5e0. Can you help me to make a plan?

Here's the plan I've put together. Let me know if you need any changes before I start researching.
 C# Iceberg Table Generation Plan Research Websites(1) Thoroughly analyze the provided blog post to deconstruct its methodology, identify the C# libraries used for Parquet generation, and understand the approach for creating Apache Iceberg metadata files.
(2) Research the standard C#/.NET libraries and best practices for connecting to a SQL Server database, executing T-SQL statements, and processing the resulting data streams.
(3) Consult the official Apache Iceberg documentation to gain a deep understanding of the table specification, focusing on the physical file layout and the precise structure and content required for:
   (a) The table metadata JSON file.
   (b) Manifest lists.
   (c) Manifest files.
(4) Investigate the process for schema translation, specifically how to map data types from a SQL Server result set to the corresponding types in the Apache Parquet and Apache Iceberg specifications.
(5) Formulate a detailed, step-by-step procedure for programmatically generating the Iceberg metadata in C#. This plan should detail the creation of the manifest file (linking to the Parquet data file), the manifest list, and the final table metadata file to commit a new table snapshot.
(6) Find tools and methods for validating the output. Research how to use query engines or libraries (such as Spark, DuckDB with an Iceberg extension, or a Python library) to read the locally generated files and confirm they constitute a valid and queryable Iceberg table.
(7) Synthesize all the gathered information into a comprehensive implementation plan for the tech demo. This plan should outline the entire workflow from project setup and data extraction to Parquet file writing, Iceberg metadata generation, and final validation of the table.MoreAnalyze ResultsCreate ReportReady in a few mins Edit plan  Start research

___
###### [top](#table-of-contents)
### chat-2

> Start research

I've completed your research. Feel free to ask me follow-up questions or request changes.C# Iceberg Table Generation PlanCompleted5 Oct, 15:18Open

___
###### [top](#table-of-contents)
### chat-3

> in this demo - where would the iceberg metadata be stored? we want to minimize external dependencies where-ever possible

Excellent question. To align with your goal of minimizing external dependencies, the Iceberg metadata for this demo will be stored directly on the **local filesystem** alongside the Parquet data files.

This approach uses what is known as a "filesystem catalog," which is the simplest way to manage an Iceberg table and requires no external services, databases, or network daemons.

Here’s a breakdown of how it will be structured:

1. **Root Table Directory:** A single main directory will be created for the table (e.g., `/path/to/my_iceberg_table/`). This directory will contain everything needed for the table to be a self-contained, portable unit.
2. **Subdirectory Structure:** Inside this root directory, the generated files will be organized into two standard subdirectories:

    - `data/`: This will contain all the actual data files, which in our case will be the `.parquet` files.

    - `metadata/`: This is where all the Iceberg-specific metadata files will live.
3. **Metadata Files:** The `metadata/` directory will contain the full hierarchy of files that define the table's state :   

    - **Table Metadata (`.metadata.json`):** A versioned JSON file (e.g., `v1.metadata.json`, `v2.metadata.json`) that acts as the single source of truth for the table's schema, partition information, and a list of all historical snapshots.

    - **Manifest List (`.avro`):** An Avro file for each snapshot that points to the specific manifest files making up that version of the table.

    - **Manifest Files (`.avro`):** Avro files that list the actual data files (`.parquet`) and contain detailed statistics about them, which is crucial for efficient query planning.
4. **The "Catalog" Pointer:** To identify the current version of the table without an external service, a simple text file named `version-hint.txt` will be placed in the `metadata/` directory. This file contains a single number indicating which metadata file is the current one (e.g., "1" to point to `v1.metadata.json`). This is a standard, lightweight mechanism for filesystem-based Iceberg tables.

By storing all metadata as files on the local disk, the entire tech demo becomes completely self-contained, avoiding any dependency on services like a Hive Metastore, AWS Glue, or a REST catalog service.

___
###### [top](#table-of-contents)
### chat-4

> are there any REST catalogues that would work on a developer machine, perhaps with sqlite backing?

Yes, absolutely. Moving from a simple filesystem catalog to a local REST catalog is an excellent step for a tech demo. It more closely mimics a production environment and provides a standardized API for table management, all while running self-contained on your developer machine.

There are several great options for running a lightweight Iceberg REST catalog locally, and many of them can indeed use SQLite as a backend to maintain that minimal-dependency footprint.

Here are a few of the best approaches:

### 1. The Tabular Docker Image (Recommended)

This is often the quickest and most common way to get a local REST catalog running. The team at Tabular provides a reference implementation of the Iceberg REST catalog in a Docker image.

- **How it Works:** The image runs a server that exposes the standard Iceberg REST API endpoints. By default, it is configured to use a JDBC (Java Database Connectivity) catalog that is backed by an in-memory SQLite database, which is perfect for local testing and development.
- **To run it, you simply need Docker installed and can execute the following command:**

```Bash
docker run -p 8181:8181 tabulario/iceberg-rest:latest
```
This will start the REST catalog server and make it available on `http://localhost:8181` on your machine. Your C# application can then be configured to point to this URI to manage its tables.

### 2. Other Community REST Catalog Implementations

The Iceberg community has produced other REST catalog projects that are also suitable for local development with SQLite. For example, there are Python-based implementations that you can build and run via Docker.

- **How it Works:** These projects provide a Dockerfile that builds a lightweight web server implementing the REST catalog specification. The base versions of these images are often designed to use SQLite as the metadata backend.
- **Example Usage:** You would typically clone the project's repository, build the Docker image, and then run the container, exposing the necessary port (e.g., `docker run -p 8000:8000 my-iceberg-rest-image`).

### 3. Standalone Toolsets like Altinity ice

If you prefer not to use Docker, some projects provide standalone command-line tools to run a catalog.

- **How it Works:** Altinity's `ice` is an open-source toolset that includes a command, `ice-rest-catalog`, which starts a local REST catalog. This implementation is a wrapper around the official Iceberg Java libraries, which are the reference implementation for the specification. This option requires a Java runtime (version 21 or greater) to be installed on your machine.

### An Alternative: PyIceberg's SQL Catalog (Non-REST)

If your primary goal for having a catalog is for the Python-based validation step, and you don't necessarily need a network-accessible REST _server_, you can use PyIceberg's built-in `SqlCatalog` directly.

- **How it Works:** This is not a REST catalog, but rather a direct implementation of the catalog logic within the PyIceberg library itself. It can be configured to use a local SQLite database file to store all table metadata.
- **Configuration:** You would configure your Python validation script to load the catalog with properties pointing to a local SQLite file :   

```Python
from pyiceberg.catalog import load_catalog

warehouse_path = "/path/for/your/iceberg/data"
catalog = load_catalog(
    "local_sql",
    **{
        "type": "sql",
        "uri": f"sqlite:///{warehouse_path}/catalog.db",
        "warehouse": f"file://{warehouse_path}",
    },
)
```

This approach is simpler if your only client is the Python script, but it doesn't provide a REST endpoint that your C# application or other tools could use.

For your tech demo, I would recommend starting with **Option 1 (the Tabular Docker image)**. It perfectly matches your request for a REST catalog with a SQLite backend, is extremely easy to start, and provides a realistic API endpoint for your C# application to interact with.

___
###### [top](#table-of-contents)
### chat-5

> i agree, tabular image seems like the best fit. Can you now help me to create an implementation plan for this project?

Of course. Based on your goal and the decision to use the Tabular Docker image for the REST catalog, here is a comprehensive implementation plan for your tech demo.

This plan is broken down into distinct phases, guiding you from initial setup to final validation, ensuring each component is built correctly to produce a fully compliant Apache Iceberg table.

### **Project Implementation Plan: C# SQL Server to Iceberg Table Generator**

**Objective:** Create a.NET console application that extracts data from a SQL Server query, formats it as an Apache Iceberg table with Parquet data files, and registers it using a local, containerized REST catalog.

### **Phase 1: Environment Setup and Configuration**

**Goal:** Prepare your development machine with all necessary tools, start the local Iceberg catalog, and structure the C# project.

1. **Install Prerequisites:**

    - **.NET SDK:** Ensure you have.NET 6 or later installed.

    - **Docker Desktop:** This is required to run the containerized REST catalog.

    - **IDE:** Visual Studio or VS Code with C# extensions.
2. **Launch the Iceberg REST Catalog:**

    - Open your terminal or command prompt.

    - Run the following Docker command to start the Tabular REST catalog container. This image conveniently uses an in-memory SQLite database, requiring no further setup.   

```Bash

docker run -p 8181:8181 tabulario/iceberg-rest:latest

```

    - **Verification:** The catalog is now running and accessible at `http://localhost:8181`. This endpoint will be the target for all metadata operations from your C# application.
3. **Create and Configure the C# Project:**

    - Create a new.NET Console Application.

    - **Add NuGet Packages:** Install the following essential libraries:

        - `Microsoft.Data.SqlClient`: For connecting to SQL Server.

        - `ParquetSharp`: For writing Parquet data files.

        - `Apache.Avro`: For creating Iceberg manifest files.

        - `System.Text.Json`: For serializing the `metadata.json` file.

    - **Configuration File:** Create an `appsettings.json` file in your project root to manage external settings. This avoids hard-coding values.

```JSON

{

  "SqlServerConnectionString": "Server=YOUR_SERVER;Database=YOUR_DB;Trusted_Connection=True;TrustServerCertificate=true;",

  "TsqlQuery": "SELECT * FROM YourTable",

  "IcebergRestCatalogUri": "http://localhost:8181/v1/",

  "IcebergWarehousePath": "file:///C:/path/to/your/warehouse",

  "IcebergTableName": "my_db.my_new_table"

}

```

### **Phase 2: High-Performance Data Extraction**

**Goal:** Implement a memory-efficient pipeline to stream data from SQL Server.

1. **Establish Database Connection:** Use the connection string from `appsettings.json` to create a `SqlConnection`.
2. **Implement Streaming Read:**

    - Use the **`SqlDataReader`** for data extraction. This is critical as it operates as a forward-only stream, retrieving one row at a time and preventing the entire dataset from being loaded into memory . This approach is vastly more scalable than using a `DataSet` or an ORM like Entity Framework for bulk data movement .

    - Ensure all database objects (`SqlConnection`, `SqlCommand`, `SqlDataReader`) are wrapped in `using` statements to guarantee proper resource disposal .
3. **Dynamic Schema Inference:**

    - Before reading rows, use the `SqlDataReader.GetSchemaTable()` method. This returns a `DataTable` containing metadata about each column in the result set (name, data type, nullability), which is essential for the next phase.

### **Phase 3: Schema Translation (T-SQL to Iceberg)**

**Goal:** Convert the SQL schema into a valid Iceberg schema, paying close attention to Iceberg's specific requirements.

1. **Define Data Type Mappings:** Create a utility function that maps `SqlDbType` values from the schema table to their corresponding Iceberg primitive types (e.g., `int` -> `int`, `bigint` -> `long`, `varchar` -> `string`, `datetime` -> `timestamp`, `decimal` -> `decimal(P,S)`) .
2. **Assign Unique Field IDs:**

    - A core requirement of Iceberg is that every field in the schema must have a unique integer `field-id`. This ID persists across schema changes, enabling safe evolution.

    - For this initial table creation, iterate through the columns and assign a simple, monotonically increasing ID starting from 1.
3. **Construct Schema Objects:**

    - Create C# classes (POCOs) that model the Iceberg schema structure as defined in the specification (e.g., `IcebergSchema`, `NestedField`).

    - Populate these objects with the column name, the mapped Iceberg type, the assigned `field-id`, and the nullability information. This in-memory schema object will be used for both Parquet and metadata file generation.

### **Phase 4: Writing Iceberg-Compliant Parquet Files**

**Goal:** Write the data stream to Parquet files that include the Iceberg-specific `field-id` metadata.

1. **Use `ParquetSharp`'s Low-Level API:**

    - The high-level API of `ParquetSharp` does not support adding the custom `field-id` metadata required by Iceberg.

    - You must use the lower-level **`GroupNode`** API to manually construct the Parquet file's schema. This gives you the fine-grained control needed to inject the `field-id` for each column, as successfully demonstrated in the inspirational blog post.
2. **Implement Streaming Write:**

    - Create a `ParquetFileWriter` using the custom `GroupNode` schema.

    - Inside the `while (reader.Read())` loop from Phase 2, write data in batches (known as "row groups" in Parquet).

    - For each batch, get a `RowGroupWriter`, then get a `ColumnWriter` for each column, and write the values from the `SqlDataReader`. This ensures data flows directly from the database to the file without being buffered in memory.

### **Phase 5: Metadata Generation and Catalog Commit**

**Goal:** Generate the hierarchical metadata files (manifests and table metadata) and perform an atomic commit via the REST catalog.

1. **Generate Avro Manifest Files:**

    - **Implement the Schema Interception Fix:** The standard `Apache.Avro` library silently strips the required `field-id` attributes when writing Avro file headers. You must implement the workaround detailed in the blog post: create `AvroSchemaWrapper` and `AvroDatumWriterWrapper<T>` classes. These wrappers will intercept the schema serialization call and provide the complete, compliant Iceberg Avro schema as a raw string, ensuring the `field-id`s are preserved.

    - **Populate Manifests:** After each Parquet file is written, record its metadata (path, file size, record count, column-level stats) as an entry in a manifest file (`.avro`).

    - **Populate Manifest List:** Once all data is written, create a single manifest list file (`snap-....avro`) that points to the manifest file(s) you just created. This file represents the table's snapshot .
2. **Create Initial Table via REST Catalog:**

    - Before writing any data, your application must first register the new table with the catalog.

    - Send an HTTP `POST` request to the catalog's `v1/namespaces/{namespace}/tables` endpoint. The request body will be a JSON object containing the table name and the Iceberg schema you constructed in Phase 3.

    - The REST catalog will create the table and return the initial `v1.metadata.json` file content.
3. **Commit the New Snapshot:**

    - After all Parquet data files and Avro metadata files are written to their final locations in the warehouse, construct the new table metadata JSON (`v2.metadata.json`). This file will contain a new snapshot pointing to your newly created manifest list.

    - Perform the final atomic commit by sending an HTTP `POST` request to the catalog's `v1/tables/{namespace}/{table}` endpoint. The body of this request will contain the path to the previous metadata file (`v1.metadata.json`) and the content of the new metadata file (`v2.metadata.json`).

    - The REST catalog server is responsible for performing the atomic swap, guaranteeing that the table state transitions cleanly from the old version to the new one.

### **Phase 6: Cross-Ecosystem Validation**

**Goal:** Prove the tech demo's success by verifying that the generated table is fully readable and compliant using a standard, non-.NET tool.

1. **Tool of Choice: `PyIceberg`**

    - `PyIceberg` is an excellent choice for validation as it is a pure Python implementation of the Iceberg specification.
2. **Validation Steps:**

    - **Install PyIceberg:**

```Bash

pip install "pyiceberg[pyarrow,rest]"

```

    - **Create a Python Validation Script:** Use the following script to connect to the local REST catalog, load the table, and inspect its contents.

```Python

from pyiceberg.catalog import load_catalog

# Configure catalog to connect to the local REST server

catalog = load_catalog(

    "local_rest",

    **{

        "type": "rest",

        "uri": "http://localhost:8181",

    }

)

try:

    # 1. Load the table (verifies catalog registration and metadata parsing)

    table_identifier = "my_db.my_new_table" # Must match your config

    table = catalog.load_table(table_identifier)

    print(f"Successfully loaded table: {table_identifier}")

    # 2. Validate the schema (verifies field-ids are present)

    print("\nTable Schema:")

    print(table.schema())

    # 3. Query the data and verify row count

    df = table.scan().to_pandas()

    print(f"\nSuccessfully read {len(df)} rows.")

    print("\nData Preview:")

    print(df.head())

except Exception as e:

    print(f"Validation failed: {e}")

```

    - **Execute and Verify:** Run the script. A successful output, showing the correct schema (with `field-id`s) and the expected data, confirms that your C# application has correctly implemented the entire Iceberg specification. You can also use DuckDB with the Iceberg extension for an alternative SQL-based validation .

___
###### [top](#table-of-contents)
### chat-6

> give me a markdown file that contains all of this conversation and research

# Conversation and Research: C# SQL Server to Apache Iceberg Demo

This document contains the full conversation and detailed research for implementing a tech demo to extract data from SQL Server and save it into a local Parquet file in an Iceberg table format.

> **User:**
> i want to implement a small tech demo which will extract data from a SQL Server db as defined via a T-SQL statement, and have  is saved into a local parquet file in an Iceberg table format. I'm  looking at this blog as the inspiration. [https://medium.com/@xorets/writing-iceberg-tables-with-c-a-journey-through-schema-compatibility-b169c846f5e0](https://medium.com/@xorets/writing-iceberg-tables-with-c-a-journey-through-schema-compatibility-b169c846f5e0). Can you help me to make a plan?> **You:**
> Here's the plan I've put together. Let me know if you need any changes before I start researching.
> http://googleusercontent.com/deep_research_confirmation_content/0> **User:**
> Start research> **You:**
> 
> 
> 
> # Architectural Guide: Implementing Apache Iceberg Table Generation in C# from SQL Server
> 
> 
> 
> ## I. Deconstructing the Apache Iceberg Table Format
> 
> 
> 
> To successfully implement a writer for Apache Iceberg, a foundational understanding of its architecture is paramount. Iceberg is not merely a file format but a comprehensive table format specification designed to bring the reliability and simplicity of relational database tables to large-scale analytic datasets stored in distributed file systems or object stores. Its architecture is deliberately designed to solve the problems of consistency, performance, and schema evolution that plague traditional data lakes. The format's power lies in its meticulously structured metadata, which enables transactional guarantees and efficient query planning.   
> 
> 
> 
> ### The Three-Tiered Architecture
> 
> 
> 
> An Iceberg table can be conceptualized as a three-tiered architecture, comprising the Catalog, the Metadata Layer, and the Data Layer.   
> 
> 1. **The Catalog:** The catalog is the entry point for interacting with an Iceberg table. Its primary responsibility is to track the location of the current metadata file for a given table name. When a write operation commits, the catalog performs an atomic swap to point from the old metadata file to the new one. This atomic operation is the cornerstone of Iceberg's ACID transaction guarantees. While production systems often use sophisticated catalogs like AWS Glue, a REST service, or a Hive Metastore, for a local tech demo, the catalog can be implemented simply as a pointer on the local filesystem.
> 2. **The Metadata Layer:** This is the core of the Iceberg format. It consists of a hierarchy of files that collectively describe the table's state at a specific point in time, known as a snapshot. This layer includes the table metadata file, manifest lists, and manifest files, which provide a multi-level index over the actual data. The structure of this layer is designed to enable fast, parallel query planning by allowing engines to prune massive portions of the table without listing files in the data directories.
> 3. **The Data Layer:** This layer consists of the actual data files, which are immutable once written. Iceberg is file-format agnostic, supporting Apache Parquet, Apache ORC, and Apache Avro. For this implementation, the data files will be in the Apache Parquet format, a highly efficient columnar storage format well-suited for analytic workloads.
> 
> 
> ### Anatomy of an Iceberg Table: The Metadata Hierarchy
> 
> 
> 
> The physical layout of an Iceberg table is a tree-like structure of metadata files that ultimately point to the data files. A correct implementation must generate each of these components in compliance with the specification.
> 
> - **Table Metadata (`metadata.json`):** This JSON file is the single source of truth for the entire table. It is the root of the metadata tree and contains all the essential information a query engine needs to begin planning a scan. Key fields include:   
> 
>     - `format-version`: An integer (1 or 2) specifying the version of the Iceberg spec the table adheres to. This is crucial for forward and backward compatibility.
> 
>     - `table-uuid`: A universally unique identifier for the table, ensuring that metadata operations are applied to the correct table instance.
> 
>     - `location`: The base URI for the table's location, where all data and metadata files are stored.
> 
>     - `schemas` and `current-schema-id`: A list of all historical schemas the table has ever had, with an ID pointing to the current one. This is fundamental to Iceberg's powerful schema evolution capabilities.
> 
>     - `partition-specs` and `default-spec-id`: A list of partition specifications, defining how data is partitioned. This allows for partition evolution over time.
> 
>     - `snapshots`: A list of all valid snapshots of the table. Each snapshot represents a complete state of the table at a point in time and points to a manifest list file.
> - **Manifest List (`snap-....avro`):** Each snapshot is defined by a single manifest list file, which is an Avro file. This file contains an entry for every manifest file that is part of that snapshot. The manifest list provides summary metadata, such as partition boundaries and counts of added or deleted files, which allows query planners to perform an initial, coarse-grained pruning. An engine can often skip reading a large number of manifest files entirely based on the summary information in the manifest list, drastically reducing the metadata overhead for a query. Key fields for each entry include    
> `manifest_path`, `partition_spec_id`, and `added_files_count`.
> - **Manifest Files (`....avro`):** Manifest files are also Avro files, and they contain the next level of detail. Each manifest file tracks a subset of the table's data files (or delete files, in v2 tables). For each data file, the manifest stores detailed statistics and metadata, including column-level metrics like lower and upper bounds for each column, null value counts, and NaN counts. This fine-grained information enables query engines to perform file-level pruning, skipping data files that cannot possibly contain data matching the query's predicates.
> - **Data Files (`....parquet`):** Finally, the manifest files point to the actual data files. These files contain the row data in a columnar format (Parquet in this case) and are considered immutable. Any operation that modifies data, such as an update or delete, does not change existing files but instead writes new data files (and potentially delete files) and creates a new snapshot reflecting the change.
> 
> 
> The hierarchical nature of this metadata is a deliberate and critical design choice. A key goal of the Iceberg specification is to enable query planning with a constant number of remote calls, rather than a number that scales with the size of the table (e.g., number of partitions or files). A query engine reads the small    
> 
> 
> 
> `metadata.json` file, then uses the manifest list to quickly identify a small subset of relevant manifest files. It then reads only those manifest files to get a list of data files that must be scanned. This multi-level indexing avoids the costly "list directory" operations that plague traditional Hive-style tables and is the source of Iceberg's performance at scale. Therefore, even for a local demonstration, it is imperative that the C# application generates this full, compliant hierarchy, including the summary statistics in the upper metadata layers. Omitting these statistics would result in a technically valid but un-optimizable table, defeating one of the primary advantages of the format.
> 
> 
> 
> ### The Role of Snapshots and Atomic Commits
> 
> 
> 
> Iceberg achieves transactional consistency (ACID) through the concepts of snapshots and atomic commits. A snapshot represents the complete state of a table at a specific moment in time. Every write operation—be it an append, overwrite, or schema change—produces a new snapshot by creating a new    
> 
> 
> 
> `metadata.json` file.   
> 
> 
> 
> The process of committing a new version of the table is an atomic operation. The writer first prepares all new data files and all new metadata files (manifests, manifest list, and the new `metadata.json`). Only when all these files are successfully written to storage does the writer perform a single, atomic "swap" operation in the catalog to point the table name to the new `metadata.json` file.
> 
> 
> 
> This mechanism of using immutable files and atomic metadata swaps provides several profound benefits:
> 
> - **Serializable Isolation:** Readers are completely isolated from concurrent writes. A reader will always see a consistent and complete snapshot of the table's data, as it will be using a specific `metadata.json` file that will not change during the query. It will never see partial or uncommitted changes.
> - **Time Travel and Reproducible Queries:** Since each `metadata.json` file points to a complete, historical state of the table, users can execute queries against any previous snapshot. This enables "time travel" to examine the state of data in the past or to re-run a query on a historical version for reproducibility.
> - **Version Rollback:** If a write operation introduces bad data, correcting the error is as simple as atomically resetting the catalog's pointer back to a previous, valid `metadata.json` file, effectively rolling back the erroneous commit.
> 
> 
> The principle of immutability is the bedrock of this reliability. The C# application must be designed as a "commit-based" writer. It should never attempt to modify any existing file in-place. Instead, it must orchestrate the creation of all new files and then perform the final, atomic commit of the new table state.
> 
> | File Type | File Format | Core Purpose | Key Fields / Content |
> |---|---|---|---|
> | Table Metadata | JSON | The table's single source of truth; the root of the metadata tree. | format-version, table-uuid, location, schemas, current-schema-id, partition-specs, snapshots |
> | Manifest List | Avro | An index of manifest files for a specific snapshot; enables manifest pruning. | manifest_path, manifest_length, partition_spec_id, added_files_count, partition boundary summaries |
> | Manifest | Avro | An index of data files; contains detailed stats for file pruning. | file_path, file_format, record_count, file_size_in_bytes, column-level stats (min/max values, null counts) |
> | Data File | Parquet | Stores the actual row data in a columnar format. | Row data corresponding to the table schema. |Export to Sheets
> 
> ## II. Phase 1: High-Performance Data Ingestion from SQL Server
> 
> 
> 
> The first active phase of the implementation involves efficiently extracting data from the source SQL Server database. The choice of data access strategy is a critical architectural decision that directly impacts the performance, scalability, and memory footprint of the application. For a data engineering pipeline tasked with moving potentially large volumes of data, a streaming-based approach is not just a best practice; it is a necessity.
> 
> 
> 
> ### Establishing a Robust and Secure Database Connection
> 
> 
> 
> The initial step is to establish a connection to the SQL Server instance. This should be done using a standard connection string. Best practices for security and maintainability dictate that this connection string should not be hard-coded. Instead, it should be managed through a configuration file (e.g., `appsettings.json`) and potentially secured using mechanisms like Azure Key Vault or Windows Certificate Store encryption, especially in a production context. For this tech demo, a configuration file is sufficient. The connection should ideally use Windows Authentication if the application runs in a trusted environment to avoid managing passwords in the connection string.   
> 
> 
> 
> ### The Streaming Imperative: Leveraging `SqlDataReader` for Memory-Efficient Data Extraction
> 
> 
> 
> When choosing a data access technology in.NET for bulk data movement, the options generally fall into two categories: object-relational mappers (ORMs) like Entity Framework, and lower-level libraries like Dapper or raw ADO.NET. While ORMs offer significant productivity gains for typical online transaction processing (OLTP) applications by abstracting away SQL and mapping results to C# objects, they are fundamentally ill-suited for this task. The primary drawback of most ORM approaches, including Entity Framework, is that they buffer the entire query result set into memory before returning control to the application. For a query that returns millions or billions of rows, this will inevitably lead to excessive memory consumption and    
> 
> 
> 
> `OutOfMemoryException` errors.
> 
> 
> 
> The optimal solution is to use the raw ADO.NET `SqlDataReader`. The `SqlDataReader` is designed specifically for high-performance, forward-only, read-only data retrieval. It operates as a connected, streaming cursor, often referred to as a "firehose cursor". This means it maintains an open connection to the database and retrieves data row-by-row as the application iterates through it using the    
> 
> 
> 
> `Read()` method. This approach has a minimal memory footprint, as only one row (or a small internal buffer) is held in the application's memory at any given time, making it capable of processing datasets that are orders of magnitude larger than the available system RAM.   
> 
> 
> 
> To ensure proper resource management and prevent connection leaks, which are common pitfalls in database programming, all ADO.NET objects (`SqlConnection`, `SqlCommand`, `SqlDataReader`) must be wrapped in `using` statements. This guarantees that the `Dispose()` method is called on each object, closing the reader and the connection even if an exception occurs during data processing.   
> 
> 
> 
> For queries that may involve columns with very large data, such as `VARBINARY(MAX)` or `NVARCHAR(MAX)`, an additional optimization should be employed. When executing the command, the `CommandBehavior.SequentialAccess` flag should be used. This changes the default behavior of the `SqlDataReader`. Instead of loading the entire contents of a row into memory when `Read()` is called, it requires the application to access the columns in the order they are selected. More importantly, for large object (LOB) columns, it allows the data to be read as a stream via methods like `GetStream()`, preventing the entire BLOB from being loaded into memory at once.   
> 
> 
> 
> The core logic of the application should therefore be a direct, synchronous pipeline that flows from the database to the file writer. The data should be read from the `SqlDataReader` and immediately written to the `ParquetFileWriter` within the same processing loop. This avoids the need for intermediate data structures like a `DataSet` or `DataTable`, which are in-memory representations of data and would negate the benefits of the streaming `DataReader`. A    
> 
> 
> 
> `DataSet` acts as a disconnected "bucket" of data, whereas the `DataReader` is a connected "hose"; for this task, the hose is the correct tool. The architecture should be a tight loop:    
> 
> 
> 
> `while (reader.Read()) {... process row and write to parquet... }`. This design minimizes memory allocation and garbage collection pressure, enabling the application to scale effectively.
> 
> 
> 
> ### Dynamically Inferring Schema from the `SqlDataReader` Result Set
> 
> 
> 
> To make the tech demo flexible and not tied to a specific query, the schema of the result set should be determined at runtime. The `SqlDataReader` provides a mechanism for this through the `GetSchemaTable()` method. This method returns a `DataTable` where each row describes a column in the result set, providing essential metadata such as the column name, data type (`SqlDbType`), size, precision, scale, and nullability. This metadata is the raw material needed for the next phase: translating the SQL schema into a compliant Iceberg schema.
> 
> 
> 
> ## III. Phase 2: Bridging the Schema Divide: From T-SQL to Iceberg
> 
> 
> 
> With a stream of data and its corresponding schema metadata available from the `SqlDataReader`, the next critical phase is to translate this information into an Apache Iceberg schema. This is not a simple one-to-one mapping of names and types; it requires a deep understanding of Iceberg's schema evolution principles, particularly the mandatory use of unique field identifiers. This phase lays the structural foundation for the entire Iceberg table.
> 
> 
> 
> ### Translating SQL Server Data Types to Iceberg and Parquet Equivalents
> 
> 
> 
> The first step is to establish a definitive mapping between the data types reported by SQL Server and the primitive types defined in the Apache Iceberg specification. A mismatch or incorrect mapping at this stage can lead to data corruption, loss of precision, or runtime failures during the write process. The    
> 
> 
> 
> `GetSchemaTable()` method provides the `SqlDbType` for each column, which must be mapped to an appropriate Iceberg type. The following table provides a comprehensive mapping for this translation.
> 
> | SQL Server Type (SqlDbType) | .NET Type (Type) | Iceberg Primitive Type | Parquet Physical Type | Implementation Notes |
> |---|---|---|---|---|
> | BigInt | long | long | INT64 | Direct mapping. |
> | Int | int | int | INT32 | Direct mapping. |
> | SmallInt | short | int | INT32 | SQL smallint fits within Iceberg int. |
> | TinyInt | byte | int | INT32 | SQL tinyint fits within Iceberg int. |
> | Bit | bool | boolean | BOOLEAN | Direct mapping. |
> | Float | double | double | DOUBLE | SQL float is a double-precision float. |
> | Real | float | float | FLOAT | SQL real is a single-precision float. |
> | Decimal, Numeric, Money, SmallMoney | decimal | decimal(P, S) | FIXED_LEN_BYTE_ARRAY | Precision (P) and scale (S) must be preserved from the SQL schema and included in the Iceberg type definition. |
> | Date | DateTime | date | INT32 (Logical: Date) | Represents days since the Unix epoch. |
> | DateTime, SmallDateTime, DateTime2 | DateTime | timestamp | INT64 (Logical: Timestamp) | Represents microseconds from the Unix epoch, without timezone. |
> | DateTimeOffset | DateTimeOffset | timestamptz | INT64 (Logical: Timestamp) | Represents microseconds from the Unix epoch, with timezone (stored as UTC). |
> | Char, NChar, VarChar, NVarChar, Text, NText | string | string | BYTE_ARRAY (UTF8) | All string types map to Iceberg's string type. |
> | Binary, VarBinary, Image | byte | binary | BYTE_ARRAY | For variable-length binary data. |
> | UniqueIdentifier | Guid | uuid | FIXED_LEN_BYTE_ARRAY (16) | Maps to Iceberg's dedicated uuid type. |Export to Sheets
> 
> ### The Criticality of `field-id`
> 
> 
> 
> A defining feature of the Iceberg schema is the mandatory `field-id` for every column. This is a unique integer identifier assigned to each field within the table's schema. Its purpose is to provide a stable, persistent identity for a column that survives across schema changes. This is the mechanism that enables safe and unambiguous schema evolution, a core goal of the Iceberg project.   
> 
> 
> 
> For example, consider renaming a column from `user_email` to `email_address`. In a traditional format, this might be interpreted as dropping the `user_email` column and adding a new `email_address` column. When querying old data files, the `email_address` column would appear as null because it didn't exist when those files were written. In Iceberg, a rename operation only changes the `name` property in the metadata; the `field-id` remains the same. This allows query engines to correctly map the data from old files, which was written under the `field-id` for `user_email`, to the newly named `email_address` column, preserving the data.
> 
> 
> 
> The inability to set this `field-id` is the primary technical challenge highlighted in the inspirational blog post and the reason for abandoning the high-level APIs of the `ParquetSharp` library. The implementation must therefore treat the generation and management of these IDs as a first-class concern.   
> 
> 
> 
> ### Strategy for Assigning `field-id`s
> 
> 
> 
> For the initial creation of a new Iceberg table, the strategy for assigning `field-id`s is straightforward. The application should iterate through the columns as they are discovered from the `SqlDataReader`'s schema and assign a monotonically increasing integer, starting from 1.
> 
> 1. Column 1 -> `field-id: 1`
> 2. Column 2 -> `field-id: 2`
> 3. Column 3 -> `field-id: 3`
> 
> 4....and so on.
> 
> 
> It is crucial to note that for any subsequent operations on this table (e.g., appending data, evolving the schema), these assigned IDs must be preserved. The application would need to first read the existing table's metadata to learn the existing `field-id`s and the `last-column-id` before performing any modifications. For this tech demo, which focuses only on the initial creation, the simple monotonic assignment is sufficient.
> 
> 
> 
> ### Constructing the Iceberg Schema Object
> 
> 
> 
> Before any files can be written, the complete Iceberg schema must be constructed in memory. This involves creating a set of C# Plain Old CLR Objects (POCOs) that can be serialized to the JSON format required in the `metadata.json` file. The structure should mirror the specification: a top-level schema object containing a list of `NestedField` objects.
> 
> 
> 
> A representative C# class structure might look like this:
> 
> 
> 
> csharp
> public class IcebergSchema
> {
> [JsonPropertyName("type")]
> public string Type { get; set; } = "struct";
> 
> 
> ```
> [JsonPropertyName("schema-id")]
> public int SchemaId { get; set; }
> ```> [JsonPropertyName("fields")]
> public List Fields { get; set; }
> 
> 
> ```
> }
> ```> public class NestedField
> {
> [JsonPropertyName("id")]
> public int Id { get; set; } // The critical field-id> ```
> [JsonPropertyName("name")]
> public string Name { get; set; }
> ```> [JsonPropertyName("required")]
> public bool Required { get; set; }> [JsonPropertyName("type")]
> public object Type { get; set; } // Can be a string for primitives or a complex object for decimals
> 
> 
> ```
> }
> ```> ```
> The application logic will iterate over the columns from `SqlDataReader.GetSchemaTable()`, and for each column, it will create a `NestedField` instance. It will populate the `Id` with the newly assigned monotonic integer, the `Name` from the column name, the `Required` property based on the column's nullability, and the `Type` based on the data type mapping table. This in-memory `IcebergSchema` object will then be used in both the Parquet file generation and the final `metadata.json` creation.
> ```> ## IV. Phase 3: Crafting Compliant Parquet Data Files in C#> With the data stream ready and the Iceberg schema defined, the next phase is to write the actual data files. The user's requirement is to use the Apache Parquet format. This phase addresses the technical nuances of using the `ParquetSharp` library to produce files that are not only valid Parquet files but are also fully compliant with the specific metadata requirements of the Iceberg specification.> ### Introduction to `ParquetSharp`> `ParquetSharp` is a.NET library that provides high-performance read and write access to Apache Parquet files. It serves as a C# wrapper around the official native `apache-arrow` C++ implementation of Parquet, which ensures excellent performance and adherence to the Parquet format standard. The library offers both high-level APIs for convenience and low-level APIs for fine-grained control.> ### The `field-id` Challenge and the Low-Level `GroupNode` Solution> As established, the central challenge is embedding the Iceberg-mandated `field-id` into the Parquet file's schema metadata. The inspirational blog post details a failed attempt to use `ParquetSharp`'s high-level `Column` API for this purpose, as it provides no mechanism to attach this specific piece of metadata. This demonstrates a common pattern in software development: a specification from one ecosystem (the JVM-centric world of big data where Iceberg originated) often has requirements that have not yet been incorporated as first-class features into the convenient, high-level abstractions of libraries in other ecosystems (like.NET).> To overcome this, the implementation must bypass the high-level abstractions and use the lower-level `GroupNode` API, which provides the necessary control to build the schema definition manually and inject the required metadata. This approach, while more verbose, is the key to achieving compliance.> The process for constructing the compliant Parquet schema is as follows:> 1. **Initialize a list of `Node` objects.** This list will hold the schema definition for each column.
> 2. **Iterate through the `NestedField` objects** of the in-memory `IcebergSchema` created in the previous phase.
> 3. **For each field, create a `LogicalType` annotation.** The `ParquetSharp` library provides a `LogicalType` class. The specific annotation required by Iceberg is not a standard Parquet logical type but a custom one that can be represented using the library's extensibility. The crucial part is to correctly associate the `field-id`. While Parquet itself doesn't have a native `field-id` concept in its core specification, Iceberg readers look for this information in specific locations within the file metadata. The `ParquetSharp` representation allows for this via its schema construction. The blog post's successful strategy involved creating a new `PrimitiveNode` that explicitly included the `fieldId` from the source column's definition.
> 4. **Create the `PrimitiveNode` for the column.** Based on the Iceberg-to-Parquet type mapping (e.g., `Iceberg int` -> `Parquet INT32`), create the corresponding `PrimitiveNode` (e.g., `new PrimitiveNode(typeof(int), Repetition.Optional)`). When constructing this node, the `fieldId` must be passed. The `ParquetSharp` API allows for this by manually building the schema tree. A `PrimitiveNode` can be created with a name, a repetition level, and crucially, a `field_id` parameter in some of its constructors or builder methods. The exact mechanism involves creating a `ColumnDescriptor` that includes the field ID.
> 5. **Construct the Root `GroupNode`.** Once the list of `PrimitiveNode` objects is complete, they are used to construct a single root `GroupNode`, which represents the overall schema of the Parquet file.
> 6. **Instantiate the `ParquetFileWriter`.** This `GroupNode` schema object is then passed directly to the constructor of the `ParquetFileWriter`. This ensures that the file is created with a schema that contains the necessary `field-id` information for every column, making it readable by Iceberg-compliant engines.> ### Streaming Data from `SqlDataReader` into the Writer> This low-level schema construction must be integrated seamlessly with the high-performance data streaming pipeline established in Phase 1. The overall workflow within the application's main processing loop will be:> 1. Outside the loop, create the `ParquetFileWriter` with the custom-built `GroupNode` schema and a `FileStream` pointing to the output Parquet file path.
> 2. Inside the `while (reader.Read())` loop, process the data in batches (row groups). A row group is a horizontal partitioning of the data within a Parquet file.
> 3. For each batch of rows (e.g., 100,000 rows, a configurable size), create a new `RowGroupWriter` from the `ParquetFileWriter`.
> 4. Within the batch, iterate through each row from the `SqlDataReader`.
> 5. For each row, iterate through its columns.
> 6. Get the `ColumnWriter` for the current column from the `RowGroupWriter`.
> 7. Retrieve the value from the `SqlDataReader` for that column (e.g., `reader.GetInt32(i)`).
> 8. Write the value to the `ColumnWriter` using its `WriteBatch` method.
> 9. After processing all rows in the batch, dispose of the `RowGroupWriter`, which flushes the data for that row group to the file.
> 10. After the main `while` loop finishes, dispose of the `ParquetFileWriter`, which writes the file footer containing the schema and other metadata, finalizing the file.> This approach ensures that the entire process remains a memory-efficient stream. Data flows from the database connection, through the application with minimal buffering, and directly into the Parquet file on disk.> ## V. Phase 4: The Core of Iceberg: Programmatic Metadata Generation> This phase addresses the most intricate and challenging aspect of the implementation: the programmatic generation of the Iceberg metadata files, specifically the Avro-based manifest and manifest list. Success in this phase hinges on overcoming a subtle but critical limitation in the standard.NET Avro library, a problem that was painstakingly diagnosed and solved in the user's inspirational material.> ### The `Apache.Avro` Library Challenge> The official `Apache.Avro` library for.NET is the natural choice for working with Avro files. The challenge, as discovered in the source blog, is not in its ability to handle basic Avro data, but in its handling of the specific, extended Avro schemas required by the Iceberg specification. Iceberg's Avro schemas for manifest files include custom attributes like `field-id`, `element-id`, `key-id`, and `value-id`, which are essential for schema evolution and compatibility.> The problem is that while the `Apache.Avro` library can correctly _parse_ a JSON schema string containing these attributes, its default serialization logic silently _strips_ them when it writes the schema into the header of the Avro Object Container File (OCF). The library serializes only the elements it recognizes from the base Avro specification, discarding the Iceberg-specific additions. This results in a structurally correct but semantically non-compliant Avro file that will be rejected by any standard Iceberg reader, as the required field ID mappings will be missing. This silent failure mode makes the issue particularly difficult to debug without attempting to read the generated file with an external, compliant tool.> ### Implementing the "Strategic Schema Interception" Pattern> The solution to this problem is a clever and surgical workaround that intercepts the library's schema serialization process at the precise moment of failure and injects the correct, fully compliant schema. This "Strategic Schema Interception" pattern, detailed in the blog post, avoids the need to fork the library or write a custom Avro serializer from scratch. It is a powerful technique for dealing with library-specification mismatches.> The implementation requires two custom wrapper classes:> 1. **`AvroSchemaWrapper`:** This class is a simple wrapper around an `Apache.Avro.Schema` instance. Its primary function is to store the original, untouched JSON schema string that was used to create the schema object. The critical part of this class is its override of the `ToString()` method. Instead of allowing the base class to regenerate a (stripped) JSON string from the parsed schema object, this override simply returns the original, pristine JSON string, complete with all Iceberg-specific attributes.> 1. **`AvroDatumWriterWrapper<T>`:** This class wraps a standard `DatumWriter<T>` (e.g., `GenericDatumWriter<GenericRecord>`). It delegates all actual data writing calls to the inner writer instance. Its key feature is a public `Schema` property that, instead of returning the inner writer's schema directly, returns a new instance of the `AvroSchemaWrapper` created above.> The workflow for using these wrappers is as follows:
> 
> - First, the compliant Avro schemas for the manifest file and the manifest list file are defined as static JSON strings within the C# application. These schemas should be taken directly from the official Apache Iceberg specification to ensure correctness.
> - When it's time to write a manifest file, a `GenericDatumWriter<GenericRecord>` is created with the parsed schema.
> - This standard writer is then passed into the constructor of our custom `AvroDatumWriterWrapper<T>`.
> - Finally, this wrapped writer is passed to the `DataFileWriter<GenericRecord>.OpenWriter()` method.> When the `DataFileWriter` prepares to write the file header, it needs to serialize the schema. To do this, it accesses the `Schema` property of the writer it was given and calls `ToString()` on the result. This is where the interception occurs: it calls our `AvroDatumWriterWrapper.Schema` property, which returns our `AvroSchemaWrapper`, and then calls `ToString()` on that object. Our overridden method then returns the original, complete JSON schema string, which is written correctly into the Avro file header. This targeted intervention fixes the library's limitation with minimal and non-invasive code.> ### Populating the Metadata Files> With the writing mechanism in place, the application must populate these files with the correct data.> - **Manifest File (`....avro`):** After each Parquet data file is successfully written and closed, the application will have gathered its final metadata: its full path, its length in bytes, and the total number of records it contains. An Avro `GenericRecord` is created based on the manifest entry schema. This record is populated with the data file's statistics. This `GenericRecord` is then written as a single entry to the manifest Avro file using the wrapped writer. For this simple append-only demo, the manifest will contain one entry for each Parquet file generated.> - **Manifest List File (`snap-....avro`):** Once all data files have been written and the manifest file has been finalized and closed, the application gathers the metadata of the manifest file itself (its path and length). It then creates a `GenericRecord` based on the manifest list entry schema and populates it with the manifest file's information, along with summary statistics (e.g., `added_files_count` will be the total number of Parquet files written). This record is then written to the manifest list Avro file.> ### Constructing the Final `metadata.json` File> The final step in metadata generation is creating the top-level `metadata.json` file. This is a straightforward JSON serialization task.> 1. **Create C# POCOs:** Define a set of C# classes that mirror the structure of the `metadata.json` file as defined in the Iceberg specification. This includes classes for    
> `TableMetadata`, `Snapshot`, `Schema`, `NestedField`, etc.
> 2. **Populate the Model:** Instantiate the root `TableMetadata` object and populate its properties:
> 
>     - Generate a new `table-uuid`.
> 
>     - Set the `location` to the root directory of the table.
> 
>     - Set the `format-version` to 2 (as v1 is largely legacy).
> 
>     - Assign the `IcebergSchema` object created in Phase 2 to the `schemas` list and set the `current-schema-id`.
> 
>     - Create an initial, empty `partition-specs` list.
> 
>     - Create a new `Snapshot` object. This object will be given a unique `snapshot-id` (a randomly generated long integer), the current timestamp, and, most importantly, the relative path to the manifest list file (`snap-....avro`) that was just created.
> 
>     - Add this `Snapshot` object to the `snapshots` list in the metadata and set the `current-snapshot-id` to its ID.
> 3. **Serialize to JSON:** Use a standard.NET JSON serialization library (like `System.Text.Json`) to serialize the populated `TableMetadata` object into a formatted JSON string. This string will be saved as the first version of the metadata file (e.g., `v1.metadata.json`).> ## VI. Phase 5: The Atomic Commit: Finalizing the Table State> After generating all the necessary data and metadata files, the final phase is to commit the new table state to the filesystem in a transactionally safe manner. This step is what elevates the collection of files into a true, queryable Iceberg table with ACID guarantees. The atomicity of this final action is critical to prevent race conditions and ensure that readers never observe a partially written or inconsistent state.> ### Filesystem Catalog Logic> In a production environment with a dedicated Iceberg catalog service (like AWS Glue or a REST catalog), the final commit step involves making an API call to the catalog to atomically update the pointer from the old metadata location to the new one. For this tech demo, which uses the local filesystem as both storage and catalog, a simpler but equally effective mechanism is used.> The state of a filesystem-based Iceberg table is typically determined by one of two methods:
> 
> 1. **Listing Metadata Files:** A reader can list all `v*.metadata.json` files in the metadata directory and parse the version number from the filename to identify the latest one.
> 2. **Version Hint File:** A more robust and efficient method is to use a `version-hint.txt` file in the metadata directory. This simple text file contains a single line: the version number of the current `metadata.json` file (e.g., "1", "2", etc.). A reader first reads this hint file to know which metadata file to load (e.g., `v2.metadata.json`).> The commit operation must atomically update this hint.> ### The Commit Process> The process must ensure that a concurrent reader cannot access the new metadata file until all associated data and metadata files are fully and correctly written to their final destinations.> 1. **Write Files to Final Locations:** During the previous phases, all files—the Parquet data files in the `/data` subdirectory and the Avro manifest, Avro manifest list, and the new versioned JSON metadata file (e.g., `v1.metadata.json`) in the `/metadata` subdirectory—should be written directly to their final paths.
> 2. **Prepare the Hint File:** Create the content of the new version hint file. For the first commit, this will simply be the string "1".
> 3. **Atomic Update:** The final, critical step is to atomically update the `version-hint.txt` file. This should **not** be done by opening and writing to the file directly, as that is not an atomic operation. A concurrent reader could potentially read the file during the write, resulting in a corrupted or empty version hint. The correct procedure is:
> a.  Write the new version number to a temporary file (e.g., `version-hint.txt.tmp`) in the same metadata directory.
> b.  Use an atomic rename or move operation to replace the old `version-hint.txt` with the new temporary file. On most modern filesystems, a `File.Move` operation within the same volume is an atomic metadata-only operation.> This sequence guarantees that any reader will either see the old, consistent state of the table (if it reads before the atomic rename) or the new, fully consistent state of the table (if it reads after). There is no intermediate state where a reader could load the new metadata file before all of its dependent files are present and complete. This simple filesystem operation faithfully implements the atomic commit principle at the heart of the Iceberg specification.> ## VII. Phase 6: Cross-Ecosystem Validation Strategy> The ultimate measure of success for this tech demo is not whether the C# application can write and then read its own files, but whether the generated Iceberg table is a valid, interoperable artifact that can be consumed by the broader big data ecosystem. The subtle compatibility issues, such as the one encountered with the `Apache.Avro` library, often only manifest when a different, canonical implementation attempts to read the data. Therefore, a rigorous validation phase using an external, non-.NET tool is not an optional add-on but a mandatory final step in the development process.> ### The Need for External Validation> Internal validation is insufficient because a library might have non-standard behaviors or silent bugs that produce files that are only compatible with itself. The value proposition of an open table format like Iceberg is precisely that it enables multiple, diverse engines like Spark, Trino, Flink, and DuckDB to work with the same data safely and concurrently. Proving that the C#-generated table integrates into this ecosystem is the primary goal. The inspirational blog post itself used    
> 
> 
> 
> `PyIceberg` as the external validator that ultimately uncovered the critical schema serialization bug.> For this validation plan, `PyIceberg` is the recommended tool due to its straightforward setup, Pythonic API, and direct implementation of the Iceberg specification.> ### Validation with PyIceberg> The following steps provide a concrete plan for verifying the integrity of the generated table.> 1. **Environment Setup:**
> 
>     - Install a recent version of Python.
> 
>     - Install the necessary Python packages using pip. `PyIceberg` requires an I/O manager to interact with the filesystem; `pyarrow` is a common choice. The command will be: `pip install "pyiceberg[pyarrow]"`.> 1. **Loading the Table:**
> 
>     - A Python script will be written to load the table. It will first configure and load a local filesystem catalog pointing to the parent directory of the generated table warehouse. Then, it will use `catalog.load_table()` to open the specific table.
> 
>     - **Validation Check:** A successful execution of `load_table()` without exceptions is the first sign of success. It indicates that `PyIceberg` was able to find the `version-hint.txt`, load the corresponding `metadata.json`, and parse it correctly.> ```
> ```python
> from pyiceberg.catalog import load_catalog
> ```> ```
> # Configure a catalog to read from the local filesystem
> # 'warehouse' should be the parent directory where the table folder is located
> catalog = load_catalog(
>     "local",
>     **{
>         "type": "rest", # Using REST as a placeholder for local file access logic
>         "uri": "http://localhost:8181", # Not used for local, but required
>         "warehouse": "file:///path/to/your/warehouse"
>     }
> )
> ```> ```
> # Load the table
> try:
>     table = catalog.load_table("your_db.your_table_name")
>     print("Successfully loaded the Iceberg table.")
> except Exception as e:
>     print(f"Failed to load table: {e}")
>     exit()
> ```
> ```> 1. **Schema Validation:**
> 
>     - After successfully loading the table, the script will inspect and print the schema.
> 
>     - **Validation Check:** The output of `print(table.schema())` should be visually inspected. It must show the correct column names, data types, and nullability. Most importantly, it must display the unique, integer `field-id` for each column. If the `field-id`s are missing or incorrect, it points to a failure in the Parquet schema generation (Phase 3) or the Avro manifest serialization (Phase 4).> 1. **Data Querying and Verification:**
> 
>     - The final and most comprehensive check is to read the actual data. The `table.scan()` method creates a scanner object, and `.to_pandas()` will execute the scan and load the entire table's data into a Pandas DataFrame.
> 
>     - **Validation Check:** The script should print the first few rows of the DataFrame (`df.head()`) and the total number of rows (`len(df)`). This data should be compared against the source data in SQL Server to confirm that all rows were written correctly and that the values are intact without corruption or type-casting errors.> ```
> ```python
> # Scan the table and load data into a Pandas DataFrame
> df = table.scan().to_pandas()
> ```> ```
> print("\nTable Schema:")
> print(table.schema())
> ```> ```
> print("\nData Preview (first 5 rows):")
> print(df.head())
> ```> ```
> print(f"\nTotal rows read: {len(df)}")
> # Compare this count with the source row count from SQL Server
> ```
> ```> ### Alternative Validation Tools> For even greater confidence, especially as the project evolves, other standard tools can be used:> - **Apache Spark:** Configure a local Spark session with the Iceberg extension jars. Then, register the filesystem-based catalog and run SQL queries like `SELECT * FROM local.your_db.your_table_name LIMIT 10;`. A successful query in Spark provides a very high degree of confidence in the table's correctness.
> - **DuckDB:** DuckDB, an in-process analytical database, has a powerful Iceberg extension. It can be configured to read from a local Iceberg table, allowing for fast, interactive SQL validation.> Successfully passing these external validation checks confirms that the tech demo has not just created a set of files but has produced a genuinely interoperable data asset that adheres to the Apache Iceberg open standard. This is the definitive "definition of done" for the project.> ## VIII. Strategic Recommendations and Architectural Blueprint> This final section synthesizes the preceding phases into a cohesive architectural blueprint for the tech demo. It provides recommendations on code structure, best practices, and clear pathways for extending the proof-of-concept into a more feature-rich and robust data engineering tool.> ### A Cohesive Code Structure> To ensure maintainability and clarity, the C# project should be organized logically, separating concerns into distinct namespaces or even separate class library projects. A recommended structure would be:> - **`DemoApp/` (Console Application Project):**
> 
>     - `Program.cs`: The main entry point. Responsible for parsing configuration, orchestrating the pipeline, and handling top-level exceptions.
> 
>     - `appsettings.json`: Configuration file for the SQL connection string, T-SQL query, and output table path.> - **`Core/` (Class Library Project):**
> 
>     - `Models/`: Contains the C# POCOs that map to the Iceberg metadata structures (`TableMetadata`, `Snapshot`, `IcebergSchema`, `NestedField`, `ManifestEntry`, etc.).
> 
>     - `Constants/`: Static class for holding constant values, such as the Avro schema JSON strings for manifest and manifest list files.> - **`IO/` (Class Library Project):**
> 
>     - `SqlServerExtractor.cs`: Encapsulates all logic for connecting to SQL Server and streaming data via `SqlDataReader`.
> 
>     - `ParquetWriter.cs`: Contains the logic for creating the `ParquetSharp` `GroupNode` schema with `field-id`s and writing the data stream to a Parquet file.
> 
>     - `IcebergMetadataWriter.cs`: The most complex component, containing the `AvroSchemaWrapper` and `AvroDatumWriterWrapper` classes, and the logic for writing the manifest and manifest list Avro files.
> 
>     - `TableCommitter.cs`: Handles the final atomic commit logic, such as writing the `metadata.json` and updating the `version-hint.txt` file.> This separation allows each component to focus on a single responsibility, making the code easier to understand, test, and extend.> ### Best Practices for Error Handling, Configuration, and Resource Management> - **Configuration:** As mentioned, all external parameters (connection string, query, output path, batch sizes) should be externalized into a configuration file. This allows the tool to be re-used without recompilation.
> - **Error Handling:** The entire data pipeline should be wrapped in a `try...catch...finally` block. The `catch` block should log detailed error information. The `finally` block is critical for ensuring that all I/O resources (database connections, file streams) are closed and disposed of properly, even if an error occurs mid-process. The application should also perform cleanup of any partially written files in the event of a failure to avoid leaving the table directory in a corrupted state.
> - **Resource Management:** Consistently use `using` statements for all `IDisposable` objects, including `SqlConnection`, `SqlCommand`, `SqlDataReader`, `FileStream`, and all `ParquetSharp` and `Apache.Avro` writer objects. This is the idiomatic C# way to ensure deterministic cleanup of unmanaged resources.
> - **Logging:** Implement structured logging (e.g., using Serilog or NLog) to provide visibility into the pipeline's execution. Log key events such as successful connection, schema inference, start/end of file writing, number of rows processed, and the final commit.> ### Pathways for Extending the Demo> The architecture described in this report provides a solid foundation. Once the core functionality is proven, it can be extended in several important ways to create a more powerful tool:> - **Partitioning:** This is the most logical next step for performance optimization.
> 
>     - **Implementation:** This would involve adding a `partition-specs` definition to the `metadata.json`. The data writing logic would need to be modified to route rows to different Parquet files based on their partition values (e.g., writing data for each day into a separate file in a `date=YYYY-MM-DD/` subdirectory). The manifest files would then track which partitions each data file belongs to.
> - **Schema Evolution:** The use of `field-id`s already enables this.
> 
>     - **Implementation:** An "update" operation would first need to read the latest `metadata.json` to understand the current state. To add a column, it would append a new schema to the `schemas` list with a new `last-column-id`, create a new snapshot, and commit a new `metadata.json`. The core writing logic would remain largely the same.
> - **Cloud Storage Integration:** The file I/O logic can be abstracted behind an interface.
> 
>     - **Implementation:** Create an `IFileSystem` interface with methods like `OpenWrite`, `OpenRead`, and `AtomicMove`. Implement a `LocalFileSystem` class for the current functionality and then create `S3FileSystem` or `AzureBlobFileSystem` classes that use the respective cloud provider SDKs. This would allow the tool to write Iceberg tables directly to cloud data lakes.
> - **Support for Updates and Deletes (Merge-on-Read):** This is an advanced feature requiring support for Iceberg v2 tables.
> 
>     - **Implementation:** This would involve writing "delete files" in addition to data files. Delete files record which rows in existing data files should be considered deleted. This is a significantly more complex operation but is the foundation for supporting row-level mutations in a data lakehouse architecture.> By following this comprehensive plan, the developer can successfully build a tech demo that not only meets the immediate requirement but also serves as a robust starting point for building production-grade data engineering pipelines in the.NET ecosystem.> **User:**
> in this demo - where would the iceberg metadata be stored? we want to minimize external dependencies where-ever possible> **You:**
> Excellent question. To align with your goal of minimizing external dependencies, the Iceberg metadata for this demo will be stored directly on the **local filesystem** alongside the Parquet data files.
> 
> 
> 
> This approach uses what is known as a "filesystem catalog," which is the simplest way to manage an Iceberg table and requires no external services, databases, or network daemons.
> 
> 
> 
> Here’s a breakdown of how it will be structured:
> 
> 1. **Root Table Directory:** A single main directory will be created for the table (e.g., `/path/to/my_iceberg_table/`). This directory will contain everything needed for the table to be a self-contained, portable unit.
> 2. **Subdirectory Structure:** Inside this root directory, the generated files will be organized into two standard subdirectories:
> 
>     - `data/`: This will contain all the actual data files, which in our case will be the `.parquet` files.
> 
>     - `metadata/`: This is where all the Iceberg-specific metadata files will live.
> 3. **Metadata Files:** The `metadata/` directory will contain the full hierarchy of files that define the table's state :   
> 
>     - **Table Metadata (`.metadata.json`):** A versioned JSON file (e.g., `v1.metadata.json`, `v2.metadata.json`) that acts as the single source of truth for the table's schema, partition information, and a list of all historical snapshots.
> 
>     - **Manifest List (`.avro`):** An Avro file for each snapshot that points to the specific manifest files making up that version of the table.
> 
>     - **Manifest Files (`.avro`):** Avro files that list the actual data files (`.parquet`) and contain detailed statistics about them, which is crucial for efficient query planning.
> 4. **The "Catalog" Pointer:** To identify the current version of the table without an external service, a simple text file named `version-hint.txt` will be placed in the `metadata/` directory. This file contains a single number indicating which metadata file is the current one (e.g., "1" to point to `v1.metadata.json`). This is a standard, lightweight mechanism for filesystem-based Iceberg tables.
> 
> 
> By storing all metadata as files on the local disk, the entire tech demo becomes completely self-contained, avoiding any dependency on services like a Hive Metastore, AWS Glue, or a REST catalog service.> **User:**
> are there any REST catalogues that would work on a developer machine, perhaps with sqlite backing?> **You:**
> Yes, absolutely. Moving from a simple filesystem catalog to a local REST catalog is an excellent step for a tech demo. It more closely mimics a production environment and provides a standardized API for table management, all while running self-contained on your developer machine.
> 
> 
> 
> There are several great options for running a lightweight Iceberg REST catalog locally, and many of them can indeed use SQLite as a backend to maintain that minimal-dependency footprint.
> 
> 
> 
> Here are a few of the best approaches:
> 
> 
> 
> ### 1. The Tabular Docker Image (Recommended)
> 
> 
> 
> This is often the quickest and most common way to get a local REST catalog running. The team at Tabular provides a reference implementation of the Iceberg REST catalog in a Docker image.
> 
> - **How it Works:** The image runs a server that exposes the standard Iceberg REST API endpoints. By default, it is configured to use a JDBC (Java Database Connectivity) catalog that is backed by an in-memory SQLite database, which is perfect for local testing and development.
> - **To run it, you simply need Docker installed and can execute the following command:**
> 
> ```Bash
> docker run -p 8181:8181 tabulario/iceberg-rest:latest
> ```
> This will start the REST catalog server and make it available on `http://localhost:8181` on your machine. Your C# application can then be configured to point to this URI to manage its tables.
> 
> 
> ### 2. Other Community REST Catalog Implementations
> 
> 
> 
> The Iceberg community has produced other REST catalog projects that are also suitable for local development with SQLite. For example, there are Python-based implementations that you can build and run via Docker.
> 
> - **How it Works:** These projects provide a Dockerfile that builds a lightweight web server implementing the REST catalog specification. The base versions of these images are often designed to use SQLite as the metadata backend.
> - **Example Usage:** You would typically clone the project's repository, build the Docker image, and then run the container, exposing the necessary port (e.g., `docker run -p 8000:8000 my-iceberg-rest-image`).
> 
> 
> ### 3. Standalone Toolsets like Altinity ice
> 
> 
> 
> If you prefer not to use Docker, some projects provide standalone command-line tools to run a catalog.
> 
> - **How it Works:** Altinity's `ice` is an open-source toolset that includes a command, `ice-rest-catalog`, which starts a local REST catalog. This implementation is a wrapper around the official Iceberg Java libraries, which are the reference implementation for the specification. This option requires a Java runtime (version 21 or greater) to be installed on your machine.
> 
> 
> ### An Alternative: PyIceberg's SQL Catalog (Non-REST)
> 
> 
> 
> If your primary goal for having a catalog is for the Python-based validation step, and you don't necessarily need a network-accessible REST _server_, you can use PyIceberg's built-in `SqlCatalog` directly.
> 
> - **How it Works:** This is not a REST catalog, but rather a direct implementation of the catalog logic within the PyIceberg library itself. It can be configured to use a local SQLite database file to store all table metadata.
> - **Configuration:** You would configure your Python validation script to load the catalog with properties pointing to a local SQLite file :   
> 
> ```Python
> from pyiceberg.catalog import load_catalog
> ```> ```
> warehouse_path = "/path/for/your/iceberg/data"
> catalog = load_catalog(
>     "local_sql",
>     **{
>         "type": "sql",
>         "uri": f"sqlite:///{warehouse_path}/catalog.db",
>         "warehouse": f"file://{warehouse_path}",
>     },
> )
> ```
> ```
> 
> 
> This approach is simpler if your only client is the Python script, but it doesn't provide a REST endpoint that your C# application or other tools could use.
> 
> 
> 
> For your tech demo, I would recommend starting with **Option 1 (the Tabular Docker image)**. It perfectly matches your request for a REST catalog with a SQLite backend, is extremely easy to start, and provides a realistic API endpoint for your C# application to interact with.> **User:**
> i agree, tabular image seems like the best fit. Can you now help me to create an implementation plan for this project?> **You:**
> Of course. Based on your goal and the decision to use the Tabular Docker image for the REST catalog, here is a comprehensive implementation plan for your tech demo.
> 
> 
> 
> This plan is broken down into distinct phases, guiding you from initial setup to final validation, ensuring each component is built correctly to produce a fully compliant Apache Iceberg table.
> 
> 
> 
> ### **Project Implementation Plan: C# SQL Server to Iceberg Table Generator**
> 
> 
> 
> **Objective:** Create a.NET console application that extracts data from a SQL Server query, formats it as an Apache Iceberg table with Parquet data files, and registers it using a local, containerized REST catalog.
> 
> 
> 
> ### **Phase 1: Environment Setup and Configuration**
> 
> 
> 
> **Goal:** Prepare your development machine with all necessary tools, start the local Iceberg catalog, and structure the C# project.
> 
> 1. **Install Prerequisites:**
> 
>     - **.NET SDK:** Ensure you have.NET 6 or later installed.
> 
>     - **Docker Desktop:** This is required to run the containerized REST catalog.
> 
>     - **IDE:** Visual Studio or VS Code with C# extensions.
> 2. **Launch the Iceberg REST Catalog:**
> 
>     - Open your terminal or command prompt.
> 
>     - Run the following Docker command to start the Tabular REST catalog container. This image conveniently uses an in-memory SQLite database, requiring no further setup.   
> 
> 
> 
> ```Bash
> 
> docker run -p 8181:8181 tabulario/iceberg-rest:latest
> 
> ```
> 
>     - **Verification:** The catalog is now running and accessible at `http://localhost:8181`. This endpoint will be the target for all metadata operations from your C# application.
> 3. **Create and Configure the C# Project:**
> 
>     - Create a new.NET Console Application.
> 
>     - **Add NuGet Packages:** Install the following essential libraries:
> 
> 
> 
>         - `Microsoft.Data.SqlClient`: For connecting to SQL Server.
> 
> 
> 
>         - `ParquetSharp`: For writing Parquet data files.
> 
> 
> 
>         - `Apache.Avro`: For creating Iceberg manifest files.
> 
> 
> 
>         - `System.Text.Json`: For serializing the `metadata.json` file.
> 
>     - **Configuration File:** Create an `appsettings.json` file in your project root to manage external settings. This avoids hard-coding values.
> 
> 
> 
> ```JSON
> 
> {
> 
>   "SqlServerConnectionString": "Server=YOUR_SERVER;Database=YOUR_DB;Trusted_Connection=True;TrustServerCertificate=true;",
> 
>   "TsqlQuery": "SELECT * FROM YourTable",
> 
>   "IcebergRestCatalogUri": "http://localhost:8181/v1/",
> 
>   "IcebergWarehousePath": "file:///C:/path/to/your/warehouse",
> 
>   "IcebergTableName": "my_db.my_new_table"
> 
> }
> 
> ```
> 
> 
> ### **Phase 2: High-Performance Data Extraction**
> 
> 
> 
> **Goal:** Implement a memory-efficient pipeline to stream data from SQL Server.
> 
> 1. **Establish Database Connection:** Use the connection string from `appsettings.json` to create a `SqlConnection`.
> 2. **Implement Streaming Read:**
> 
>     - Use the **`SqlDataReader`** for data extraction. This is critical as it operates as a forward-only stream, retrieving one row at a time and preventing the entire dataset from being loaded into memory. This approach is vastly more scalable than using a `DataSet` or an ORM like Entity Framework for bulk data movement.
> 
>     - Ensure all database objects (`SqlConnection`, `SqlCommand`, `SqlDataReader`) are wrapped in `using` statements to guarantee proper resource disposal.
> 3. **Dynamic Schema Inference:**
> 
>     - Before reading rows, use the `SqlDataReader.GetSchemaTable()` method. This returns a `DataTable` containing metadata about each column in the result set (name, data type, nullability), which is essential for the next phase.
> 
> 
> ### **Phase 3: Schema Translation (T-SQL to Iceberg)**
> 
> 
> 
> **Goal:** Convert the SQL schema into a valid Iceberg schema, paying close attention to Iceberg's specific requirements.
> 
> 1. **Define Data Type Mappings:** Create a utility function that maps `SqlDbType` values from the schema table to their corresponding Iceberg primitive types (e.g., `int` -> `int`, `bigint` -> `long`, `varchar` -> `string`, `datetime` -> `timestamp`, `decimal` -> `decimal(P,S)`).
> 2. **Assign Unique Field IDs:**
> 
>     - A core requirement of Iceberg is that every field in the schema must have a unique integer `field-id`. This ID persists across schema changes, enabling safe evolution.
> 
>     - For this initial table creation, iterate through the columns and assign a simple, monotonically increasing ID starting from 1.
> 3. **Construct Schema Objects:**
> 
>     - Create C# classes (POCOs) that model the Iceberg schema structure as defined in the specification (e.g., `IcebergSchema`, `NestedField`).
> 
>     - Populate these objects with the column name, the mapped Iceberg type, the assigned `field-id`, and the nullability information. This in-memory schema object will be used for both Parquet and metadata file generation.
> 
> 
> ### **Phase 4: Writing Iceberg-Compliant Parquet Files**
> 
> 
> 
> **Goal:** Write the data stream to Parquet files that include the Iceberg-specific `field-id` metadata.
> 
> 1. **Use `ParquetSharp`'s Low-Level API:**
> 
>     - The high-level API of `ParquetSharp` does not support adding the custom `field-id` metadata required by Iceberg.
> 
>     - You must use the lower-level **`GroupNode`** API to manually construct the Parquet file's schema. This gives you the fine-grained control needed to inject the `field-id` for each column, as successfully demonstrated in the inspirational blog post.
> 2. **Implement Streaming Write:**
> 
>     - Create a `ParquetFileWriter` using the custom `GroupNode` schema.
> 
>     - Inside the `while (reader.Read())` loop from Phase 2, write data in batches (known as "row groups" in Parquet).
> 
>     - For each batch, get a `RowGroupWriter`, then get a `ColumnWriter` for each column, and write the values from the `SqlDataReader`. This ensures data flows directly from the database to the file without being buffered in memory.
> 
> 
> ### **Phase 5: Metadata Generation and Catalog Commit**
> 
> 
> 
> **Goal:** Generate the hierarchical metadata files (manifests and table metadata) and perform an atomic commit via the REST catalog.
> 
> 1. **Generate Avro Manifest Files:**
> 
>     - **Implement the Schema Interception Fix:** The standard `Apache.Avro` library silently strips the required `field-id` attributes when writing Avro file headers. You must implement the workaround detailed in the blog post: create `AvroSchemaWrapper` and `AvroDatumWriterWrapper<T>` classes. These wrappers will intercept the schema serialization call and provide the complete, compliant Iceberg Avro schema as a raw string, ensuring the `field-id`s are preserved.
> 
>     - **Populate Manifests:** After each Parquet file is written, record its metadata (path, file size, record count, column-level stats) as an entry in a manifest file (`.avro`).
> 
>     - **Populate Manifest List:** Once all data is written, create a single manifest list file (`snap-....avro`) that points to the manifest file(s) you just created. This file represents the table's snapshot.
> 2. **Create Initial Table via REST Catalog:**
> 
>     - Before writing any data, your application must first register the new table with the catalog.
> 
>     - Send an HTTP `POST` request to the catalog's `v1/namespaces/{namespace}/tables` endpoint. The request body will be a JSON object containing the table name and the Iceberg schema you constructed in Phase 3.
> 
>     - The REST catalog will create the table and return the initial `v1.metadata.json` file content.
> 3. **Commit the New Snapshot:**
> 
>     - After all Parquet data files and Avro metadata files are written to their final locations in the warehouse, construct the new table metadata JSON (`v2.metadata.json`). This file will contain a new snapshot pointing to your newly created manifest list.
> 
>     - Perform the final atomic commit by sending an HTTP `POST` request to the catalog's `v1/tables/{namespace}/{table}` endpoint. The body of this request will contain the path to the previous metadata file (`v1.metadata.json`) and the content of the new metadata file (`v2.metadata.json`).
> 
>     - The REST catalog server is responsible for performing the atomic swap, guaranteeing that the table state transitions cleanly from the old version to the new one.
> 
> 
> ### **Phase 6: Cross-Ecosystem Validation**
> 
> 
> 
> **Goal:** Prove the tech demo's success by verifying that the generated table is fully readable and compliant using a standard, non-.NET tool.
> 
> 1. **Tool of Choice: `PyIceberg`**
> 
>     - `PyIceberg` is an excellent choice for validation as it is a pure Python implementation of the Iceberg specification.
> 2. **Validation Steps:**
> 
>     - **Install PyIceberg:**
> 
> 
> 
> ```Bash
> 
> pip install "pyiceberg[pyarrow,rest]"
> 
> ```
> 
>     - **Create a Python Validation Script:** Use the following script to connect to the local REST catalog, load the table, and inspect its contents.
> 
> 
> 
> ```Python
> 
> from pyiceberg.catalog import load_catalog
> 
> ```> ```
> # Configure catalog to connect to the local REST server
>     catalog = load_catalog(
>         "local_rest",
>         **{
>             "type": "rest",
>             "uri": "http://localhost:8181",
>         }
>     )
> ```> ```
> try:
>         # 1. Load the table (verifies catalog registration and metadata parsing)
>         table_identifier = "my_db.my_new_table" # Must match your config
>         table = catalog.load_table(table_identifier)
>         print(f"Successfully loaded table: {table_identifier}")
> ```> ```
> # 2. Validate the schema (verifies field-ids are present)
>         print("\nTable Schema:")
>         print(table.schema())
> ```> ```
> # 3. Query the data and verify row count
>         df = table.scan().to_pandas()
>         print(f"\nSuccessfully read {len(df)} rows.")
>         print("\nData Preview:")
>         print(df.head())
> ```> ```
> except Exception as e:
>         print(f"Validation failed: {e}")
>     ```
>   * **Execute and Verify:** Run the script. A successful output, showing the correct schema (with `field-id`s) and the expected data, confirms that your C\# application has correctly implemented the entire Iceberg specification. You can also use DuckDB with the Iceberg extension for an alternative SQL-based validation.
> ```

___
###### [top](#table-of-contents)
### chat-7

> how can a save this full conversation to a single file?

Of course. Here is the complete record of our conversation and all the research, formatted as a single Markdown file.

You can copy the entire content below and save it as a file with a `.md` extension (for example, `iceberg_project.md`) to keep a complete record.

# Conversation and Research: C# SQL Server to Apache Iceberg Demo

This document contains the full conversation and detailed research for implementing a tech demo to extract data from SQL Server and save it into a local Parquet file in an Iceberg table format.

> **User:**
> i want to implement a small tech demo which will extract data from a SQL Server db as defined via a T-SQL statement, and have  is saved into a local parquet file in an Iceberg table format. I'm  looking at this blog as the inspiration. [https://medium.com/@xorets/writing-iceberg-tables-with-c-a-journey-through-schema-compatibility-b169c846f5e0](https://medium.com/@xorets/writing-iceberg-tables-with-c-a-journey-through-schema-compatibility-b169c846f5e0). Can you help me to make a plan?> **You:**
> Here's the plan I've put together. Let me know if you need any changes before I start researching.
> http://googleusercontent.com/deep_research_confirmation_content/0> **User:**
> Start research> **You:**
> 
> 
> 
> # Architectural Guide: Implementing Apache Iceberg Table Generation in C# from SQL Server
> 
> 
> 
> ## I. Deconstructing the Apache Iceberg Table Format
> 
> 
> 
> To successfully implement a writer for Apache Iceberg, a foundational understanding of its architecture is paramount. Iceberg is not merely a file format but a comprehensive table format specification designed to bring the reliability and simplicity of relational database tables to large-scale analytic datasets stored in distributed file systems or object stores. Its architecture is deliberately designed to solve the problems of consistency, performance, and schema evolution that plague traditional data lakes. The format's power lies in its meticulously structured metadata, which enables transactional guarantees and efficient query planning.   
> 
> 
> 
> ### The Three-Tiered Architecture
> 
> 
> 
> An Iceberg table can be conceptualized as a three-tiered architecture, comprising the Catalog, the Metadata Layer, and the Data Layer.   
> 
> 1. **The Catalog:** The catalog is the entry point for interacting with an Iceberg table. Its primary responsibility is to track the location of the current metadata file for a given table name. When a write operation commits, the catalog performs an atomic swap to point from the old metadata file to the new one. This atomic operation is the cornerstone of Iceberg's ACID transaction guarantees. While production systems often use sophisticated catalogs like AWS Glue, a REST service, or a Hive Metastore, for a local tech demo, the catalog can be implemented simply as a pointer on the local filesystem.
> 2. **The Metadata Layer:** This is the core of the Iceberg format. It consists of a hierarchy of files that collectively describe the table's state at a specific point in time, known as a snapshot. This layer includes the table metadata file, manifest lists, and manifest files, which provide a multi-level index over the actual data. The structure of this layer is designed to enable fast, parallel query planning by allowing engines to prune massive portions of the table without listing files in the data directories.
> 3. **The Data Layer:** This layer consists of the actual data files, which are immutable once written. Iceberg is file-format agnostic, supporting Apache Parquet, Apache ORC, and Apache Avro. For this implementation, the data files will be in the Apache Parquet format, a highly efficient columnar storage format well-suited for analytic workloads.
> 
> 
> ### Anatomy of an Iceberg Table: The Metadata Hierarchy
> 
> 
> 
> The physical layout of an Iceberg table is a tree-like structure of metadata files that ultimately point to the data files. A correct implementation must generate each of these components in compliance with the specification.
> 
> - **Table Metadata (`metadata.json`):** This JSON file is the single source of truth for the entire table. It is the root of the metadata tree and contains all the essential information a query engine needs to begin planning a scan. Key fields include:   
> 
>     - `format-version`: An integer (1 or 2) specifying the version of the Iceberg spec the table adheres to. This is crucial for forward and backward compatibility.
> 
>     - `table-uuid`: A universally unique identifier for the table, ensuring that metadata operations are applied to the correct table instance.
> 
>     - `location`: The base URI for the table's location, where all data and metadata files are stored.
> 
>     - `schemas` and `current-schema-id`: A list of all historical schemas the table has ever had, with an ID pointing to the current one. This is fundamental to Iceberg's powerful schema evolution capabilities.
> 
>     - `partition-specs` and `default-spec-id`: A list of partition specifications, defining how data is partitioned. This allows for partition evolution over time.
> 
>     - `snapshots`: A list of all valid snapshots of the table. Each snapshot represents a complete state of the table at a point in time and points to a manifest list file.
> - **Manifest List (`snap-....avro`):** Each snapshot is defined by a single manifest list file, which is an Avro file. This file contains an entry for every manifest file that is part of that snapshot. The manifest list provides summary metadata, such as partition boundaries and counts of added or deleted files, which allows query planners to perform an initial, coarse-grained pruning. An engine can often skip reading a large number of manifest files entirely based on the summary information in the manifest list, drastically reducing the metadata overhead for a query. Key fields for each entry include    
> `manifest_path`, `partition_spec_id`, and `added_files_count`.
> - **Manifest Files (`....avro`):** Manifest files are also Avro files, and they contain the next level of detail. Each manifest file tracks a subset of the table's data files (or delete files, in v2 tables). For each data file, the manifest stores detailed statistics and metadata, including column-level metrics like lower and upper bounds for each column, null value counts, and NaN counts. This fine-grained information enables query engines to perform file-level pruning, skipping data files that cannot possibly contain data matching the query's predicates.
> - **Data Files (`....parquet`):** Finally, the manifest files point to the actual data files. These files contain the row data in a columnar format (Parquet in this case) and are considered immutable. Any operation that modifies data, such as an update or delete, does not change existing files but instead writes new data files (and potentially delete files) and creates a new snapshot reflecting the change.
> 
> 
> The hierarchical nature of this metadata is a deliberate and critical design choice. A key goal of the Iceberg specification is to enable query planning with a constant number of remote calls, rather than a number that scales with the size of the table (e.g., number of partitions or files). A query engine reads the small    
> 
> 
> 
> `metadata.json` file, then uses the manifest list to quickly identify a small subset of relevant manifest files. It then reads only those manifest files to get a list of data files that must be scanned. This multi-level indexing avoids the costly "list directory" operations that plague traditional Hive-style tables and is the source of Iceberg's performance at scale. Therefore, even for a local demonstration, it is imperative that the C# application generates this full, compliant hierarchy, including the summary statistics in the upper metadata layers. Omitting these statistics would result in a technically valid but un-optimizable table, defeating one of the primary advantages of the format.
> 
> 
> 
> ### The Role of Snapshots and Atomic Commits
> 
> 
> 
> Iceberg achieves transactional consistency (ACID) through the concepts of snapshots and atomic commits. A snapshot represents the complete state of a table at a specific moment in time. Every write operation—be it an append, overwrite, or schema change—produces a new snapshot by creating a new    
> 
> 
> 
> `metadata.json` file.   
> 
> 
> 
> The process of committing a new version of the table is an atomic operation. The writer first prepares all new data files and all new metadata files (manifests, manifest list, and the new `metadata.json`). Only when all these files are successfully written to storage does the writer perform a single, atomic "swap" operation in the catalog to point the table name to the new `metadata.json` file.
> 
> 
> 
> This mechanism of using immutable files and atomic metadata swaps provides several profound benefits:
> 
> - **Serializable Isolation:** Readers are completely isolated from concurrent writes. A reader will always see a consistent and complete snapshot of the table's data, as it will be using a specific `metadata.json` file that will not change during the query. It will never see partial or uncommitted changes.
> - **Time Travel and Reproducible Queries:** Since each `metadata.json` file points to a complete, historical state of the table, users can execute queries against any previous snapshot. This enables "time travel" to examine the state of data in the past or to re-run a query on a historical version for reproducibility.
> - **Version Rollback:** If a write operation introduces bad data, correcting the error is as simple as atomically resetting the catalog's pointer back to a previous, valid `metadata.json` file, effectively rolling back the erroneous commit.
> 
> 
> The principle of immutability is the bedrock of this reliability. The C# application must be designed as a "commit-based" writer. It should never attempt to modify any existing file in-place. Instead, it must orchestrate the creation of all new files and then perform the final, atomic commit of the new table state.
> 
> | File Type | File Format | Core Purpose | Key Fields / Content |
> |---|---|---|---|
> | Table Metadata | JSON | The table's single source of truth; the root of the metadata tree. | format-version, table-uuid, location, schemas, current-schema-id, partition-specs, snapshots |
> | Manifest List | Avro | An index of manifest files for a specific snapshot; enables manifest pruning. | manifest_path, manifest_length, partition_spec_id, added_files_count, partition boundary summaries |
> | Manifest | Avro | An index of data files; contains detailed stats for file pruning. | file_path, file_format, record_count, file_size_in_bytes, column-level stats (min/max values, null counts) |
> | Data File | Parquet | Stores the actual row data in a columnar format. | Row data corresponding to the table schema. |Export to Sheets
> 
> ## II. Phase 1: High-Performance Data Ingestion from SQL Server
> 
> 
> 
> The first active phase of the implementation involves efficiently extracting data from the source SQL Server database. The choice of data access strategy is a critical architectural decision that directly impacts the performance, scalability, and memory footprint of the application. For a data engineering pipeline tasked with moving potentially large volumes of data, a streaming-based approach is not just a best practice; it is a necessity.
> 
> 
> 
> ### Establishing a Robust and Secure Database Connection
> 
> 
> 
> The initial step is to establish a connection to the SQL Server instance. This should be done using a standard connection string. Best practices for security and maintainability dictate that this connection string should not be hard-coded. Instead, it should be managed through a configuration file (e.g., `appsettings.json`) and potentially secured using mechanisms like Azure Key Vault or Windows Certificate Store encryption, especially in a production context. For this tech demo, a configuration file is sufficient. The connection should ideally use Windows Authentication if the application runs in a trusted environment to avoid managing passwords in the connection string.   
> 
> 
> 
> ### The Streaming Imperative: Leveraging `SqlDataReader` for Memory-Efficient Data Extraction
> 
> 
> 
> When choosing a data access technology in.NET for bulk data movement, the options generally fall into two categories: object-relational mappers (ORMs) like Entity Framework, and lower-level libraries like Dapper or raw ADO.NET. While ORMs offer significant productivity gains for typical online transaction processing (OLTP) applications by abstracting away SQL and mapping results to C# objects, they are fundamentally ill-suited for this task. The primary drawback of most ORM approaches, including Entity Framework, is that they buffer the entire query result set into memory before returning control to the application. For a query that returns millions or billions of rows, this will inevitably lead to excessive memory consumption and    
> 
> 
> 
> `OutOfMemoryException` errors.
> 
> 
> 
> The optimal solution is to use the raw ADO.NET `SqlDataReader`. The `SqlDataReader` is designed specifically for high-performance, forward-only, read-only data retrieval. It operates as a connected, streaming cursor, often referred to as a "firehose cursor". This means it maintains an open connection to the database and retrieves data row-by-row as the application iterates through it using the    
> 
> 
> 
> `Read()` method. This approach has a minimal memory footprint, as only one row (or a small internal buffer) is held in the application's memory at any given time, making it capable of processing datasets that are orders of magnitude larger than the available system RAM.   
> 
> 
> 
> To ensure proper resource management and prevent connection leaks, which are common pitfalls in database programming, all ADO.NET objects (`SqlConnection`, `SqlCommand`, `SqlDataReader`) must be wrapped in `using` statements. This guarantees that the `Dispose()` method is called on each object, closing the reader and the connection even if an exception occurs during data processing.   
> 
> 
> 
> For queries that may involve columns with very large data, such as `VARBINARY(MAX)` or `NVARCHAR(MAX)`, an additional optimization should be employed. When executing the command, the `CommandBehavior.SequentialAccess` flag should be used. This changes the default behavior of the `SqlDataReader`. Instead of loading the entire contents of a row into memory when `Read()` is called, it requires the application to access the columns in the order they are selected. More importantly, for large object (LOB) columns, it allows the data to be read as a stream via methods like `GetStream()`, preventing the entire BLOB from being loaded into memory at once.   
> 
> 
> 
> The core logic of the application should therefore be a direct, synchronous pipeline that flows from the database to the file writer. The data should be read from the `SqlDataReader` and immediately written to the `ParquetFileWriter` within the same processing loop. This avoids the need for intermediate data structures like a `DataSet` or `DataTable`, which are in-memory representations of data and would negate the benefits of the streaming `DataReader`. A    
> 
> 
> 
> `DataSet` acts as a disconnected "bucket" of data, whereas the `DataReader` is a connected "hose"; for this task, the hose is the correct tool. The architecture should be a tight loop:    
> 
> 
> 
> `while (reader.Read()) {... process row and write to parquet... }`. This design minimizes memory allocation and garbage collection pressure, enabling the application to scale effectively.
> 
> 
> 
> ### Dynamically Inferring Schema from the `SqlDataReader` Result Set
> 
> 
> 
> To make the tech demo flexible and not tied to a specific query, the schema of the result set should be determined at runtime. The `SqlDataReader` provides a mechanism for this through the `GetSchemaTable()` method. This method returns a `DataTable` where each row describes a column in the result set, providing essential metadata such as the column name, data type (`SqlDbType`), size, precision, scale, and nullability. This metadata is the raw material needed for the next phase: translating the SQL schema into a compliant Iceberg schema.
> 
> 
> 
> ## III. Phase 2: Bridging the Schema Divide: From T-SQL to Iceberg
> 
> 
> 
> With a stream of data and its corresponding schema metadata available from the `SqlDataReader`, the next critical phase is to translate this information into an Apache Iceberg schema. This is not a simple one-to-one mapping of names and types; it requires a deep understanding of Iceberg's schema evolution principles, particularly the mandatory use of unique field identifiers. This phase lays the structural foundation for the entire Iceberg table.
> 
> 
> 
> ### Translating SQL Server Data Types to Iceberg and Parquet Equivalents
> 
> 
> 
> The first step is to establish a definitive mapping between the data types reported by SQL Server and the primitive types defined in the Apache Iceberg specification. A mismatch or incorrect mapping at this stage can lead to data corruption, loss of precision, or runtime failures during the write process. The    
> 
> 
> 
> `GetSchemaTable()` method provides the `SqlDbType` for each column, which must be mapped to an appropriate Iceberg type. The following table provides a comprehensive mapping for this translation.
> 
> | SQL Server Type (SqlDbType) | .NET Type (Type) | Iceberg Primitive Type | Parquet Physical Type | Implementation Notes |
> |---|---|---|---|---|
> | BigInt | long | long | INT64 | Direct mapping. |
> | Int | int | int | INT32 | Direct mapping. |
> | SmallInt | short | int | INT32 | SQL smallint fits within Iceberg int. |
> | TinyInt | byte | int | INT32 | SQL tinyint fits within Iceberg int. |
> | Bit | bool | boolean | BOOLEAN | Direct mapping. |
> | Float | double | double | DOUBLE | SQL float is a double-precision float. |
> | Real | float | float | FLOAT | SQL real is a single-precision float. |
> | Decimal, Numeric, Money, SmallMoney | decimal | decimal(P, S) | FIXED_LEN_BYTE_ARRAY | Precision (P) and scale (S) must be preserved from the SQL schema and included in the Iceberg type definition. |
> | Date | DateTime | date | INT32 (Logical: Date) | Represents days since the Unix epoch. |
> | DateTime, SmallDateTime, DateTime2 | DateTime | timestamp | INT64 (Logical: Timestamp) | Represents microseconds from the Unix epoch, without timezone. |
> | DateTimeOffset | DateTimeOffset | timestamptz | INT64 (Logical: Timestamp) | Represents microseconds from the Unix epoch, with timezone (stored as UTC). |
> | Char, NChar, VarChar, NVarChar, Text, NText | string | string | BYTE_ARRAY (UTF8) | All string types map to Iceberg's string type. |
> | Binary, VarBinary, Image | byte | binary | BYTE_ARRAY | For variable-length binary data. |
> | UniqueIdentifier | Guid | uuid | FIXED_LEN_BYTE_ARRAY (16) | Maps to Iceberg's dedicated uuid type. |Export to Sheets
> 
> ### The Criticality of `field-id`
> 
> 
> 
> A defining feature of the Iceberg schema is the mandatory `field-id` for every column. This is a unique integer identifier assigned to each field within the table's schema. Its purpose is to provide a stable, persistent identity for a column that survives across schema changes. This is the mechanism that enables safe and unambiguous schema evolution, a core goal of the Iceberg project.   
> 
> 
> 
> For example, consider renaming a column from `user_email` to `email_address`. In a traditional format, this might be interpreted as dropping the `user_email` column and adding a new `email_address` column. When querying old data files, the `email_address` column would appear as null because it didn't exist when those files were written. In Iceberg, a rename operation only changes the `name` property in the metadata; the `field-id` remains the same. This allows query engines to correctly map the data from old files, which was written under the `field-id` for `user_email`, to the newly named `email_address` column, preserving the data.
> 
> 
> 
> The inability to set this `field-id` is the primary technical challenge highlighted in the inspirational blog post and the reason for abandoning the high-level APIs of the `ParquetSharp` library. The implementation must therefore treat the generation and management of these IDs as a first-class concern.   
> 
> 
> 
> ### Strategy for Assigning `field-id`s
> 
> 
> 
> For the initial creation of a new Iceberg table, the strategy for assigning `field-id`s is straightforward. The application should iterate through the columns as they are discovered from the `SqlDataReader`'s schema and assign a monotonically increasing integer, starting from 1.
> 
> 1. Column 1 -> `field-id: 1`
> 2. Column 2 -> `field-id: 2`
> 3. Column 3 -> `field-id: 3`
> 
> 4....and so on.
> 
> 
> It is crucial to note that for any subsequent operations on this table (e.g., appending data, evolving the schema), these assigned IDs must be preserved. The application would need to first read the existing table's metadata to learn the existing `field-id`s and the `last-column-id` before performing any modifications. For this tech demo, which focuses only on the initial creation, the simple monotonic assignment is sufficient.
> 
> 
> 
> ### Constructing the Iceberg Schema Object
> 
> 
> 
> Before any files can be written, the complete Iceberg schema must be constructed in memory. This involves creating a set of C# Plain Old CLR Objects (POCOs) that can be serialized to the JSON format required in the `metadata.json` file. The structure should mirror the specification: a top-level schema object containing a list of `NestedField` objects.
> 
> 
> 
> A representative C# class structure might look like this:
> 
> 
> 
> csharp
> public class IcebergSchema
> {
> [JsonPropertyName("type")]
> public string Type { get; set; } = "struct";
> 
> 
> ```
> [JsonPropertyName("schema-id")]
> public int SchemaId { get; set; }
> ```> [JsonPropertyName("fields")]
> public List Fields { get; set; }
> 
> 
> ```
> }
> ```> public class NestedField
> {
> [JsonPropertyName("id")]
> public int Id { get; set; } // The critical field-id> ```
> [JsonPropertyName("name")]
> public string Name { get; set; }
> ```> [JsonPropertyName("required")]
> public bool Required { get; set; }> [JsonPropertyName("type")]
> public object Type { get; set; } // Can be a string for primitives or a complex object for decimals
> 
> 
> ```
> }
> ```> ```
> The application logic will iterate over the columns from `SqlDataReader.GetSchemaTable()`, and for each column, it will create a `NestedField` instance. It will populate the `Id` with the newly assigned monotonic integer, the `Name` from the column name, the `Required` property based on the column's nullability, and the `Type` based on the data type mapping table. This in-memory `IcebergSchema` object will then be used in both the Parquet file generation and the final `metadata.json` creation.
> ```> ## IV. Phase 3: Crafting Compliant Parquet Data Files in C#> With the data stream ready and the Iceberg schema defined, the next phase is to write the actual data files. The user's requirement is to use the Apache Parquet format. This phase addresses the technical nuances of using the `ParquetSharp` library to produce files that are not only valid Parquet files but are also fully compliant with the specific metadata requirements of the Iceberg specification.> ### Introduction to `ParquetSharp`> `ParquetSharp` is a.NET library that provides high-performance read and write access to Apache Parquet files. It serves as a C# wrapper around the official native `apache-arrow` C++ implementation of Parquet, which ensures excellent performance and adherence to the Parquet format standard. The library offers both high-level APIs for convenience and low-level APIs for fine-grained control.> ### The `field-id` Challenge and the Low-Level `GroupNode` Solution> As established, the central challenge is embedding the Iceberg-mandated `field-id` into the Parquet file's schema metadata. The inspirational blog post details a failed attempt to use `ParquetSharp`'s high-level `Column` API for this purpose, as it provides no mechanism to attach this specific piece of metadata. This demonstrates a common pattern in software development: a specification from one ecosystem (the JVM-centric world of big data where Iceberg originated) often has requirements that have not yet been incorporated as first-class features into the convenient, high-level abstractions of libraries in other ecosystems (like.NET).> To overcome this, the implementation must bypass the high-level abstractions and use the lower-level `GroupNode` API, which provides the necessary control to build the schema definition manually and inject the required metadata. This approach, while more verbose, is the key to achieving compliance.> The process for constructing the compliant Parquet schema is as follows:> 1. **Initialize a list of `Node` objects.** This list will hold the schema definition for each column.
> 2. **Iterate through the `NestedField` objects** of the in-memory `IcebergSchema` created in the previous phase.
> 3. **For each field, create a `LogicalType` annotation.** The `ParquetSharp` library provides a `LogicalType` class. The specific annotation required by Iceberg is not a standard Parquet logical type but a custom one that can be represented using the library's extensibility. The crucial part is to correctly associate the `field-id`. While Parquet itself doesn't have a native `field-id` concept in its core specification, Iceberg readers look for this information in specific locations within the file metadata. The `ParquetSharp` representation allows for this via its schema construction. The blog post's successful strategy involved creating a new `PrimitiveNode` that explicitly included the `fieldId` from the source column's definition.
> 4. **Create the `PrimitiveNode` for the column.** Based on the Iceberg-to-Parquet type mapping (e.g., `Iceberg int` -> `Parquet INT32`), create the corresponding `PrimitiveNode` (e.g., `new PrimitiveNode(typeof(int), Repetition.Optional)`). When constructing this node, the `fieldId` must be passed. The `ParquetSharp` API allows for this by manually building the schema tree. A `PrimitiveNode` can be created with a name, a repetition level, and crucially, a `field_id` parameter in some of its constructors or builder methods. The exact mechanism involves creating a `ColumnDescriptor` that includes the field ID.
> 5. **Construct the Root `GroupNode`.** Once the list of `PrimitiveNode` objects is complete, they are used to construct a single root `GroupNode`, which represents the overall schema of the Parquet file.
> 6. **Instantiate the `ParquetFileWriter`.** This `GroupNode` schema object is then passed directly to the constructor of the `ParquetFileWriter`. This ensures that the file is created with a schema that contains the necessary `field-id` information for every column, making it readable by Iceberg-compliant engines.> ### Streaming Data from `SqlDataReader` into the Writer> This low-level schema construction must be integrated seamlessly with the high-performance data streaming pipeline established in Phase 1. The overall workflow within the application's main processing loop will be:> 1. Outside the loop, create the `ParquetFileWriter` with the custom-built `GroupNode` schema and a `FileStream` pointing to the output Parquet file path.
> 2. Inside the `while (reader.Read())` loop, process the data in batches (row groups). A row group is a horizontal partitioning of the data within a Parquet file.
> 3. For each batch of rows (e.g., 100,000 rows, a configurable size), create a new `RowGroupWriter` from the `ParquetFileWriter`.
> 4. Within the batch, iterate through each row from the `SqlDataReader`.
> 5. For each row, iterate through its columns.
> 6. Get the `ColumnWriter` for the current column from the `RowGroupWriter`.
> 7. Retrieve the value from the `SqlDataReader` for that column (e.g., `reader.GetInt32(i)`).
> 8. Write the value to the `ColumnWriter` using its `WriteBatch` method.
> 9. After processing all rows in the batch, dispose of the `RowGroupWriter`, which flushes the data for that row group to the file.
> 10. After the main `while` loop finishes, dispose of the `ParquetFileWriter`, which writes the file footer containing the schema and other metadata, finalizing the file.> This approach ensures that the entire process remains a memory-efficient stream. Data flows from the database connection, through the application with minimal buffering, and directly into the Parquet file on disk.> ## V. Phase 4: The Core of Iceberg: Programmatic Metadata Generation> This phase addresses the most intricate and challenging aspect of the implementation: the programmatic generation of the Iceberg metadata files, specifically the Avro-based manifest and manifest list. Success in this phase hinges on overcoming a subtle but critical limitation in the standard.NET Avro library, a problem that was painstakingly diagnosed and solved in the user's inspirational material.> ### The `Apache.Avro` Library Challenge> The official `Apache.Avro` library for.NET is the natural choice for working with Avro files. The challenge, as discovered in the source blog, is not in its ability to handle basic Avro data, but in its handling of the specific, extended Avro schemas required by the Iceberg specification. Iceberg's Avro schemas for manifest files include custom attributes like `field-id`, `element-id`, `key-id`, and `value-id`, which are essential for schema evolution and compatibility.> The problem is that while the `Apache.Avro` library can correctly _parse_ a JSON schema string containing these attributes, its default serialization logic silently _strips_ them when it writes the schema into the header of the Avro Object Container File (OCF). The library serializes only the elements it recognizes from the base Avro specification, discarding the Iceberg-specific additions. This results in a structurally correct but semantically non-compliant Avro file that will be rejected by any standard Iceberg reader, as the required field ID mappings will be missing. This silent failure mode makes the issue particularly difficult to debug without attempting to read the generated file with an external, compliant tool.> ### Implementing the "Strategic Schema Interception" Pattern> The solution to this problem is a clever and surgical workaround that intercepts the library's schema serialization process at the precise moment of failure and injects the correct, fully compliant schema. This "Strategic Schema Interception" pattern, detailed in the blog post, avoids the need to fork the library or write a custom Avro serializer from scratch. It is a powerful technique for dealing with library-specification mismatches.> The implementation requires two custom wrapper classes:> 1. **`AvroSchemaWrapper`:** This class is a simple wrapper around an `Apache.Avro.Schema` instance. Its primary function is to store the original, untouched JSON schema string that was used to create the schema object. The critical part of this class is its override of the `ToString()` method. Instead of allowing the base class to regenerate a (stripped) JSON string from the parsed schema object, this override simply returns the original, pristine JSON string, complete with all Iceberg-specific attributes.> 1. **`AvroDatumWriterWrapper<T>`:** This class wraps a standard `DatumWriter<T>` (e.g., `GenericDatumWriter<GenericRecord>`). It delegates all actual data writing calls to the inner writer instance. Its key feature is a public `Schema` property that, instead of returning the inner writer's schema directly, returns a new instance of the `AvroSchemaWrapper` created above.> The workflow for using these wrappers is as follows:
> 
> - First, the compliant Avro schemas for the manifest file and the manifest list file are defined as static JSON strings within the C# application. These schemas should be taken directly from the official Apache Iceberg specification to ensure correctness.
> - When it's time to write a manifest file, a `GenericDatumWriter<GenericRecord>` is created with the parsed schema.
> - This standard writer is then passed into the constructor of our custom `AvroDatumWriterWrapper<T>`.
> - Finally, this wrapped writer is passed to the `DataFileWriter<GenericRecord>.OpenWriter()` method.> When the `DataFileWriter` prepares to write the file header, it needs to serialize the schema. To do this, it accesses the `Schema` property of the writer it was given and calls `ToString()` on the result. This is where the interception occurs: it calls our `AvroDatumWriterWrapper.Schema` property, which returns our `AvroSchemaWrapper`, and then calls `ToString()` on that object. Our overridden method then returns the original, complete JSON schema string, which is written correctly into the Avro file header. This targeted intervention fixes the library's limitation with minimal and non-invasive code.> ### Populating the Metadata Files> With the writing mechanism in place, the application must populate these files with the correct data.> - **Manifest File (`....avro`):** After each Parquet data file is successfully written and closed, the application will have gathered its final metadata: its full path, its length in bytes, and the total number of records it contains. An Avro `GenericRecord` is created based on the manifest entry schema. This record is populated with the data file's statistics. This `GenericRecord` is then written as a single entry to the manifest Avro file using the wrapped writer. For this simple append-only demo, the manifest will contain one entry for each Parquet file generated.> - **Manifest List File (`snap-....avro`):** Once all data files have been written and the manifest file has been finalized and closed, the application gathers the metadata of the manifest file itself (its path and length). It then creates a `GenericRecord` based on the manifest list entry schema and populates it with the manifest file's information, along with summary statistics (e.g., `added_files_count` will be the total number of Parquet files written). This record is then written to the manifest list Avro file.> ### Constructing the Final `metadata.json` File> The final step in metadata generation is creating the top-level `metadata.json` file. This is a straightforward JSON serialization task.> 1. **Create C# POCOs:** Define a set of C# classes that mirror the structure of the `metadata.json` file as defined in the Iceberg specification. This includes classes for    
> `TableMetadata`, `Snapshot`, `Schema`, `NestedField`, etc.
> 2. **Populate the Model:** Instantiate the root `TableMetadata` object and populate its properties:
> 
>     - Generate a new `table-uuid`.
> 
>     - Set the `location` to the root directory of the table.
> 
>     - Set the `format-version` to 2 (as v1 is largely legacy).
> 
>     - Assign the `IcebergSchema` object created in Phase 2 to the `schemas` list and set the `current-schema-id`.
> 
>     - Create an initial, empty `partition-specs` list.
> 
>     - Create a new `Snapshot` object. This object will be given a unique `snapshot-id` (a randomly generated long integer), the current timestamp, and, most importantly, the relative path to the manifest list file (`snap-....avro`) that was just created.
> 
>     - Add this `Snapshot` object to the `snapshots` list in the metadata and set the `current-snapshot-id` to its ID.
> 3. **Serialize to JSON:** Use a standard.NET JSON serialization library (like `System.Text.Json`) to serialize the populated `TableMetadata` object into a formatted JSON string. This string will be saved as the first version of the metadata file (e.g., `v1.metadata.json`).> ## VI. Phase 5: The Atomic Commit: Finalizing the Table State> After generating all the necessary data and metadata files, the final phase is to commit the new table state to the filesystem in a transactionally safe manner. This step is what elevates the collection of files into a true, queryable Iceberg table with ACID guarantees. The atomicity of this final action is critical to prevent race conditions and ensure that readers never observe a partially written or inconsistent state.> ### Filesystem Catalog Logic> In a production environment with a dedicated Iceberg catalog service (like AWS Glue or a REST catalog), the final commit step involves making an API call to the catalog to atomically update the pointer from the old metadata location to the new one. For this tech demo, which uses the local filesystem as both storage and catalog, a simpler but equally effective mechanism is used.> The state of a filesystem-based Iceberg table is typically determined by one of two methods:
> 
> 1. **Listing Metadata Files:** A reader can list all `v*.metadata.json` files in the metadata directory and parse the version number from the filename to identify the latest one.
> 2. **Version Hint File:** A more robust and efficient method is to use a `version-hint.txt` file in the metadata directory. This simple text file contains a single line: the version number of the current `metadata.json` file (e.g., "1", "2", etc.). A reader first reads this hint file to know which metadata file to load (e.g., `v2.metadata.json`).> The commit operation must atomically update this hint.> ### The Commit Process> The process must ensure that a concurrent reader cannot access the new metadata file until all associated data and metadata files are fully and correctly written to their final destinations.> 1. **Write Files to Final Locations:** During the previous phases, all files—the Parquet data files in the `/data` subdirectory and the Avro manifest, Avro manifest list, and the new versioned JSON metadata file (e.g., `v1.metadata.json`) in the `/metadata` subdirectory—should be written directly to their final paths.
> 2. **Prepare the Hint File:** Create the content of the new version hint file. For the first commit, this will simply be the string "1".
> 3. **Atomic Update:** The final, critical step is to atomically update the `version-hint.txt` file. This should **not** be done by opening and writing to the file directly, as that is not an atomic operation. A concurrent reader could potentially read the file during the write, resulting in a corrupted or empty version hint. The correct procedure is:
> a.  Write the new version number to a temporary file (e.g., `version-hint.txt.tmp`) in the same metadata directory.
> b.  Use an atomic rename or move operation to replace the old `version-hint.txt` with the new temporary file. On most modern filesystems, a `File.Move` operation within the same volume is an atomic metadata-only operation.> This sequence guarantees that any reader will either see the old, consistent state of the table (if it reads before the atomic rename) or the new, fully consistent state of the table (if it reads after). There is no intermediate state where a reader could load the new metadata file before all of its dependent files are present and complete. This simple filesystem operation faithfully implements the atomic commit principle at the heart of the Iceberg specification.> ## VII. Phase 6: Cross-Ecosystem Validation Strategy> The ultimate measure of success for this tech demo is not whether the C# application can write and then read its own files, but whether the generated Iceberg table is a valid, interoperable artifact that can be consumed by the broader big data ecosystem. The subtle compatibility issues, such as the one encountered with the `Apache.Avro` library, often only manifest when a different, canonical implementation attempts to read the data. Therefore, a rigorous validation phase using an external, non-.NET tool is not an optional add-on but a mandatory final step in the development process.> ### The Need for External Validation> Internal validation is insufficient because a library might have non-standard behaviors or silent bugs that produce files that are only compatible with itself. The value proposition of an open table format like Iceberg is precisely that it enables multiple, diverse engines like Spark, Trino, Flink, and DuckDB to work with the same data safely and concurrently. Proving that the C#-generated table integrates into this ecosystem is the primary goal. The inspirational blog post itself used    
> 
> 
> 
> `PyIceberg` as the external validator that ultimately uncovered the critical schema serialization bug.> For this validation plan, `PyIceberg` is the recommended tool due to its straightforward setup, Pythonic API, and direct implementation of the Iceberg specification.> ### Validation with PyIceberg> The following steps provide a concrete plan for verifying the integrity of the generated table.> 1. **Environment Setup:**
> 
>     - Install a recent version of Python.
> 
>     - Install the necessary Python packages using pip. `PyIceberg` requires an I/O manager to interact with the filesystem; `pyarrow` is a common choice. The command will be: `pip install "pyiceberg[pyarrow]"`.> 1. **Loading the Table:**
> 
>     - A Python script will be written to load the table. It will first configure and load a local filesystem catalog pointing to the parent directory of the generated table warehouse. Then, it will use `catalog.load_table()` to open the specific table.
> 
>     - **Validation Check:** A successful execution of `load_table()` without exceptions is the first sign of success. It indicates that `PyIceberg` was able to find the `version-hint.txt`, load the corresponding `metadata.json`, and parse it correctly.> ```
> ```python
> from pyiceberg.catalog import load_catalog
> ```> ```
> # Configure a catalog to read from the local filesystem
> # 'warehouse' should be the parent directory where the table folder is located
> catalog = load_catalog(
>     "local",
>     **{
>         "type": "rest", # Using REST as a placeholder for local file access logic
>         "uri": "http://localhost:8181", # Not used for local, but required
>         "warehouse": "file:///path/to/your/warehouse"
>     }
> )
> ```> ```
> # Load the table
> try:
>     table = catalog.load_table("your_db.your_table_name")
>     print("Successfully loaded the Iceberg table.")
> except Exception as e:
>     print(f"Failed to load table: {e}")
>     exit()
> ```
> ```> 1. **Schema Validation:**
> 
>     - After successfully loading the table, the script will inspect and print the schema.
> 
>     - **Validation Check:** The output of `print(table.schema())` should be visually inspected. It must show the correct column names, data types, and nullability. Most importantly, it must display the unique, integer `field-id` for each column. If the `field-id`s are missing or incorrect, it points to a failure in the Parquet schema generation (Phase 3) or the Avro manifest serialization (Phase 4).> 1. **Data Querying and Verification:**
> 
>     - The final and most comprehensive check is to read the actual data. The `table.scan()` method creates a scanner object, and `.to_pandas()` will execute the scan and load the entire table's data into a Pandas DataFrame.
> 
>     - **Validation Check:** The script should print the first few rows of the DataFrame (`df.head()`) and the total number of rows (`len(df)`). This data should be compared against the source data in SQL Server to confirm that all rows were written correctly and that the values are intact without corruption or type-casting errors.> ```
> ```python
> # Scan the table and load data into a Pandas DataFrame
> df = table.scan().to_pandas()
> ```> ```
> print("\nTable Schema:")
> print(table.schema())
> ```> ```
> print("\nData Preview (first 5 rows):")
> print(df.head())
> ```> ```
> print(f"\nTotal rows read: {len(df)}")
> # Compare this count with the source row count from SQL Server
> ```
> ```> ### Alternative Validation Tools> For even greater confidence, especially as the project evolves, other standard tools can be used:> - **Apache Spark:** Configure a local Spark session with the Iceberg extension jars. Then, register the filesystem-based catalog and run SQL queries like `SELECT * FROM local.your_db.your_table_name LIMIT 10;`. A successful query in Spark provides a very high degree of confidence in the table's correctness.
> - **DuckDB:** DuckDB, an in-process analytical database, has a powerful Iceberg extension. It can be configured to read from a local Iceberg table, allowing for fast, interactive SQL validation.> Successfully passing these external validation checks confirms that the tech demo has not just created a set of files but has produced a genuinely interoperable data asset that adheres to the Apache Iceberg open standard. This is the definitive "definition of done" for the project.> ## VIII. Strategic Recommendations and Architectural Blueprint> This final section synthesizes the preceding phases into a cohesive architectural blueprint for the tech demo. It provides recommendations on code structure, best practices, and clear pathways for extending the proof-of-concept into a more feature-rich and robust data engineering tool.> ### A Cohesive Code Structure> To ensure maintainability and clarity, the C# project should be organized logically, separating concerns into distinct namespaces or even separate class library projects. A recommended structure would be:> - **`DemoApp/` (Console Application Project):**
> 
>     - `Program.cs`: The main entry point. Responsible for parsing configuration, orchestrating the pipeline, and handling top-level exceptions.
> 
>     - `appsettings.json`: Configuration file for the SQL connection string, T-SQL query, and output table path.> - **`Core/` (Class Library Project):**
> 
>     - `Models/`: Contains the C# POCOs that map to the Iceberg metadata structures (`TableMetadata`, `Snapshot`, `IcebergSchema`, `NestedField`, `ManifestEntry`, etc.).
> 
>     - `Constants/`: Static class for holding constant values, such as the Avro schema JSON strings for manifest and manifest list files.> - **`IO/` (Class Library Project):**
> 
>     - `SqlServerExtractor.cs`: Encapsulates all logic for connecting to SQL Server and streaming data via `SqlDataReader`.
> 
>     - `ParquetWriter.cs`: Contains the logic for creating the `ParquetSharp` `GroupNode` schema with `field-id`s and writing the data stream to a Parquet file.
> 
>     - `IcebergMetadataWriter.cs`: The most complex component, containing the `AvroSchemaWrapper` and `AvroDatumWriterWrapper` classes, and the logic for writing the manifest and manifest list Avro files.
> 
>     - `TableCommitter.cs`: Handles the final atomic commit logic, such as writing the `metadata.json` and updating the `version-hint.txt` file.> This separation allows each component to focus on a single responsibility, making the code easier to understand, test, and extend.> ### Best Practices for Error Handling, Configuration, and Resource Management> - **Configuration:** As mentioned, all external parameters (connection string, query, output path, batch sizes) should be externalized into a configuration file. This allows the tool to be re-used without recompilation.
> - **Error Handling:** The entire data pipeline should be wrapped in a `try...catch...finally` block. The `catch` block should log detailed error information. The `finally` block is critical for ensuring that all I/O resources (database connections, file streams) are closed and disposed of properly, even if an error occurs mid-process. The application should also perform cleanup of any partially written files in the event of a failure to avoid leaving the table directory in a corrupted state.
> - **Resource Management:** Consistently use `using` statements for all `IDisposable` objects, including `SqlConnection`, `SqlCommand`, `SqlDataReader`, `FileStream`, and all `ParquetSharp` and `Apache.Avro` writer objects. This is the idiomatic C# way to ensure deterministic cleanup of unmanaged resources.
> - **Logging:** Implement structured logging (e.g., using Serilog or NLog) to provide visibility into the pipeline's execution. Log key events such as successful connection, schema inference, start/end of file writing, number of rows processed, and the final commit.> ### Pathways for Extending the Demo> The architecture described in this report provides a solid foundation. Once the core functionality is proven, it can be extended in several important ways to create a more powerful tool:> - **Partitioning:** This is the most logical next step for performance optimization.
> 
>     - **Implementation:** This would involve adding a `partition-specs` definition to the `metadata.json`. The data writing logic would need to be modified to route rows to different Parquet files based on their partition values (e.g., writing data for each day into a separate file in a `date=YYYY-MM-DD/` subdirectory). The manifest files would then track which partitions each data file belongs to.
> - **Schema Evolution:** The use of `field-id`s already enables this.
> 
>     - **Implementation:** An "update" operation would first need to read the latest `metadata.json` to understand the current state. To add a column, it would append a new schema to the `schemas` list with a new `last-column-id`, create a new snapshot, and commit a new `metadata.json`. The core writing logic would remain largely the same.
> - **Cloud Storage Integration:** The file I/O logic can be abstracted behind an interface.
> 
>     - **Implementation:** Create an `IFileSystem` interface with methods like `OpenWrite`, `OpenRead`, and `AtomicMove`. Implement a `LocalFileSystem` class for the current functionality and then create `S3FileSystem` or `AzureBlobFileSystem` classes that use the respective cloud provider SDKs. This would allow the tool to write Iceberg tables directly to cloud data lakes.
> - **Support for Updates and Deletes (Merge-on-Read):** This is an advanced feature requiring support for Iceberg v2 tables.
> 
>     - **Implementation:** This would involve writing "delete files" in addition to data files. Delete files record which rows in existing data files should be considered deleted. This is a significantly more complex operation but is the foundation for supporting row-level mutations in a data lakehouse architecture.> By following this comprehensive plan, the developer can successfully build a tech demo that not only meets the immediate requirement but also serves as a robust starting point for building production-grade data engineering pipelines in the.NET ecosystem.> **User:**
> in this demo - where would the iceberg metadata be stored? we want to minimize external dependencies where-ever possible> **You:**
> Excellent question. To align with your goal of minimizing external dependencies, the Iceberg metadata for this demo will be stored directly on the **local filesystem** alongside the Parquet data files.
> 
> 
> 
> This approach uses what is known as a "filesystem catalog," which is the simplest way to manage an Iceberg table and requires no external services, databases, or network daemons.
> 
> 
> 
> Here’s a breakdown of how it will be structured:
> 
> 1. **Root Table Directory:** A single main directory will be created for the table (e.g., `/path/to/my_iceberg_table/`). This directory will contain everything needed for the table to be a self-contained, portable unit.
> 2. **Subdirectory Structure:** Inside this root directory, the generated files will be organized into two standard subdirectories:
> 
>     - `data/`: This will contain all the actual data files, which in our case will be the `.parquet` files.
> 
>     - `metadata/`: This is where all the Iceberg-specific metadata files will live.
> 3. **Metadata Files:** The `metadata/` directory will contain the full hierarchy of files that define the table's state :   
> 
>     - **Table Metadata (`.metadata.json`):** A versioned JSON file (e.g., `v1.metadata.json`, `v2.metadata.json`) that acts as the single source of truth for the table's schema, partition information, and a list of all historical snapshots.
> 
>     - **Manifest List (`.avro`):** An Avro file for each snapshot that points to the specific manifest files making up that version of the table.
> 
>     - **Manifest Files (`.avro`):** Avro files that list the actual data files (`.parquet`) and contain detailed statistics about them, which is crucial for efficient query planning.
> 4. **The "Catalog" Pointer:** To identify the current version of the table without an external service, a simple text file named `version-hint.txt` will be placed in the `metadata/` directory. This file contains a single number indicating which metadata file is the current one (e.g., "1" to point to `v1.metadata.json`). This is a standard, lightweight mechanism for filesystem-based Iceberg tables.
> 
> 
> By storing all metadata as files on the local disk, the entire tech demo becomes completely self-contained, avoiding any dependency on services like a Hive Metastore, AWS Glue, or a REST catalog service.> **User:**
> are there any REST catalogues that would work on a developer machine, perhaps with sqlite backing?> **You:**
> Yes, absolutely. Moving from a simple filesystem catalog to a local REST catalog is an excellent step for a tech demo. It more closely mimics a production environment and provides a standardized API for table management, all while running self-contained on your developer machine.
> 
> 
> 
> There are several great options for running a lightweight Iceberg REST catalog locally, and many of them can indeed use SQLite as a backend to maintain that minimal-dependency footprint.
> 
> 
> 
> Here are a few of the best approaches:
> 
> 
> 
> ### 1. The Tabular Docker Image (Recommended)
> 
> 
> 
> This is often the quickest and most common way to get a local REST catalog running. The team at Tabular provides a reference implementation of the Iceberg REST catalog in a Docker image.
> 
> - **How it Works:** The image runs a server that exposes the standard Iceberg REST API endpoints. By default, it is configured to use a JDBC (Java Database Connectivity) catalog that is backed by an in-memory SQLite database, which is perfect for local testing and development.
> - **To run it, you simply need Docker installed and can execute the following command:**
> 
> ```Bash
> docker run -p 8181:8181 tabulario/iceberg-rest:latest
> ```
> This will start the REST catalog server and make it available on `http://localhost:8181` on your machine. Your C# application can then be configured to point to this URI to manage its tables.
> 
> 
> ### 2. Other Community REST Catalog Implementations
> 
> 
> 
> The Iceberg community has produced other REST catalog projects that are also suitable for local development with SQLite. For example, there are Python-based implementations that you can build and run via Docker.
> 
> - **How it Works:** These projects provide a Dockerfile that builds a lightweight web server implementing the REST catalog specification. The base versions of these images are often designed to use SQLite as the metadata backend.
> - **Example Usage:** You would typically clone the project's repository, build the Docker image, and then run the container, exposing the necessary port (e.g., `docker run -p 8000:8000 my-iceberg-rest-image`).
> 
> 
> ### 3. Standalone Toolsets like Altinity ice
> 
> 
> 
> If you prefer not to use Docker, some projects provide standalone command-line tools to run a catalog.
> 
> - **How it Works:** Altinity's `ice` is an open-source toolset that includes a command, `ice-rest-catalog`, which starts a local REST catalog. This implementation is a wrapper around the official Iceberg Java libraries, which are the reference implementation for the specification. This option requires a Java runtime (version 21 or greater) to be installed on your machine.
> 
> 
> ### An Alternative: PyIceberg's SQL Catalog (Non-REST)
> 
> 
> 
> If your primary goal for having a catalog is for the Python-based validation step, and you don't necessarily need a network-accessible REST _server_, you can use PyIceberg's built-in `SqlCatalog` directly.
> 
> - **How it Works:** This is not a REST catalog, but rather a direct implementation of the catalog logic within the PyIceberg library itself. It can be configured to use a local SQLite database file to store all table metadata.
> - **Configuration:** You would configure your Python validation script to load the catalog with properties pointing to a local SQLite file :   
> 
> ```Python
> from pyiceberg.catalog import load_catalog
> ```> ```
> warehouse_path = "/path/for/your/iceberg/data"
> catalog = load_catalog(
>     "local_sql",
>     **{
>         "type": "sql",
>         "uri": f"sqlite:///{warehouse_path}/catalog.db",
>         "warehouse": f"file://{warehouse_path}",
>     },
> )
> ```
> ```
> 
> 
> This approach is simpler if your only client is the Python script, but it doesn't provide a REST endpoint that your C# application or other tools could use.
> 
> 
> 
> For your tech demo, I would recommend starting with **Option 1 (the Tabular Docker image)**. It perfectly matches your request for a REST catalog with a SQLite backend, is extremely easy to start, and provides a realistic API endpoint for your C# application to interact with.> **User:**
> i agree, tabular image seems like the best fit. Can you now help me to create an implementation plan for this project?> **You:**
> Of course. Based on your goal and the decision to use the Tabular Docker image for the REST catalog, here is a comprehensive implementation plan for your tech demo.
> 
> 
> 
> This plan is broken down into distinct phases, guiding you from initial setup to final validation, ensuring each component is built correctly to produce a fully compliant Apache Iceberg table.
> 
> 
> 
> ### **Project Implementation Plan: C# SQL Server to Iceberg Table Generator**
> 
> 
> 
> **Objective:** Create a.NET console application that extracts data from a SQL Server query, formats it as an Apache Iceberg table with Parquet data files, and registers it using a local, containerized REST catalog.
> 
> 
> 
> ### **Phase 1: Environment Setup and Configuration**
> 
> 
> 
> **Goal:** Prepare your development machine with all necessary tools, start the local Iceberg catalog, and structure the C# project.
> 
> 1. **Install Prerequisites:**
> 
>     - **.NET SDK:** Ensure you have.NET 6 or later installed.
> 
>     - **Docker Desktop:** This is required to run the containerized REST catalog.
> 
>     - **IDE:** Visual Studio or VS Code with C# extensions.
> 2. **Launch the Iceberg REST Catalog:**
> 
>     - Open your terminal or command prompt.
> 
>     - Run the following Docker command to start the Tabular REST catalog container. This image conveniently uses an in-memory SQLite database, requiring no further setup.   
> 
> 
> 
> ```Bash
> 
> docker run -p 8181:8181 tabulario/iceberg-rest:latest
> 
> ```
> 
>     - **Verification:** The catalog is now running and accessible at `http://localhost:8181`. This endpoint will be the target for all metadata operations from your C# application.
> 3. **Create and Configure the C# Project:**
> 
>     - Create a new.NET Console Application.
> 
>     - **Add NuGet Packages:** Install the following essential libraries:
> 
> 
> 
>         - `Microsoft.Data.SqlClient`: For connecting to SQL Server.
> 
> 
> 
>         - `ParquetSharp`: For writing Parquet data files.
> 
> 
> 
>         - `Apache.Avro`: For creating Iceberg manifest files.
> 
> 
> 
>         - `System.Text.Json`: For serializing the `metadata.json` file.
> 
>     - **Configuration File:** Create an `appsettings.json` file in your project root to manage external settings. This avoids hard-coding values.
> 
> 
> 
> ```JSON
> 
> {
> 
>   "SqlServerConnectionString": "Server=YOUR_SERVER;Database=YOUR_DB;Trusted_Connection=True;TrustServerCertificate=true;",
> 
>   "TsqlQuery": "SELECT * FROM YourTable",
> 
>   "IcebergRestCatalogUri": "http://localhost:8181/v1/",
> 
>   "IcebergWarehousePath": "file:///C:/path/to/your/warehouse",
> 
>   "IcebergTableName": "my_db.my_new_table"
> 
> }
> 
> ```
> 
> 
> ### **Phase 2: High-Performance Data Extraction**
> 
> 
> 
> **Goal:** Implement a memory-efficient pipeline to stream data from SQL Server.
> 
> 1. **Establish Database Connection:** Use the connection string from `appsettings.json` to create a `SqlConnection`.
> 2. **Implement Streaming Read:**
> 
>     - Use the **`SqlDataReader`** for data extraction. This is critical as it operates as a forward-only stream, retrieving one row at a time and preventing the entire dataset from being loaded into memory. This approach is vastly more scalable than using a `DataSet` or an ORM like Entity Framework for bulk data movement.
> 
>     - Ensure all database objects (`SqlConnection`, `SqlCommand`, `SqlDataReader`) are wrapped in `using` statements to guarantee proper resource disposal.
> 3. **Dynamic Schema Inference:**
> 
>     - Before reading rows, use the `SqlDataReader.GetSchemaTable()` method. This returns a `DataTable` containing metadata about each column in the result set (name, data type, nullability), which is essential for the next phase.
> 
> 
> ### **Phase 3: Schema Translation (T-SQL to Iceberg)**
> 
> 
> 
> **Goal:** Convert the SQL schema into a valid Iceberg schema, paying close attention to Iceberg's specific requirements.
> 
> 1. **Define Data Type Mappings:** Create a utility function that maps `SqlDbType` values from the schema table to their corresponding Iceberg primitive types (e.g., `int` -> `int`, `bigint` -> `long`, `varchar` -> `string`, `datetime` -> `timestamp`, `decimal` -> `decimal(P,S)`).
> 2. **Assign Unique Field IDs:**
> 
>     - A core requirement of Iceberg is that every field in the schema must have a unique integer `field-id`. This ID persists across schema changes, enabling safe evolution.
> 
>     - For this initial table creation, iterate through the columns and assign a simple, monotonically increasing ID starting from 1.
> 3. **Construct Schema Objects:**
> 
>     - Create C# classes (POCOs) that model the Iceberg schema structure as defined in the specification (e.g., `IcebergSchema`, `NestedField`).
> 
>     - Populate these objects with the column name, the mapped Iceberg type, the assigned `field-id`, and the nullability information. This in-memory schema object will be used for both Parquet and metadata file generation.
> 
> 
> ### **Phase 4: Writing Iceberg-Compliant Parquet Files**
> 
> 
> 
> **Goal:** Write the data stream to Parquet files that include the Iceberg-specific `field-id` metadata.
> 
> 1. **Use `ParquetSharp`'s Low-Level API:**
> 
>     - The high-level API of `ParquetSharp` does not support adding the custom `field-id` metadata required by Iceberg.
> 
>     - You must use the lower-level **`GroupNode`** API to manually construct the Parquet file's schema. This gives you the fine-grained control needed to inject the `field-id` for each column, as successfully demonstrated in the inspirational blog post.
> 2. **Implement Streaming Write:**
> 
>     - Create a `ParquetFileWriter` using the custom `GroupNode` schema.
> 
>     - Inside the `while (reader.Read())` loop from Phase 2, write data in batches (known as "row groups" in Parquet).
> 
>     - For each batch, get a `RowGroupWriter`, then get a `ColumnWriter` for each column, and write the values from the `SqlDataReader`. This ensures data flows directly from the database to the file without being buffered in memory.
> 
> 
> ### **Phase 5: Metadata Generation and Catalog Commit**
> 
> 
> 
> **Goal:** Generate the hierarchical metadata files (manifests and table metadata) and perform an atomic commit via the REST catalog.
> 
> 1. **Generate Avro Manifest Files:**
> 
>     - **Implement the Schema Interception Fix:** The standard `Apache.Avro` library silently strips the required `field-id` attributes when writing Avro file headers. You must implement the workaround detailed in the blog post: create `AvroSchemaWrapper` and `AvroDatumWriterWrapper<T>` classes. These wrappers will intercept the schema serialization call and provide the complete, compliant Iceberg Avro schema as a raw string, ensuring the `field-id`s are preserved.
> 
>     - **Populate Manifests:** After each Parquet file is written, record its metadata (path, file size, record count, column-level stats) as an entry in a manifest file (`.avro`).
> 
>     - **Populate Manifest List:** Once all data is written, create a single manifest list file (`snap-....avro`) that points to the manifest file(s) you just created. This file represents the table's snapshot.
> 2. **Create Initial Table via REST Catalog:**
> 
>     - Before writing any data, your application must first register the new table with the catalog.
> 
>     - Send an HTTP `POST` request to the catalog's `v1/namespaces/{namespace}/tables` endpoint. The request body will be a JSON object containing the table name and the Iceberg schema you constructed in Phase 3.
> 
>     - The REST catalog will create the table and return the initial `v1.metadata.json` file content.
> 3. **Commit the New Snapshot:**
> 
>     - After all Parquet data files and Avro metadata files are written to their final locations in the warehouse, construct the new table metadata JSON (`v2.metadata.json`). This file will contain a new snapshot pointing to your newly created manifest list.
> 
>     - Perform the final atomic commit by sending an HTTP `POST` request to the catalog's `v1/tables/{namespace}/{table}` endpoint. The body of this request will contain the path to the previous metadata file (`v1.metadata.json`) and the content of the new metadata file (`v2.metadata.json`).
> 
>     - The REST catalog server is responsible for performing the atomic swap, guaranteeing that the table state transitions cleanly from the old version to the new one.
> 
> 
> ### **Phase 6: Cross-Ecosystem Validation**
> 
> 
> 
> **Goal:** Prove the tech demo's success by verifying that the generated table is fully readable and compliant using a standard, non-.NET tool.
> 
> 1. **Tool of Choice: `PyIceberg`**
> 
>     - `PyIceberg` is an excellent choice for validation as it is a pure Python implementation of the Iceberg specification.
> 2. **Validation Steps:**
> 
>     - **Install PyIceberg:**
> 
> 
> 
> ```Bash
> 
> pip install "pyiceberg[pyarrow,rest]"
> 
> ```
> 
>     - **Create a Python Validation Script:** Use the following script to connect to the local REST catalog, load the table, and inspect its contents.
> 
> 
> 
> ```Python
> 
> from pyiceberg.catalog import load_catalog
> 
> ```> ```
> # Configure catalog to connect to the local REST server
>     catalog = load_catalog(
>         "local_rest",
>         **{
>             "type": "rest",
>             "uri": "http://localhost:8181",
>         }
>     )
> ```> ```
> try:
>         # 1. Load the table (verifies catalog registration and metadata parsing)
>         table_identifier = "my_db.my_new_table" # Must match your config
>         table = catalog.load_table(table_identifier)
>         print(f"Successfully loaded table: {table_identifier}")
> ```> ```
> # 2. Validate the schema (verifies field-ids are present)
>         print("\nTable Schema:")
>         print(table.schema())
> ```> ```
> # 3. Query the data and verify row count
>         df = table.scan().to_pandas()
>         print(f"\nSuccessfully read {len(df)} rows.")
>         print("\nData Preview:")
>         print(df.head())
> ```> ```
> except Exception as e:
>         print(f"Validation failed: {e}")
>     ```
>   * **Execute and Verify:** Run the script. A successful output, showing the correct schema (with `field-id`s) and the expected data, confirms that your C\# application has correctly implemented the entire Iceberg specification. You can also use DuckDB with the Iceberg extension for an alternative SQL-based validation.
> ```> **User:**
> how can a save this full  conversation to a single file?> **You:**
> Of course. Here is the complete record of our conversation and all the research, formatted as a single Markdown file.
> 
> 
> 
> You can copy the entire content below and save it as a file with a `.md` extension (for example, `iceberg_project.md`) to keep a complete record.

___
###### [top](#table-of-contents)

