# Design Document

## Usage

This application will be a compiled JAR, which you run with `spark-submit` and pass local YAML configs to.

```bash
spark-submit --XXXXX xxxxx app.jar --config ./yaml/*.yaml
```

## Control Flow

### 1. Process Config YAML

Instantiate Scala classes for each of the YAML resources:
- `MetadataApplicator`
- `DataSource`:
    - `HDFSLikeDataSource`
    - `JDBCDataSource`
    - `SimpleVirtualDataSource`
    - `ComplexVirtualDataSource`
- `DataRelationship`
- `OutputDataset`

### 2. Validate Resources

> NOTE: some validation happens lazily, and some is preformed eagerly to detect misconfigurations early
   
__MetadataApplicator:__
- EAGER:
   - N/A
- LAZY:
   - N/A

__HDFSLikeDataSource:__
- EAGER:
   - ensure `sourceUris` and `sourceUriPrefix` are compatible
- LAZY:
   - ensure `snapshotConfig` is compatible with the data

__JDBCDataSource__
- EAGER:
   - ensure `driverClass` is present on classpath
- LAZY:
   - ensure `snapshotConfig` is compatible with the data

__SimpleVirtualDataSource:__
- EAGER:
   - ensure `dataQuery` contains at least one `{{ SNAPSHOT | XXXX }}` substitution value
- LAZY:
   - ensure output of `snapshotQuery` has two columns: `interval_start`, `interval_end`

__ComplexVirtualDataSource:__
- EAGER:
   - ensure `dataQuery` only has basic SELECT/WHERE/GROUP-BY clauses (no JOIN, etc)
   - ensure `dataSource` is not a VirtualDataSource
- LAZY:
   - N/A
 
__DataRelationship:__
- EAGER:
   - ensure DataSources only appear once in `equivalentFields`
   - ensure `fields` have the same number of elements
   - ensure `castType` is equal for `fields[]` at the same array index
- LAZY:
   - ensure `fields[]` is compatible with the data

__OutputDataset:__
- EAGER:
   - ensure each `otherDataSources[].fieldName` is unique (also, `output.snapshot.hivePartition.fieldName`)
   - ensure `metadataUri` is NOT a subfolder of `dataUri`
   - ensure configs have not changed (if output folder already exists)
      - `otherDataSources[].transformation.groupBy`
      - `output.format`
      - `output.compression`
- LAZY:
   - ensure the required join fields are actually present in the DataSources data
   - ensure that the output URIs are either empty, or look like folders this app has created

### 3. Process OutputDatasets

> for each `OutputDataset` resource, do the following

#### 3.1. Define a DataModel Graph

__Definition:__
- NODES --> DataSources
- EDGES --> DataRelationships

__Note:__
- At this stage, there can be multiple relationships between each pair of NODES, as multiple columns could be related in each table pair

#### 3.2. Calculate the JoinPlan over the DataModel Graph

__Procedure:__
1. Prune invalid EDGES:
   - For NODES with `groupBy` defined, remove EDGES which don't use the `groupBy.fieldNames` (Note, treat `groupBy.fieldNames` a compound-key, not multiple options)
      - This is required, because all other fields will end up in an ARRAY<STRUCT> (with potentially multiple values)
1. Calculate the set of "unweighted minimum spanning trees" over the DataModel graph
1. If multiple trees are returned:
      1. Calculate an anti-priority score for each tree, by summing together the anti-priorities of each relationship present in the graph
      1. Return the tree with the lowest anti-priority score
         - If there is still a tie, the job must fail
1. Calculate the JoinPlan:
   1. Number the tree's nodes using breadth-first-search
   1. The JoinPlan is to start at the highest numbered node, and join to the lowest-numbered adjacent node
       - note, there will only be a single relationship between each node (as this is a minimum spanning tree)

__Note:__
- The calculated JoinPlan is completely independent of which snapshots are available in the data, as we only consider the DataModel graph structure

#### 3.3. Calculate the list of candidate snapshot-times

__Procedure:__
1. for each interval of time, find a candidate snapshot of the root DataSource:
    - don't waste processing time looking at intervals which already have a snapshot
    - use `snapshotFilters.inverval.prefrence` if multiple snapshots exist in the same interval
1. for each candidate root snapshot:
    1. traversing nodes in breadth-first-search order, check if a snapshot exists between `snapshot.timeBefore`, the root snapshot-time, and `snapshot.timeAfter`
    1. apply the behaviour specified by `snapshot.missingMode`, if no valid snapshot exists for a node

#### 3.4. Create new snapshots for the OutputDataset

> for each new snapshot-time

__Procedure:__
1. Open lock on `dataUri` and `metadataUri` folders
1. Execute JoinPlan for the snapshot-time:
    1. select the highest-number node
    1. let the lowest-numbered adjacent node be called the `junction-node`
    1. left-join all higher-numbered adjacent nodes to the `junction-node`, let the output be called `junction-node-joined`
        - use `SparkListener` to count the number of rows in (left side), and rows out, and ensure it is the same (fail if not)
        - NOTE: join order doesn't matter, because left joins are commutative (under one-to-at-most-one relationships, which is guaranteed by our row count check)
    1. collect columns from `junction-node-joined` into STRUCTS, let the output be called `junction-node-grouped`:
        - columns from `junction-node` go into a STRUCT named "data"
        - columns from other DataSources go into STRUCTS named from their `fieldName` configs
    1. if `junction-node` has `groupBy.fieldNames` defined, preform a GROUP BY and COLLECT_LIST as the STRUCTS are created:
        - the `groupBy.fieldNames` should be combined into a STRUCT/TUPLE field called "key" during the GROUP BY
        - NOTE: "key" will be a top-level field in `junction-node-grouped`, and MUST ALWAYS be the join column (enforced by checks when calculating the JoinPlan)
    1. Repeat until all tables have been joined
1. Validate the schema that would be created:
   - ensure `OutputDataset().spec.dataModel.maxDepth` is not exceeded
   - ensure "structural compatibility":
      1. any field type changes must be mergeable
      1. every node must have the same `groupBy` configs (across all previous runs)
          - using `fieldName` to uniquely identity nodes (across all history)
      1. ????? the old "minimum spanning tree" (less any previously unseen NODES/EDGES) must be an "induced subgraph" of the "disjoint union" of previous "minimum spanning trees"
          - NOTE: this is to ensure that DataSources don't move around, that is, once we see a `fieldName` in one part of the graph, we should never see it anywhere else
1. Write data to a new snapshot partition of `dataUri`
1. Write metadata to `metadataUri`
1. Release lock on `dataUri` and `metadataUri` folders

__NOTE:__
- We should not create output-snapshots which would be immediately deleted by `OutputDataset().spec.output.snapshot.autoDelete`
- Rather than storing the snapshot column in the DataFrames we pass around, we should define a SnapshotDataFrame, which represents a snapshot of the data, and remove any snapshot columns from the data itself

## OTHER NOTES

- ?? should column schema be stripped out from the written parquet files?
   - ?? how should we store metadata, should it be separate from the schema itself 
     (to reduce the number of unique schemas)

- inside `OutputDataset.spec.dataModel.rootDataSource.snapshotFilter.fromInterval`, we should have a max-snapshot-age config
   - this would be the time from present after which we would not look at that interval
      - helps with computational complexity of checking if snapshot exists
      - is different from `spec.outputFolder.snapshotAutoDelete` because we don't retroactively delete snapshots which we wouldn't check today

- Support keeping less frequent snapshots for times longer in the past (think like tiered database backups)
   - This config would be in OutputDataset().spec.outputFolder.snapshotAutoDelete

- Setup separate logger to spark, to highlight non-spark logs.

- Treat all `fieldName` provided by the user case-insensitively 
  (however, ensure the output tables have the same case as what the user provided)

- We should implement a "FieldTransformer" Resource:
   - FieldTransformer are used in DataSources, like MetadataApplicators:
      - specify input fields (BY: name-regex, or metadata)
         - may return one or more fields for each input
      - specify if input fields are replaced by the output of the transformer 
         - ?? or is this just a hard-coded attribute of the transfomer itself
      - specify snapshot ranges
   - What's difference between FieldTransformer and SimpleVirtualDataSources?
      - ?? should we just allow non-sql in virtual data sources (e.g. a sequence FieldTransformer)
      - transformers are for common things, to allow less repetition of code
      - transformers are more aware of snapshots (limiting to specific snapshots is first-class support)
   - What are the first FieldTransformer
      - Rename Columns
         - deal with cases where columns have changed name (even for join keys)
         - !! must not create two columns of the same name (for a specific snapshot) (but this should be checked for all transformers)
         - ?? do we allow inner struct fields to be renamed 
      - Cast DataTypes    
      - transform data 

- Should we let users hot-swap DataSources in their OutputDatasets?
   - the main question is how do we identify a DataSource in the data model, 
     are we doing it by its fieldName, or its DataSource reference
   - this is important because:
      - people might need to swap to a VirtualDataSource to apply fixes after there are already some output snapshots
      - we don't let DataSources move around in the DataModel graph structure between snapshots

- We need to support more generic intervals for `OutputDataset.spec.dataModel.snapshotFilter.interval`, for example hourly, but not between the hours of 00-05.
   - This is done in Airflow using cron expressions, we should do the same.

- We should consider upstremaing a hint that "join order doesnt matter" into core spark

- We should enable `spark.sql.cbo.enabled` (if it doesn't break our join process, e.g. row-counts)

- We should catch SIGTERM and release the locks on the files.

- Should we alert users to `null` in their join columns?
  - Or is our `rows_in == rows_out` check sufficient to detect most issues?
  - How do we even detect this in an efficient way?

- We should make a tool to visualise DataModel graph from the metadata

- Should we let users hint DataFrame caching/checkpointing?
   - This would let users truncate the logical plan if it gets too complex 
   - To use this, we need to let the user specify a checkpoint URI

- What are the implications of setting `catchupOrder` to `NEWEST_FIRST`, with regards to storing schema changes over time?
     
- Here is an example DataModel (stripped down): https://csacademy.com/app/graph_editor/
    ```text
    !customer! customer_group
    !customer! customer_address
    !customer! customer_contact
    
    customer_contact customer_address
    
    customer_group customer_contact
    customer_group customer_address
    
    customer_group group_attributes
    ```
