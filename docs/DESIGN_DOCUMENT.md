# Design Document

## Usage

This application will be a compiled JAR, which you run with `java` and pass local YAML configs to.

```bash
java app.jar --config ./yaml/*.yaml
```

## Control Flow

### 1. Process Config YAML

Instantiate Scala classes for each of the YAML resources:
- `Config`
- `MetadataApplicator`
- `DataSource`:
    - `HDFSLikeDataSource`
    - `IncrementalDataSource`
    - `JDBCDataSource`
    - `VirtualDataSource`
- `DataRelationship`
- `OutputDataset`

### 2. Validate Resources

> some validation happens lazily, and some is preformed eagerly to detect misconfigurations early

__Config:__
- EAGER:
   - ensure that the Spark configurations are valid
- LAZY:
   - N/A
   
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

__IncrementalDataSource:__
- EAGER:
   - ensure `dataQuery` contains at least one `{{ SNAPSHOT | XXXX }}` substitution value
- LAZY:
   - ensure output of `snapshotQuery` has two columns: `interval_start`, `interval_end`
   
__JDBCDataSource__
- EAGER:
   - ensure `driverClass` is present on classpath
- LAZY:
   - ensure `snapshotConfig` is compatible with the data

__VirtualDataSource:__
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
   - ensure each `fieldName` is unique 
   - ensure `metadataUri` is NOT a subfolder of `dataUri`
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
1. Calculate the set of "unweighted minimum spanning trees" over the DataModel graph
   - If multiple trees are returned:
      1. Calculate an anti-priority score for each tree, by summing together the anti-priorities of each relationship present in the graph
      1. Return the tree with the lowest anti-priority score
         - If there is still a tie, the job must fail
1. Validate the returned tree:
   - Ensure `OutputDataset().spec.dataModel.maxDepth` is not exceeded
   - Ensure previous trees are not "structurally incompatible":
     1. the root node must not change `fieldName`
     1. every node must have the same `groupBy` configs (across all previous runs)
         - using `fieldName` to uniquely identity nodes (across all history)
     1. the old graph must be an "induced subgraph" of the "disjoint union" of previous trees
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
1. Execute JoinPlan for the snapshot-time, with the general join procedure being:
    - at each data source juncture, join all tables to the node closest to the root 
      (order doesn't matter, because left joins are commutative)
    - collect all columns into a STRUCT per DataSource (or if `groupBy` into ARRAY<STRUCT>)
    - after each join, ensure no rows have been gained:
       - left-joins should NEVER change the number of rows, if the relationship is "one to at-most-one"
       - use `spark.addSparkListener()` to listen to `SparkListenerStageCompleted` or `SparkListenerTaskEnd` events for rows-in and rows-out
       - "ERROR: `DataSource/X` --> `DataRelationship/Z` --> `DataSource/Y` is one-to-many: the number of rows increased from `N` to `M`" 
1. Write data to a new snapshot partition of `dataUri`
1. Write metadata to `metadataUri`
1. Release lock on `dataUri` and `metadataUri` folders

__NOTE:__
- We should not create output-snapshots which would be immediately deleted by `OutputDataset().spec.output.snapshot.autoDelete`
- Rather than storing the snapshot column in the DataFrames we pass around, we should define a SnapshotDataFrame, which represents a snapshot of the data, and remove any snapshot columns from the data itself

## OTHER NOTES

- We can use https://circe.github.io/circe/ for marshaling configs into JVM classes:
  - Conversion from YAML to Circle JSON can be done with: https://github.com/circe/circe-yaml

- We can use https://jgrapht.org/ as our graph library

- Should we alert users to `null` in their join columns?
  - Or is our `rows_in == rows_out` check sufficient to detect most issues?
  - How do we even detect this in an efficient way?

- We should make a tool to visualise DataModel graph from the metadata

- We should store the schema over time using some sort of GIT-like DIFF approach:
  - schema includes metadata from `DataSource().spec.metadataApplicators`
  - expose a reader (e.g. Python Library) with the ability to query:
    1. the schema at any specific snapshot
    2. the merged schema across a range of snapshots (if merge is not possible, reader should error, e.g. string --> int)

- Should we let users hint DataFrame caching/checkpointing?
   - This would let users truncate the logical plan if it gets too complex 
   - To use this, we need to let the user specify a checkpoint URI

- What are the implications of setting `catchupOrder` to `NEWEST_FIRST`, with regards to schema changes over time
   - Is it possible for our git-schema approach to move 'commits' to the correct snapshot-time 
     (where the schema actually changed)
     
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
