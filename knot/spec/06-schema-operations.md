# Schema Operations

All schema operations modify metadata and the corresponding rKV namespaces.

## Create table

1. Validate name (UTF-8, max 511 bytes, no control characters, no dots)
2. Check metadata — error if table already exists (unless `IF NOT EXISTS`)
3. Write `table:{name}` to metadata namespace
4. Create rKV namespace `knot.{ns}.t.{name}` (empty)

## Create link table

1. Validate name, source table, and target table
2. Verify source and target tables exist in metadata — error if not
3. Check metadata — error if link table already exists (unless `IF NOT EXISTS`)
4. Write `link:{name}` to metadata with source, target, bidirectional, cascade flags
5. Create forward namespace `knot.{ns}.l.{name}` (empty)
6. Create reverse namespace `knot.{ns}.r.{name}` (empty)

## Create index

1. Validate table (or link table) and field names exist in metadata
2. Check metadata — error if index already exists
3. Write index definition to metadata (`ddx:`, `ldx:`, or `sdx:` prefix)
4. Create index namespace
5. Backfill: scan all existing entries and populate the index

## Drop table

Dropping a table removes everything associated with it:

1. Query metadata for link tables where source or target = this table
2. Drop each referencing link table (see Drop link table)
3. Drop all data indexes (`ddx.{table}.*`)
4. Drop all spatial indexes (`sdx.{table}.*`)
5. Remove data table namespace (`t.{table}`)
6. Remove metadata entry (`table:{name}`)

Dropping a table **always** cascades to link tables — there is no reject option.

## Drop link table

1. Remove forward namespace (`l.{name}`)
2. Remove reverse namespace (`r.{name}`)
3. Remove all link indexes (`ldx.{name}.*`)
4. Remove metadata entries (`link:{name}` and any `ldx:{name}:*`)

Data tables on both sides are unaffected.

## Drop index

1. Remove the index namespace
2. Remove the metadata entry

Data is unaffected — only the index is removed.

## Drop namespace

1. List all tables and link tables in metadata
2. Drop each table (which cascades to link tables and indexes)
3. Remove the metadata namespace itself

## Alter link table

Link table properties that can be changed after creation:

| Property      | Effect                                                   |
| ------------- | -------------------------------------------------------- |
| bidirectional | Changes whether reverse index is queryable for traversal |
| cascade       | Changes whether node deletion cascades through this link |

Altering these properties takes effect immediately for new operations.
Existing data is not modified.

## IF NOT EXISTS

Create operations for tables, link tables, and indexes support an `IF NOT EXISTS`
flag. When set, the operation succeeds silently if the entity already exists
instead of returning an error.
