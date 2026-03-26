# CLI REPL

Interactive shell for Knot. Schema operations use text commands; data operations
use symbol-based expressions. All keywords are case-insensitive (documented in
UPPERCASE by convention).

## Quick reference

```text
  Query:    ?{table | condition}           ?{person | age>30}
  Get:      ?{table key}                  ?{person alice}
  Limit:    ?:N{...}                      ?:10{person | role=teacher}
  Page:     ?:N+M{...}                    ?:10+20{person | role=teacher}
  Insert:   +{table key}[props]           +{person alice}[role=teacher]
  Delete:   -{table key}                  -{person alice}
  Cascade:  -!{table key}                 -!{person alice}
  Link:     +(link from -> to)[props]     +(attends alice -> mit)[year=2020]
  Unlink:   -(link from -> to)            -(attends alice -> mit)
  BIDI:     <-> instead of ->             +(friends alice <-> bob)
  Traverse: ?{source} -> (link) -> ...    ?{person alice} -> (attends)
  Discover: ?{source} ->|<-> (*:N)        ?{person alice} -> (*:3)
  Filter:   | cond AND/OR/NOT cond        ?{person | role=teacher, age>30}
  Sort:     [field:asc|desc, ...]         ?{person}[age:asc, name:desc]
  Project:  => {}[field=source.field]     => {}[name=person.name]
  Pipe:     => {}[...] | +{table key=x}  => {}[name=person.name] | +{out key=name}
  Schema:   CREATE|DROP|ALTER ...          CREATE TABLE person
  Batch:    batch → expressions           batch → +{...}[...] | -{...} | commit
```

## Prompt

```text
knot>                    # no namespace selected
knot [campus]>           # namespace selected
```

## Namespace commands

```text
USE campus               # select namespace
NAMESPACES               # list all namespaces
CREATE NAMESPACE campus  # create namespace
DROP NAMESPACE campus    # drop namespace
```

## Schema commands

```text
TABLES / LINKS / INDEXES             # list in current namespace
CREATE TABLE person
CREATE TABLE person IF NOT EXISTS     # idempotent
CREATE LINK attends person -> school
CREATE LINK friends person -> person BIDI
CREATE LINK attends person -> school CASCADE
ALTER LINK attends CASCADE            # change cascade flag
ALTER LINK attends BIDI               # change direction
DROP TABLE person / DROP LINK attends
CREATE INDEX person.age               # single-field
CREATE INDEX person.age,dept          # composite
CREATE INDEX person.age SPARSE
CREATE LINDEX attends.year            # link index
CREATE SINDEX school.location         # spatial index
DROP INDEX person.age / DROP LINDEX attends.year / DROP SINDEX school.location
```

## Expression language

Every data expression has a prefix that determines the action:

| Prefix  | Meaning                      | Applies to    |
| ------- | ---------------------------- | ------------- |
| `?`     | query                        | `{}` and `()` |
| `?:N`   | query with limit N           | `{}` and `()` |
| `?:N+M` | query with limit N, offset M | `{}` and `()` |
| `+`     | insert/upsert                | `{}` and `()` |
| `-`     | delete                       | `{}` and `()` |
| `-!`    | cascade delete               | `{}` and `()` |

Brackets: `{}` for nodes, `()` for links, `[]` for sort (on query) or
properties (on insert), `->` for directional, `<->` for bidirectional.

### Node operations

```text
?{person}                                 # query: all nodes
?{person alice}                           # query: by key
?{person | role=teacher}                  # query: filter
?:10{person | role=teacher}               # query: first 10
?:10+20{person | role=teacher}            # query: skip 20, take 10
+{person alice}[role=teacher, dept=eng]   # insert/replace
+{person charlie}                         # insert set mode
-{person alice}                           # delete
-!{person alice}                          # cascade delete
```

### Link operations

```text
+(attends alice -> mit)[year=2020]        # create directional link
+(attends alice -> mit)                   # bare directional link
+(friends alice <-> bob)                  # create BIDI link
-(attends alice -> mit)                   # delete link
-!(attends alice -> mit)                  # cascade delete link
-(friends alice <-> bob)                  # delete BIDI link
```

### Filter conditions

| Operator          | Example                                 | Description               |
| ----------------- | --------------------------------------- | ------------------------- |
| `=`               | `age=30`                                | Equals                    |
| `!=`              | `age!=30`                               | Not equals                |
| `>` `>=` `<` `<=` | `age>30`                                | Comparison                |
| `,` or `AND`      | `role=teacher, age>30`                  | AND (comma shorthand)     |
| `OR`              | `role=teacher OR dept=eng`              | OR (AND binds tighter)    |
| `NOT`             | `NOT role=teacher`                      | Negate                    |
| `()`              | `(role=teacher OR dept=eng) AND age>30` | Grouping                  |
| `LIKE`            | `name LIKE "ali%"`                      | String match (% wildcard) |
| `IN`              | `role IN [teacher, admin]`              | Match against list        |
| `=null`           | `dept=null`                             | Property missing/null     |
| `!=null`          | `dept!=null`                            | Property exists           |
| `NEAR`            | `location NEAR (42.3,-71.1) 10km`       | Geo: within distance      |
| `WITHIN`          | `location WITHIN (40,-72, 43,-70)`      | Geo: bounding box         |

### Sort, limit, projection

```text
?{person}[age:asc]                                  # sort ascending
?{person}[dept:asc, age:desc]                       # multiple sort
?:10+5{person | role=teacher}[age:asc]              # limit + offset + sort
?{person | role=teacher} => {}[name=person.name]    # projection
?{person} => {row}[name=person.name, age=person.age]  # named shape
?{person} => {}[name=person.name] | +{out key=name}   # pipe to insert
```

`[]` meaning depends on context: after `?` = sort, after `+` = properties,
after `=>` = field mapping. The `?` prefix applies to the entire expression.
Destination node sets in traversal inherit the query context — do not prefix
them separately.

## Traversal

```text
# Directed
?{person alice} -> (attends)                        # one hop
?{person alice} -> (attends) -> (located-in)        # multi-hop
?{person alice} -> (attends | year>2019)            # link filter
?{person alice} -> (attends) -> {school | ranking<50}  # node filter

# Bidirectional
?{person alice} <-> (friends)                       # BIDI hop
?{person alice} <-> (friends) -> (attends)          # mixed

# Discovery
?{person alice} -> (*:3)                            # forward, max 3
?{person alice} <-> (*:3)                           # both directions

# With sort and limit
?:5{person alice} -> (attends) -> {school}[ranking:asc]

# Full paths
?{person alice} -> (attends) -> (located-in) PATHS
```

## Temporal commands

```text
# Point-in-time
?{person alice} AT 2024-01-01                       # node at timestamp
?{person alice} -> (attends) AT 2024-01-01          # temporal traversal

# History
HISTORY person alice                                # list revisions
HISTORY person alice FROM 2024-01-01 TO 2024-06-01  # revision range

# Compaction
COMPACT BEFORE 2024-01-01                           # remove old revisions
```

## Batch mode

```text
batch                                    # enter batch sub-REPL
  +{person alice}[role=teacher]          # queue insert
  +(attends alice -> mit)[year=2020]     # queue link
  -{person bob}                          # queue delete
  -!(attends bob -> mit)                 # queue cascade unlink
  show                                   # list queued ops
  commit                                 # apply all in order
  abort                                  # discard and exit
```

## Error display

Errors are printed with an `ERROR:` prefix. Cascade operations print a
`WARNING:` with the count of affected nodes.

```text
knot [campus]> ?{person alice}
ERROR: table "person" does not exist

knot [campus]> -!{person alice}
WARNING: cascade deleted 47 nodes
```

## Grammar summary

```text
Query:    ?[:N[+M]]{table [key] [| cond]}[sort]
            [=> {}[field=src.field]]
            [| +{table key=field}]
Traverse: ?[:N[+M]]{source} ->|<-> (link [| cond]) [->|<-> ...][sort]
          ?[:N[+M]]{source} ->|<-> (*:N)
Insert:   +{table key}[properties]
Delete:   -{table key}
Cascade:  -!{table key}
Link:     +(link from ->|<-> to)[properties]
Unlink:   -(link from ->|<-> to)
Cascade:  -!(link from ->|<-> to)
Temporal: ?{...} AT timestamp
          HISTORY table key [FROM ts TO ts]
          COMPACT BEFORE timestamp
Schema:   CREATE|DROP TABLE|LINK|INDEX|LINDEX|SINDEX|NAMESPACE ...
Alter:    ALTER LINK name CASCADE | BIDI
List:     TABLES | LINKS | INDEXES | NAMESPACES
Navigate: USE namespace
Batch:    batch → (+{}[] | -{} | -!{} | +()[] | -() | -!() | show | commit | abort)
```
