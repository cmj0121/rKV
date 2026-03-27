# CLI REPL — v1 Implementation

This documents the v1 subset of the full REPL spec (see `14-cli-repl.md`).
Deferred features: projection, pipe, batch, temporal commands, spatial queries.

## Prompt

```text
knot>                    # no namespace selected
knot [campus]>           # namespace selected
```

## v1 Command Summary

### Text commands (schema + navigation)

```text
USE campus
NAMESPACES
CREATE NAMESPACE campus
DROP NAMESPACE campus
TABLES
LINKS
CREATE TABLE person
CREATE TABLE person IF NOT EXISTS
DROP TABLE person
CREATE LINK attends person -> school
CREATE LINK friends person -> person BIDI
CREATE LINK attends person -> school CASCADE
ALTER LINK attends CASCADE
ALTER LINK attends BIDI
DROP LINK attends
help
exit
```

### Expression commands (data operations)

```text
+{person alice}[role=teacher, age=45]    # insert with props
+{person alice}                          # insert set mode
-{person alice}                          # delete
-!{person alice}                         # cascade delete
?{person alice}                          # get by key
?{person}                                # scan all
?{person | role=teacher}                 # query with filter
?{person | role=teacher, age>30}         # AND filter
?{person | role=teacher OR dept=eng}     # OR filter
?:10{person}                             # limit
?:10+5{person}                           # limit + offset
?{person}[age:asc]                       # sort
+(attends alice -> mit)[year=2020]       # insert link
+(attends alice -> mit)                  # bare link
+(friends alice <-> bob)                 # BIDI link
-(attends alice -> mit)                  # delete link
-!(attends alice -> mit)                 # cascade delete link
?{person alice} -> (attends)             # traverse 1 hop
?{person alice} -> (attends) -> (loc)    # traverse multi-hop
?{person alice} -> (*:3)                 # discovery
```

## Parser Design

The REPL parser dispatches on the first character(s):

| First chars | Parse as                         |
| ----------- | -------------------------------- |
| `?`         | Query/traverse expression        |
| `+`         | Insert expression                |
| `-!`        | Cascade delete expression        |
| `-`         | Delete expression                |
| letter      | Text command (schema/navigation) |

### Text command parsing

Split on whitespace, match first token (case-insensitive):

- `USE` → namespace selection
- `NAMESPACES` → list namespaces
- `CREATE` → next token: `TABLE`, `LINK`, `NAMESPACE`
- `DROP` → next token: `TABLE`, `LINK`, `NAMESPACE`
- `ALTER` → next token: `LINK`
- `TABLES` → list tables
- `LINKS` → list links
- `help` → show quick reference
- `exit` / `quit` → exit REPL

### Expression parsing

1. Parse prefix: `?[:N[+M]]`, `+`, `-`, `-!`
2. Parse bracket: `{` → node expression, `(` → link expression
3. Inside `{}`: `table [key] [| conditions]`
4. Inside `()`: `link from ->|<-> to`
5. After `{}`: optional `[sort]` (on query) or `[props]` (on insert)
6. After expression: optional `-> (link)` chain (traversal)

### Condition parsing (inside `|`)

```text
field=value          # equal
field!=value         # not equal
field>value          # greater
field>=value         # greater or equal
field<value          # less
field<=value         # less or equal
cond, cond           # AND (comma)
cond AND cond        # AND (keyword)
cond OR cond         # OR
NOT cond             # NOT
```

Values are auto-typed:

- Quoted `"..."` → string
- Unquoted number → integer or float
- `true` / `false` → boolean
- Unquoted text → string

## Output Format

### Node output

```text
knot [campus]> ?{person alice}
alice
  role = teacher
  age  = 45

knot [campus]> ?{person}
alice  {role: teacher, age: 45}
bob    {role: student, age: 22}
(2 nodes)
```

### Link output

```text
knot [campus]> +(attends alice -> mit)[year=2020]
OK

knot [campus]> ?{person alice} -> (attends)
alice -> mit
(1 result)
```

### Error output

```text
knot [campus]> ?{nobody alice}
ERROR: table "nobody" does not exist

knot [campus]> -!{person alice}
WARNING: cascade deleted 3 nodes
```

## Implementation Plan

| #   | Commit                | Description                                         |
| --- | --------------------- | --------------------------------------------------- |
| 1   | REPL scaffold         | Binary entry point, rustyline, prompt, command loop |
| 2   | Text commands         | USE, NAMESPACES, CREATE/DROP TABLE/LINK/NAMESPACE   |
| 3   | Expression tokenizer  | Tokenize prefixes, brackets, conditions             |
| 4   | Insert expressions    | `+{...}[...]` and `+(...)` parsing + execution      |
| 5   | Query expressions     | `?{...}` with filters, sort, limit                  |
| 6   | Delete expressions    | `-{...}` and `-!{...}`                              |
| 7   | Traversal expressions | `-> (link)` chains and `(*:N)` discovery            |
| 8   | Integration tests     | End-to-end REPL command tests                       |
