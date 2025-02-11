# TODO

## one operation -> one index

> high priority

1. store index into **operation** table
2. remove `index` parameters in whole API
3. use `operation_id` instead, to enforce ACL check on `token` vs `operation` object.

## query

1. > high priority: remove `query_gulp` and `flt` everywhere: `query_raw` should be used instead
2. remove/simplify `q_options`
3. remove sigma on `external query`

## stored query

- same cleanup as above
- review `stored_query` API: should it be used for `raw query` only or also sigma ?
- should it include support for external query (actually it does, but imho should be removed)
- create `stored_sigma` collab object ?
- implement in the UI

## data normalization

- > medium priority: data normalization across all plugins (remove unneeded fields)
- > high priority: better ws messages, to handle most popular use cases of request.
Example:
```
# When new source/file do this instead of query_operation and other sync requests 
type: "new_source"
data: GulpSourceObject
# When new note/link, do this instead of note_list/link_list
type: "new_note"
data: GulpNoteObject
```

## generic

- improve `datastream_get_mapping_by_src`: use ws to send fields during the query, so the UI do not block (annoying as the dataset grows)

