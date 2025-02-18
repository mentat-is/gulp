# TODO

## one operation -> one index

> high priority

~~1. store index into **operation** table~~ *DONE on gulp-1.0-rc*
~~2. remove `index` parameters in whole API~~ *DONE on gulp-1.0-rc*
~~3. use `operation_id` instead, to enforce ACL check on `token` vs `operation` object.~~ *DONE on gulp-1.0-rc*

## query

~~1. > high priority: remove `query_gulp` and `flt` everywhere: `query_raw` should be used instead~~ *DONE on gulp-1.0-rc*
~~2. remove/simplify `q_options`~~ *DONE on gulp-1.0-rc*
~~3. remove sigma on `external query`~~ *DONE on gulp-1.0-rc*

## stored query

~~- same cleanup as above~~ *DONE on gulp-1.0-rc*
~~- review `stored_query` API: should it be used for `raw query` only or also sigma ?~~ *DONE on gulp-1.0-rc*
~~- should it include support for external query (actually it does, but imho should be removed)~~ *removed*
~~- create `stored_sigma` collab object ?~~
- implement in the UI

## data normalization

- > medium priority: data normalization across all plugins (remove unneeded fields)
~~- > NEW_SOURCE, NEW_CONTEXT messages on websocket~~ *DONE on gulp-1.0-rc*

## generic

~~- improve `datastream_get_mapping_by_src`: use ws to send fields during the query, so the UI do not block (annoying as the dataset grows)~~ *DONE on gulp-1.0-rc, already merged on 20-refactoring branch*