## feature/uri

* Implemented new method `parse_many` in tarantool uri library,
  which allowed to parse several URIs with different parameters
  passed in different ways (as a table of string URIs and URIs
  tables with URI and it's parameters).
  Update `parse` method to make possible the same as for the new
  `parse_many` method but only for single URI.
