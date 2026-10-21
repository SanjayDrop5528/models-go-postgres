# UUID arrays in writes and dataset preview

The engine now converts `data_type: "uuid", is_array: true` into an ARRAY
attribute with UUID item validation. Empty arrays and nullable NULL values are
accepted; scalar strings and invalid UUID items are rejected. Typed array
elements also survive schema generation, producing `UUID[]` for new columns.

The existing `DataSetService.Preview` / save compilation path now resolves JOIN
fields through model metadata. The live PostgreSQL adapter verifies their
physical types and target uniqueness against database catalogs before generating
SQL. Client type hints cannot override these types. For `uuid[]` and `uuid`, it
emits `target.id = ANY(source.ids)` even if an old payload has
`convert_To_String: true` or `cast_mode: "ANY_FROM"`. A text/UUID mismatch only
permits text conversion when the payload explicitly allows conversion.

Preview preserves selected aliases inside nested objects and arrays. SQL
execution failures now propagate as errors instead of returning successful empty
rows. `MANY` with explicit `FLAT` retains normal row multiplication; `JSON_ARRAY`
uses correlated aggregation. This integration supports the existing dataset
output formats; the standalone dynamic relationship API has additional formats.

## Existing employee dataset configuration

- Set primary plants, secondary plants, and primary workplaces to
  `cardinality: "MANY", outputFormat: "JSON_ARRAY"` for nested arrays.
- Map `primary_plant_mine_alias.name` to `primary_plant_name` and
  `secondary_plant_mine_alias.name` to `secondary_plant_name`. Those header labels
  were reversed in the supplied request.
- Configure driver identification and driving licenses as MANY/JSON_ARRAY if
  multiple rows can have the same `driver_id`. ONE/OBJECT now requires an actual
  unique target key; incorrect claims fail with an actionable validation error.
- An empty `primary_location` input correctly produces an empty workplace array.
- ARRAY metadata alone is not evidence that a live column is UUID[]. If the live
  schema differs, use an explicit supported conversion or correct the schema
  separately. These changes perform no database migration.

## Rollout and verification

Rebuild the HTTP service with the updated engine and PostgreSQL modules, then
restart/reinitialize its model metadata cache. Re-preview and save existing
datasets to regenerate their stored SQL. Local changes do not update UAT.

The HTTP service requires a sibling `fleetone-go-shared-lib` checkout. It was
absent in the working environment, preventing a full HTTP service build.

Run both module test suites and `go vet ./...`. Set `DYNAMIC_JOIN_TEST_DSN` to an
isolated PostgreSQL database to run `TestUUIDArrayCreateThroughDatasetPreview`.
This test builds models from UUID-array metadata, validates and inserts JSON-like
arrays, compiles through the actual adapter/service path, and checks the returned
nested data. It verifies no redundant casts, empty-array behavior, preservation
of aliased fields, independent driver aggregates, rejection of false ONE claims,
and propagation of SQL errors. Fixtures use their own generated schema and clean
it up afterwards.
