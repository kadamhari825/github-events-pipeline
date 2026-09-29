# Decisions

## TRANSFORMED: append vs overwrite

TRANSFORMED initially used .mode("overwrite") on every run, which silently
destroyed prior runs' data while the SQLite state table still claimed those
files were valid and processed. Fixed by switching TRANSFORMED to
.mode("append"), since this stage only reads files that are newly in
QUALITY_CHECKED state, so appending is safe and correct.

## PUBLISHED: kept on overwrite, not switched to append

PUBLISHED intentionally stays on .mode("overwrite") — it re-reads the
entire /transformed folder each run and rewrites /published from scratch.
Switching it to append was considered and explicitly rejected, since
append here would double-count rows already published in a prior run.
A later idempotency bug (crash between the parquet write and the SQLite
commit) was fixed separately with a guard that checks /published for
already-written source files before writing again.

## Partitioning: partitionBy("type"), not event_date

Chose partitionBy("type") as the write strategy for /published. An
external critique argued for event_date partitioning instead, but with
only one date present in the dataset, date partitioning gives zero
pruning benefit, while type partitioning directly serves the planned
analysis queries (breakdowns by event type). event_date becomes the
right call if the pipeline extends beyond a single day. Confirmed via
explain() that partitionBy("type") produces real partition pruning
(PartitionFilters populated, PushedFilters empty) when filtering on
type. Also confirmed this does not fix the underlying PushEvent skew
(94.4% of events), only exposes it physically as an uneven partition.

## Schema evolution (inner field / type checking)

The original schema check only verified that 5 top-level field names
existed (id, type, actor, repo, created_at). It never looked inside
actor/repo, so a renamed inner field (e.g. actor.login) or a type change
(e.g. actor.id becoming a string) would have passed silently and
corrupted downstream data.

Replaced with EXPECTED_SCHEMA, a dict of required field/type pairs
including dotted inner fields (actor.id, actor.login, repo.id,
repo.name). A flat_types lookup walks the DataFrame schema and flattens
struct columns into dotted names, so inner renames and type changes are
visible. A file fails (FAILED, no retry) if any expected field is
missing or has the wrong type, since neither issue resolves itself on
rerun.

payload was deliberately excluded from EXPECTED_SCHEMA since its shape
varies by event type and nothing downstream reads it. The old
EXPECTED_FIELDS set is kept alongside the new dict, now purely
informational — it logs extra top-level fields (org, payload, public)
and total column count on success, but does not affect pass/fail.

Tested in an isolated sandbox (separate raw files, separate SQLite DB,
never touching real pipeline data) with two real fault-injection cases:
- Renamed actor.login -> actor.user_login: caught as a missing field,
  file FAILED, other files in the batch unaffected.
- Changed actor.id from bigint to string: caught as a wrong type, file
  FAILED, other files unaffected.

## New or unexpected fields in raw data

Decision: a new field appearing in a raw file is logged, never treated as a failure.

Reasoning: adding a field is an additive change and cannot break anything that already works. The transform stage selects only id, type, actor, repo and created_at, so extra columns are dropped before they reach transformed or published. Missing fields and changed types can corrupt downstream data, which is why those fail a file. A new field cannot, so failing on it would make the pipeline brittle every time GitHub adds something.

Current behavior: extra top-level columns (org, payload, public) are logged on success. Only top-level extra columns are logged. New fields nested inside actor or repo are not logged, which is acceptable because nothing downstream reads them.

a downstream step starts reading a field that is not in EXPECTED_SCHEMA. At that point the field should be added to EXPECTED_SCHEMA so it is validated properly.
