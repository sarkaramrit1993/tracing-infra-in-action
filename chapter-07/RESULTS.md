# Chapter 7 benchmark results

Generated from the committed JSON in `benchmarks/results/` by
`scripts/render_results.py`. Re-run that script after adding a result file.

## Bloom index granule pruning

Source: `bloom-index-2026-07-15.json`
Measured: 2026-07-15

| Metric | Value |
|---|---|
| transport | http |
| probe_trace_id | a94dffa36e0fd0e1b42e2b10864c2ab5 |
| granules_without_skip_index.selected | 19 |
| granules_without_skip_index.total | 147 |
| granules_with_bloom_index.selected | 4 |
| granules_with_bloom_index.total | 19 |
| granules_pruned | 15 |

## Per-column compression

Source: `compression-ratio-2026-07-15.json`
Measured: 2026-07-15

200,000 spans at 6 spans per trace.

| Column | Stored bytes | Raw bytes | Ratio |
|---|---|---|---|
| adjusted_count | 1,350 | 1,600,000 | 1185.19x |
| service_name | 420 | 200,503 | 477.39x |
| span_name | 525 | 200,549 | 382.00x |
| status_code | 4,316 | 200,450 | 46.44x |
| attributes | 365,019 | 4,204,609 | 11.52x |
| span_id | 656,955 | 3,400,000 | 5.18x |
| duration_ns | 701,825 | 1,600,000 | 2.28x |
| trace_id | 3,375,024 | 6,600,000 | 1.96x |
| timestamp | 1,083,118 | 1,600,000 | 1.48x |

## Per-tenant attribute cardinality

Source: `tenant-cardinality-2026-07-15.json`
Measured: 2026-07-15

| Metric | Value |
|---|---|
| transport | http |
| num_spans_per_phase | 1000000 |
| spans_per_trace | 6 |
| tenants | 4 |
| column | attributes |
| baseline.stored_bytes | 2792458 |
| baseline.raw_bytes | 31022853 |
| baseline.ratio | 11.11 |
| baseline.active_parts | 1 |
| baseline.rows | 1000000 |
| blowup.stored_bytes | 4428489 |
| blowup.raw_bytes | 35522865 |
| blowup.ratio | 8.02 |
| blowup.active_parts | 1 |
| blowup.rows | 1000000 |
| delta.ratio_drop | 3.09 |
| delta.stored_bytes_growth_x | 1.59 |
| delta.stored_bytes_added | 1636031 |

## Hot-to-cold tiering

Source: `tiering-move-2026-07-15.json`
Measured: 2026-07-15

| Metric | Value |
|---|---|
| transport | http |
| num_spans | 50000 |
| move_ttl_seconds | 10 |
| cold_disk | s3_cold |
| parts_by_disk_before.default | 1 |
| parts_by_disk_after.s3_cold | 2 |
| parts_by_disk_after.default | 1 |
| move_latency_seconds | 1.01 |

> parts_by_disk_after is the snapshot taken the moment the first part reaches the cold disk, which is also where move_latency_seconds stops. A remaining count on 'default' at that instant is expected and does not indicate an incomplete move: parts age and migrate individually.
