<!--
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# Parallel metadata tasks: proof of concept

This draft builds on Apache Iceberg C++ PR
[#1120](https://github.com/apache/iceberg-cpp/pull/1120). It demonstrates a possible
follow-up to the review discussion about multiple metadata tasks, parallel reads,
and serialization. The implementation is experimental and its new helper header
is internal and is not installed.

## Work assignment and execution

`PlanManifestMetadataScan()` resolves the selected snapshot once and reads its
manifest list. It returns a `MetadataScan` that plans one task per data manifest.
Each task has one distinct manifest, the fixed snapshot ID, the source schema,
the applicable partition spec, and the output projection. Deleted manifest entries
are excluded by `ManifestReader::LiveEntries()`.

The output is a small demonstration schema: `manifest_path`, `file_path`,
`record_count`, and `file_size_in_bytes`. It is not the full FilesTable schema.

Callers can use the existing `ToArrowArrayStream()` for sequential execution, or
execute the planned tasks through the prototype collector:

```cpp
// `context` captures table metadata and the PoC schema/projection. `io` and
// `executor` belong to the caller. Results use the original task planning order.
auto scan = PlanManifestMetadataScan(context, io).value();
auto tasks = scan->PlanTasks().value();
auto stream = ExecuteManifestMetadataTasks(
    context.projected_schema, std::move(tasks),
    {.executor = &executor, .max_tasks_in_flight = 4, .batch_size = 1024}).value();
```

`ExecuteManifestMetadataTasks()` uses the existing `TaskGroup` and `Executor`.
Each worker opens its own reader/Arrow stream and consumes it independently. Work
is submitted in bounded windows. On failure, the active window is drained, later
windows are not submitted, and buffered partial output is released. The supplied
FileIO implementation must support concurrent reads.

The collector is synchronous and eagerly buffers every output batch before
returning its Arrow stream. The in-flight task limit bounds concurrency, not
total result memory. A production implementation needs bounded output queues,
streaming backpressure, and cancellation semantics.

## Task transport

`SerializeManifestMetadataTask()` writes versioned JSON containing the task type,
fixed snapshot ID, manifest location/length and inherited snapshot/sequence
metadata, source schema, partition spec, and projection. It carries no function
pointer, object address, FileIO object, or driver-side table handle.

Workers call `DeserializeManifestMetadataTask(payload, worker_io)` with their own
configured execution FileIO. Reader dispatch uses the task kind. Workers read the
assigned manifest directly and do not resolve the current table snapshot or
reread its manifest list. The format is a prototype, not a stable wire protocol.

The tests copy only manifest bytes into independent worker storage, destroy the
driver's scan/table/IO, deserialize the task payloads, and compare parallel output
with the original sequential scan. An instrumented FileIO makes two opens
rendezvous to prove concurrent dispatch without timing-based speed assertions.

## Scope of this draft

- Unencrypted data manifests, live data-file metadata, fixed snapshot selection,
  whole-column projections, and task-local batches are implemented.
- JSON task transport and bounded local parallel execution are executable. A
  remote scheduler or RPC transport is outside this draft.
- Delete-file metadata, filters, the full FilesTable schema, and streaming
  manifest-entry decoding need further integration.
- Partition summaries need partial aggregation followed by a merge using the
  original partition grouping rules. Concatenating per-manifest summaries would
  produce duplicate partition rows, so no partition-table parallelism is claimed.
- The manifest-list planning pass remains sequential; manifest grouping and cost
  balancing can build on the descriptor without changing the executor boundary.

Run `metadata_table_test` with the filter `ParallelMetadataPocTest.*` for the
prototype coverage, and run the full target to check existing metadata scans.
