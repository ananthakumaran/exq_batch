# ExqBatch

[![Hex.pm](https://img.shields.io/hexpm/v/exq_batch.svg)](https://hex.pm/packages/exq_batch)

ExqBatch provides a building block to create complex workflows using
Exq jobs. A batch monitors a group of Exq jobs and creates callback
job when all the jobs are processed.

## Example

```elixir
{:ok, batch} = ExqBatch.new(on_complete: [queue: "default", class: CompletionWorker, args: ["complete"]])
{:ok, batch, jid} = ExqBatch.add(batch, queue: "default", class: Worker, args: [1])
{:ok, batch, jid} = ExqBatch.add(batch, queue: "default", class: Worker, args: [2])
{:ok, _batch} = ExqBatch.create(batch)
```

Checkout [documentation](https://hexdocs.pm/exq_batch/ExqBatch.html)
for more information.

## Config

```elixir
config :exq,
  middleware: [
    Exq.Middleware.Stats,
    ExqBatch.Middleware,
    Exq.Middleware.Job,
    Exq.Middleware.Manager,
    Exq.Middleware.Logger
  ]
```

`ExqBatch.Middleware` middleware **must** be added before the
`Exq.Middleware.Job` middleware. The middleware is used to track job
life cycle.

```elixir
config :exq_batch,
  # TTL of a batch job. Under normal condition, batch will get deleted
  # on completion. If a batch failed to complete for any reason, it
  # will get deleted after TTL seconds
  ttl_in_seconds: 60 * 60 * 24 * 30,
  # All the data related to batch will be stored under the prefix in redis
  prefix: "exq_batch",
  # Optional. If present, batch middleware will be enabled only for
  # the given queues. If not present, will be enabled for all queues.
  queues: ["default"]
```

## Caveats

* The completion job will get enqueued only once after all the jobs in
  a group is either done or dead. A Resurrected job does not belong to
  the batch and will not lead to creation of completion job.

* All the jobs in a batch **must** be allowed to be complete (either
  dead or done). Deleting any jobs while they are in retry queue will
  cause the batch to get stuck and will expire eventually.
