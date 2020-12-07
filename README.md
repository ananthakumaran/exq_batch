# ExqBatch

ExqBatch provides a building block to create complex workflows using
Exq jobs. A Batch monitors a group of Exq jobs and creates callback
job when all the jobs are processed.

## Example

```elixir
{:ok, batch} = ExqBatch.new(on_complete: %{queue: "default", class: CompletionWorker, args: ["complete"]})
{:ok, jid} = Exq.enqueue(Exq, "default", Worker, [1])
{:ok, batch} = ExqBatch.add(batch, jid)
{:ok, jid} = Exq.enqueue(Exq, "default", Worker, [2])
{:ok, batch} = ExqBatch.add(batch, jid)
{:ok, _batch} = ExqBatch.create(batch)
```

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
  ttl_in_seconds: 60 * 60 * 24 * 30,
  prefix: "exq_batch"
```

## Caveats

* The completion job will get enqueued only once after all the jobs in
  a group is either done or dead. A Resurrected job does not belong to
  the batch and will not lead to creation of completion job.

* All the jobs in batch **must** be allowed to be complete (either
  dead or done). Deleting any jobs while they are in retry queue will
  cause the batch to get stuck and will expire eventually.
