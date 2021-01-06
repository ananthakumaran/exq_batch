use Mix.Config

config :exq,
  middleware: [
    Exq.Middleware.Stats,
    ExqBatch.Middleware,
    Exq.Middleware.Job,
    Exq.Middleware.Manager,
    Exq.Middleware.Logger
  ],
  max_retries: 1,
  backoff: ExqBatchTest.Backoff,
  poll_timeout: 10,
  scheduler_pool_timeout: 10,
  queues: ["default", "low"]

config :exq_batch,
  ttl_in_seconds: 60 * 5

config :logger,
  level: :warn
