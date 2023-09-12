defmodule ExqBatch.Internal do
  alias ExqBatch.Utils
  alias ExqBatch.Redis.Script
  require ExqBatch.Redis.Script

  @moduledoc false

  @state ":i"
  @on_complete ":c"
  @jobs ":b"
  @successful_jobs ":s"
  @dead_jobs ":d"
  @jid_to_batch_id ":j"

  def init(batch) do
    start = System.monotonic_time()
    {args, on_complete} = Map.pop(batch.on_complete, :args)

    on_complete_kvs = [
      "args",
      Jason.encode!(args),
      "job",
      Jason.encode!(on_complete),
      "queue",
      on_complete.queue
    ]

    commands = [
      ["MULTI"],
      ["SETEX", batch.prefix <> batch.id <> @state, batch.ttl, "initialized"],
      ["HMSET", batch.prefix <> batch.id <> @on_complete] ++ on_complete_kvs,
      ["EXPIRE", batch.prefix <> batch.id <> @on_complete, batch.ttl],
      ["DEL", batch.prefix <> batch.id <> @jobs],
      ["DEL", batch.prefix <> batch.id <> @successful_jobs],
      ["DEL", batch.prefix <> batch.id <> @dead_jobs],
      ["EXEC"]
    ]

    case Redix.pipeline(batch.redis, commands)
         |> emit_event(batch, :new, start) do
      {:ok,
       [
         "OK",
         "QUEUED",
         "QUEUED",
         "QUEUED",
         "QUEUED",
         "QUEUED",
         "QUEUED",
         ["OK", "OK", 1, _, _, _]
       ]} ->
        :ok

      {:ok, response} ->
        {:error, response}

      error ->
        error
    end
  end

  def add(batch, jid) when is_binary(jid) do
    start = System.monotonic_time()

    commands = [
      ["MULTI"],
      ["SETEX", batch.prefix <> jid <> @jid_to_batch_id, batch.ttl, batch.id],
      ["SADD", batch.prefix <> batch.id <> @jobs, jid],
      ["EXPIRE", batch.prefix <> batch.id <> @jobs, batch.ttl],
      ["EXEC"]
    ]

    case Redix.pipeline(batch.redis, commands)
         |> emit_event(batch, :add, start, %{jid: jid}) do
      {:ok, ["OK", "QUEUED", "QUEUED", "QUEUED", ["OK", _, 1]]} ->
        :ok

      {:ok, response} ->
        {:error, response}

      error ->
        error
    end
  end

  def add(batch, job) when is_map(job) do
    start = System.monotonic_time()
    namespace = Utils.namespace()
    job = Map.put(job, :enqueued_at, Utils.unix_seconds())

    commands = [
      ["MULTI"],
      ["SADD", Utils.queues_key(namespace), job.queue],
      ["LPUSH", Utils.queue_key(namespace, job.queue), Jason.encode!(job)],
      ["SETEX", batch.prefix <> job.jid <> @jid_to_batch_id, batch.ttl, batch.id],
      ["SADD", batch.prefix <> batch.id <> @jobs, job.jid],
      ["EXPIRE", batch.prefix <> batch.id <> @jobs, batch.ttl],
      ["EXEC"]
    ]

    case Redix.pipeline(batch.redis, commands)
         |> emit_event(batch, :add, start, %{jid: job.jid}) do
      {:ok, ["OK", "QUEUED", "QUEUED", "QUEUED", "QUEUED", "QUEUED", [_, _, "OK", _, 1]]} ->
        :ok

      {:ok, response} ->
        {:error, response}

      error ->
        error
    end
  end

  Script.compile(:create)

  def create(batch) do
    start = System.monotonic_time()
    namespace = Utils.namespace()

    keys = [
      Utils.queues_key(namespace),
      Utils.queue_key(namespace, batch.on_complete.queue),
      batch.prefix <> batch.id <> @state,
      batch.prefix <> batch.id <> @on_complete,
      batch.prefix <> batch.id <> @jobs,
      batch.prefix <> batch.id <> @successful_jobs,
      batch.prefix <> batch.id <> @dead_jobs
    ]

    args = [
      Utils.unix_seconds_string(),
      batch.ttl
    ]

    {:ok, code} = Script.eval(batch.redis, @create, keys, args)

    case code do
      0 ->
        emit_event(:ok, batch, :create, start)

      1 ->
        emit_event(:ok, batch, :create, start)
        emit_event(:ok, batch, :done, start)
    end
  end

  def after_success(redis, jid) do
    batch = find_batch_by_jid(redis, jid)

    if batch do
      complete(batch, jid, "success")
    else
      :ok
    end
  end

  def after_dead(redis, jid) do
    batch = find_batch_by_jid(redis, jid)

    if batch do
      complete(batch, jid, "dead")
    else
      :ok
    end
  end

  ## Private

  Script.compile(:complete)

  defp complete(batch, jid, status) do
    start = System.monotonic_time()
    namespace = Utils.namespace()

    queue =
      Redix.command!(batch.redis, ["HGET", batch.prefix <> batch.id <> @on_complete, "queue"])

    keys = [
      Utils.queues_key(namespace),
      Utils.queue_key(namespace, queue),
      batch.prefix <> batch.id <> @state,
      batch.prefix <> batch.id <> @on_complete,
      batch.prefix <> batch.id <> @jobs,
      batch.prefix <> batch.id <> @successful_jobs,
      batch.prefix <> batch.id <> @dead_jobs,
      batch.prefix <> jid <> @jid_to_batch_id
    ]

    args = [
      Utils.unix_seconds_string(),
      jid,
      status,
      batch.ttl
    ]

    {:ok, code} = Script.eval(batch.redis, @complete, keys, args)

    case code do
      0 ->
        emit_event(:ok, batch, :progress, start, %{jid: jid})

      1 ->
        emit_event(:ok, batch, :progress, start, %{jid: jid})
        emit_event(:ok, batch, :done, start)

      2 ->
        :ok
    end
  end

  defp find_batch_by_jid(redis, jid) do
    prefix = Application.get_env(:exq_batch, :prefix, "exq_batch") <> ":"
    batch_id = Redix.command!(redis, ["GET", prefix <> jid <> @jid_to_batch_id])

    if is_binary(batch_id) do
      ExqBatch.from_id(batch_id)
    end
  end

  defp emit_event(result, batch, event_name, start, metadata \\ %{}) do
    duration = System.monotonic_time() - start

    :telemetry.execute(
      [:exq_batch, :batch, event_name],
      %{duration: duration},
      Map.merge(metadata, %{
        id: batch.id
      })
    )

    result
  end
end
