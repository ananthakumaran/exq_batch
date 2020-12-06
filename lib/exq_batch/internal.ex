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
    on_complete_kvs =
      Enum.flat_map(batch.on_complete, fn {key, value} -> [to_string(key), to_string(value)] end)

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

    case Redix.pipeline(batch.redis, commands) do
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

  def add(batch, jid) do
    commands = [
      ["MULTI"],
      ["SETEX", batch.prefix <> jid <> @jid_to_batch_id, batch.ttl, batch.id],
      ["SADD", batch.prefix <> batch.id <> @jobs, jid],
      ["EXPIRE", batch.prefix <> batch.id <> @jobs, batch.ttl],
      ["EXEC"]
    ]

    case Redix.pipeline(batch.redis, commands) do
      {:ok, ["OK", "QUEUED", "QUEUED", "QUEUED", ["OK", _, 1]]} ->
        :ok

      {:ok, response} ->
        {:error, response}

      error ->
        error
    end
  end

  Script.compile(:create)

  def create(batch) do
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
      Utils.unix_seconds()
    ]

    Script.eval(batch.redis, @create, keys, args)
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
      Utils.unix_seconds(),
      jid,
      status
    ]

    {:ok, 0} = Script.eval(batch.redis, @complete, keys, args)
    :ok
  end

  defp find_batch_by_jid(redis, jid) do
    prefix = Application.get_env(:exq_batch, :prefix, "exq_batch") <> ":"
    batch_id = Redix.command!(redis, ["GET", prefix <> jid <> @jid_to_batch_id])

    if is_binary(batch_id) do
      ExqBatch.from_id(batch_id)
    end
  end
end
