defmodule ExqBatch.Utils do
  def max_retries do
    :max_retries
    |> Exq.Support.Config.get()
    |> Exq.Support.Coercion.to_integer()
  end

  def namespace, do: Exq.Support.Config.get(:namespace)

  def queues_key(namespace) do
    namespace <> ":queues"
  end

  def queue_key(namespace, queue) do
    namespace <> ":queue:" <> queue
  end

  def unix_seconds do
    DateTime.utc_now()
    |> DateTime.to_unix(:microsecond)
    |> Kernel./(1_000_000.0)
    |> Float.to_string()
  end
end
