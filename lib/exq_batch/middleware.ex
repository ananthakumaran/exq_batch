defmodule ExqBatch.Middleware do
  @moduledoc """
  Monitors job life cycle and creates completion job when all job in a
  batch is done.
  """

  @behaviour Exq.Middleware.Behaviour
  alias Exq.Middleware.Pipeline
  alias ExqBatch.Utils
  alias ExqBatch.Internal

  def before_work(pipeline) do
    pipeline
  end

  def after_processed_work(%Pipeline{assigns: assigns} = pipeline) do
    :ok = Internal.after_success(assigns.redis, assigns.job.jid)
    pipeline
  end

  def after_failed_work(%Pipeline{assigns: assigns} = pipeline) do
    if dead?(assigns.job) do
      :ok = Internal.after_dead(assigns.redis, assigns.job.jid)
    end

    pipeline
  end

  defp dead?(%{retry: retry} = job)
       when is_integer(retry) and retry > 0 do
    dead?(job, retry)
  end

  defp dead?(%{retry: true} = job) do
    dead?(job, Utils.max_retries())
  end

  defp dead?(_job) do
    true
  end

  defp dead?(job, max_retries) do
    retry_count = (job.retry_count || 0) + 1
    retry_count > max_retries
  end
end
