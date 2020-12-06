defmodule ExqBatch do
  alias ExqBatch.Internal
  alias ExqBatch.Utils

  @moduledoc """
  Documentation for `ExqBatch`.
  """

  defstruct [:id, :redis, :on_complete, :prefix, :ttl]

  def new(options) do
    id = Keyword.get_lazy(options, :id, fn -> UUID.uuid4() end)
    batch = from_id(id)

    on_complete =
      Keyword.fetch!(options, :on_complete)
      |> Map.put_new_lazy(:jid, fn -> UUID.uuid4() end)
      |> Map.put_new_lazy(:retries, fn -> Utils.max_retries() end)
      |> Map.update!(:args, fn args when is_list(args) ->
        Jason.encode!(args)
      end)
      |> Map.update!(:class, fn
        worker when is_atom(worker) ->
          "Elixir." <> worker = to_string(worker)
          worker

        worker when is_binary(worker) ->
          worker
      end)

    batch = %{batch | on_complete: on_complete}

    with :ok <- Internal.init(batch) do
      {:ok, batch}
    end
  end

  def add(batch, jid) do
    with :ok <- Internal.add(batch, jid) do
      {:ok, batch}
    end
  end

  def create(batch) do
    with {:ok, _} <- Internal.create(batch) do
      {:ok, batch}
    end
  end

  ## Private

  @doc false
  def from_id(id) do
    ttl = Application.get_env(:exq_batch, :ttl_in_seconds, 60 * 60 * 24 * 30)
    prefix = Application.get_env(:exq_batch, :prefix, "exq_batch") <> ":"

    redis =
      Application.get_env(
        :exq_batch,
        :redis,
        Exq.Support.Config.get(:name)
        |> Exq.Support.Opts.redis_client_name()
      )

    %__MODULE__{
      id: id,
      redis: redis,
      prefix: prefix,
      ttl: ttl
    }
  end
end
