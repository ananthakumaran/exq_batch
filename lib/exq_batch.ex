defmodule ExqBatch do
  alias ExqBatch.Internal
  alias ExqBatch.Utils

  @moduledoc """
  ExqBatch provides a building block to create complex workflows using
  Exq jobs. A Batch monitors a group of Exq jobs and creates callback
  job when all the jobs are processed.
  """

  defstruct [:id, :redis, :on_complete, :prefix, :ttl]
  @derive {Inspect, only: [:id]}
  @type t :: %__MODULE__{id: String.t()}

  @doc """
  Initialize a new Batch.

  ### Options

  * on\_complete (keyword) *required* - A Keyword list that specifies the details of job that will get enqueued on when all the jobs in a batch get completed
    * queue (string) *required* - exq job queue
    * args (array) *required* - exq job args.
    * class (string) *required* - exq job class.
    * retries (integer) - no of times the job should be retried
  * id (string) - A UUID is used by default. If same id is used for two batch jobs, the previous batch jobs will get cleared.
  """
  @spec new(Keyword.t()) :: {:ok, t} | {:error, term()}
  def new(options) do
    id = Keyword.get_lazy(options, :id, fn -> UUID.uuid4() end)
    batch = from_id(id)

    on_complete =
      Keyword.fetch!(options, :on_complete)
      |> Keyword.put_new_lazy(:jid, fn -> UUID.uuid4() end)
      |> Keyword.put_new_lazy(:retries, fn -> Utils.max_retries() end)
      |> Keyword.update!(:args, fn args when is_list(args) ->
        Jason.encode!(args)
      end)
      |> Keyword.update!(:class, fn
        worker when is_atom(worker) ->
          "Elixir." <> worker = to_string(worker)
          worker

        worker when is_binary(worker) ->
          worker
      end)
      |> Keyword.update!(:queue, fn queue -> to_string(queue) end)
      |> Enum.into(%{})

    batch = %{batch | on_complete: on_complete}

    with :ok <- Internal.init(batch) do
      {:ok, batch}
    end
  end

  @doc """
  Add the jid to the given batch
  """
  @spec add(t, String.t()) :: {:ok, t} | {:error, term()}
  def add(batch, jid) do
    with :ok <- Internal.add(batch, jid) do
      {:ok, batch}
    end
  end

  @doc """
  Finalize the batch creation process.
  """
  @spec create(t) :: {:ok, t} | {:error, term()}
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
