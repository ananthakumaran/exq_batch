defmodule ExqBatch do
  alias ExqBatch.Internal
  alias ExqBatch.Utils

  @moduledoc """
  ExqBatch provides a building block to create complex workflows using
  Exq jobs. A batch monitors a group of Exq jobs and creates callback
  job when all the jobs are processed.
  """

  defstruct [:id, :redis, :on_complete, :prefix, :ttl]
  @derive {Inspect, only: [:id]}
  @type t :: %__MODULE__{id: String.t()}

  @doc """
  Initialize a new batch.

  on\_complete job will receive an extra arg which includes the list of dead and succeeded jids. Example `%{"dead" => [], "succeeded" => [jid1, jid2]}`

  ### Options

  * on\_complete (keyword) *required* - A Keyword list that specifies the details of job that will get enqueued on when all the jobs in a batch get completed
    * queue (string) *required* - exq job queue
    * args (array) *required* - exq job args.
    * class (string) *required* - exq job class.
    * retry (integer) - no of times the job should be retried.
    * jid (string) - if not present, A UUID is used.
  * id (string) - A UUID is used by default. If same id is used for two batch jobs, the previous batch jobs will get cleared.
  """
  @spec new(Keyword.t()) :: {:ok, t} | {:error, term()}
  def new(options) do
    id = Keyword.get_lazy(options, :id, fn -> UUID.uuid4() end)
    batch = from_id(id)

    on_complete =
      Keyword.fetch!(options, :on_complete)
      |> to_job()

    batch = %{batch | on_complete: on_complete}

    with :ok <- Internal.init(batch) do
      {:ok, batch}
    end
  end

  @doc """
  Add a job to the given batch.

  There are two ways to add a job to batch

  1) Pass the job params and let ExqBatch enqueue the job. ExqBatch
  will both enqueue the job and add it to the batch using a atomic
  [MULTI EXEC](https://redis.io/topics/transactions) operation. Refer
  the on\_complete option in `new/1` for job options.

      ```
      {:ok, batch, jid} = ExqBatch.add(batch, queue: "default", class: Worker, args: [1])
      ```

  2) Add a job using jid. Note that, the `add/2` should be called
  **before** the job is enqueued. Otherwise, there is a potential race
  condition where the job could finish before `add/2` is called and
  would cause the batch to hang. Exq allows to specify the jid of the
  job, so generate a jid first, then add it to the batch and after
  that enqueue the job.

      ```
      jid = UUID.uuid4()
      {:ok, batch, ^jid} = ExqBatch.add(batch, jid)
      {:ok, ^jid} = Exq.enqueue(Exq, "default", SuccessWorker, [1], jid: jid)
      ```
  """
  @spec add(t, String.t() | Keyword.t()) :: {:ok, t, binary} | {:error, term()}
  def add(batch, jid) when is_binary(jid) do
    with :ok <- Internal.add(batch, jid) do
      {:ok, batch, jid}
    end
  end

  def add(batch, job) when is_list(job) do
    job = to_job(job)

    with :ok <- Internal.add(batch, job) do
      {:ok, batch, job.jid}
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

  defp to_job(options) do
    Keyword.put_new_lazy(options, :jid, fn -> UUID.uuid4() end)
    |> Keyword.put_new_lazy(:retry, fn -> Utils.max_retries() end)
    |> Keyword.update!(:args, fn args when is_list(args) ->
      args
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
  end
end
