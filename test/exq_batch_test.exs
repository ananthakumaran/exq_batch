defmodule ExqBatchTest do
  use ExUnit.Case, async: false
  require Logger

  defmodule Backoff do
    @behaviour Exq.Backoff.Behaviour
    def offset(_job) do
      0.001
    end
  end

  defmodule SuccessWorker do
    def perform(id) do
      send(:runner, id)
      :ok
    end
  end

  defmodule FailureWorker do
    def perform(id) do
      send(:runner, id)
      raise ArgumentError
    end
  end

  defmodule CompletionWorker do
    def perform(id, status) do
      send(:runner, {id, status})
      :ok
    end
  end

  setup_all do
    :ok =
      :telemetry.attach_many(
        "debug",
        [
          [:exq_batch, :batch, :new],
          [:exq_batch, :batch, :add],
          [:exq_batch, :batch, :create],
          [:exq_batch, :batch, :progress],
          [:exq_batch, :batch, :done]
        ],
        fn event, measurements, metadata, _ ->
          Logger.info(
            "#{inspect(event)} measurements: #{inspect(measurements)}, metadata: #{inspect(metadata)}"
          )
        end,
        []
      )
  end

  setup do
    {:ok, redix} = Redix.start_link()
    "OK" = Redix.command!(redix, ["FLUSHALL"])
    Process.register(self(), :runner)
    %{redix: redix}
  end

  test "all jobs succeeded", %{redix: redix} do
    {:ok, batch} =
      ExqBatch.new(on_complete: [queue: "default", class: CompletionWorker, args: ["complete"]])

    {:ok, batch, _jid} = ExqBatch.add(batch, queue: "default", class: SuccessWorker, args: [1])
    {:ok, batch, _jid} = ExqBatch.add(batch, queue: "default", class: SuccessWorker, args: [2])
    {:ok, _batch} = ExqBatch.create(batch)

    assert_receive 1, 1000
    assert_receive 2, 1000
    assert_receive {"complete", %{"succeeded" => [_, _]}}, 1000

    assert [] == Redix.command!(redix, ["KEYS", "exq_batch:*"])
    refute_receive _, 1000
  end

  test "some jobs failed", %{redix: redix} do
    {:ok, batch} =
      ExqBatch.new(on_complete: [queue: "default", class: CompletionWorker, args: ["complete"]])

    jid = UUID.uuid4()
    {:ok, batch, ^jid} = ExqBatch.add(batch, jid)
    {:ok, ^jid} = Exq.enqueue(Exq, "default", SuccessWorker, [1], jid: jid)

    jid = UUID.uuid4()
    {:ok, batch, ^jid} = ExqBatch.add(batch, jid)
    {:ok, ^jid} = Exq.enqueue(Exq, "default", FailureWorker, [2], jid: jid)
    {:ok, _batch} = ExqBatch.create(batch)

    assert_receive 1, 1000
    assert_receive 2, 1000
    assert_receive 2, 1000
    assert_receive {"complete", %{"succeeded" => [_], "dead" => [_]}}, 1000

    assert [] == Redix.command!(redix, ["KEYS", "exq_batch:*"])
    refute_receive _, 1000
  end

  test "all jobs completed before create", %{redix: redix} do
    {:ok, batch} =
      ExqBatch.new(on_complete: [queue: "default", class: CompletionWorker, args: ["complete"]])

    {:ok, batch, _jid} = ExqBatch.add(batch, queue: "default", class: SuccessWorker, args: [1])
    {:ok, batch, _jid} = ExqBatch.add(batch, queue: "default", class: SuccessWorker, args: [2])

    assert_receive 1, 1000
    assert_receive 2, 1000
    refute_receive {"complete", _}, 1000

    {:ok, _batch} = ExqBatch.create(batch)
    assert_receive {"complete", %{"succeeded" => [_, _]}}, 1000

    assert [] == Redix.command!(redix, ["KEYS", "exq_batch:*"])
    refute_receive _, 1000
  end

  test "idempotent batch creation", %{redix: redix} do
    id = UUID.uuid4()

    {:ok, batch} =
      ExqBatch.new(
        id: id,
        on_complete: [queue: "default", class: CompletionWorker, args: ["complete"]]
      )

    {:ok, _batch, _jid} = ExqBatch.add(batch, queue: "default", class: SuccessWorker, args: [1])

    {:ok, batch} =
      ExqBatch.new(
        id: id,
        on_complete: [queue: "default", class: CompletionWorker, args: ["complete"]]
      )

    {:ok, batch, _jid} = ExqBatch.add(batch, queue: "default", class: SuccessWorker, args: [1])
    {:ok, batch, _jid} = ExqBatch.add(batch, queue: "default", class: SuccessWorker, args: [2])
    {:ok, _batch} = ExqBatch.create(batch)

    assert_receive 1, 1000
    assert_receive 1, 1000
    assert_receive 2, 1000
    assert_receive {"complete", %{"succeeded" => [_, _]}}, 1000

    assert [] == Redix.command!(redix, ["KEYS", "exq_batch:*"])
    refute_receive _, 1000
  end

  test "batch expires after ttl", %{redix: redix} do
    with_application_env(:exq_batch, :ttl_in_seconds, 5, fn ->
      {:ok, batch} =
        ExqBatch.new(on_complete: [queue: "default", class: CompletionWorker, args: ["complete"]])

      {:ok, batch, _jid} = ExqBatch.add(batch, queue: "default", class: SuccessWorker, args: [1])
      {:ok, batch, _jid} = ExqBatch.add(batch, queue: "unknown", class: SuccessWorker, args: [2])
      {:ok, batch, _jid} = ExqBatch.add(batch, queue: "default", class: FailureWorker, args: [3])
      {:ok, _batch} = ExqBatch.create(batch)

      assert_receive 1, 1000
      assert_receive 3, 1000
      assert_receive 3, 1000
      refute_receive 2, 1000
      refute_receive {"complete", _}, 1000

      Process.sleep(5000)
      assert [] == Redix.command!(redix, ["KEYS", "exq_batch:*"])
      refute_receive _, 1000
    end)
  end

  test "config :queues" do
    with_application_env(:exq_batch, :queues, ["default"], fn ->
      {:ok, batch} =
        ExqBatch.new(on_complete: [queue: "default", class: CompletionWorker, args: ["complete"]])

      {:ok, batch, _jid} = ExqBatch.add(batch, queue: "default", class: SuccessWorker, args: [1])
      {:ok, _batch} = ExqBatch.create(batch)
      assert_receive 1, 1000
      assert_receive {"complete", _}, 1000

      {:ok, batch} =
        ExqBatch.new(on_complete: [queue: "low", class: CompletionWorker, args: ["complete"]])

      {:ok, batch, _jid} = ExqBatch.add(batch, queue: "low", class: SuccessWorker, args: [1])
      {:ok, _batch} = ExqBatch.create(batch)
      assert_receive 1, 1000
      refute_receive {"complete", _}, 1000
    end)
  end

  test "encodes args correctly", %{redix: redix} do
    {:ok, batch} =
      ExqBatch.new(
        on_complete: [queue: "default", class: CompletionWorker, args: [["complete", []]]]
      )

    {:ok, batch, _jid} =
      ExqBatch.add(batch, queue: "default", class: SuccessWorker, args: [[[], %{}, 1]])

    {:ok, batch, _jid} =
      ExqBatch.add(batch, queue: "default", class: SuccessWorker, args: [[%{}, [], 2]])

    {:ok, _batch} = ExqBatch.create(batch)

    assert_receive [[], %{}, 1], 1000
    assert_receive [%{}, [], 2], 1000
    assert_receive {["complete", []], %{"succeeded" => [_, _]}}, 1000

    assert [] == Redix.command!(redix, ["KEYS", "exq_batch:*"])
    refute_receive _, 1000
  end

  test "empty batch", %{redix: redix} do
    {:ok, batch} =
      ExqBatch.new(on_complete: [queue: "default", class: CompletionWorker, args: ["complete"]])

    {:ok, _batch} = ExqBatch.create(batch)

    assert_receive {"complete", %{"succeeded" => [], "dead" => []}}, 1000

    assert [] == Redix.command!(redix, ["KEYS", "exq_batch:*"])
    refute_receive _, 1000
  end

  def with_application_env(app, key, new, context) do
    old = Application.get_env(app, key)
    Application.put_env(app, key, new)

    try do
      context.()
    after
      Application.put_env(app, key, old)
    end
  end
end
