defmodule ExqBatchTest do
  use ExUnit.Case, async: false

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

  setup do
    {:ok, redix} = Redix.start_link()
    "OK" = Redix.command!(redix, ["FLUSHALL"])
    Process.register(self(), :runner)
    %{redix: redix}
  end

  test "all jobs succeeded", %{redix: redix} do
    {:ok, batch} =
      ExqBatch.new(on_complete: %{queue: "default", class: CompletionWorker, args: ["complete"]})

    {:ok, jid} = Exq.enqueue(Exq, "default", SuccessWorker, [1])
    {:ok, batch} = ExqBatch.add(batch, jid)
    {:ok, jid} = Exq.enqueue(Exq, "default", SuccessWorker, [2])
    {:ok, batch} = ExqBatch.add(batch, jid)
    {:ok, _batch} = ExqBatch.create(batch)

    assert_receive 1, 1000
    assert_receive 2, 1000
    assert_receive {"complete", %{"succeeded" => [_, _]}}, 1000

    assert [] == Redix.command!(redix, ["KEYS", "exq_batch:*"])
    refute_receive _, 1000
  end

  test "some jobs failed", %{redix: redix} do
    {:ok, batch} =
      ExqBatch.new(on_complete: %{queue: "default", class: CompletionWorker, args: ["complete"]})

    {:ok, jid} = Exq.enqueue(Exq, "default", SuccessWorker, [1])
    {:ok, batch} = ExqBatch.add(batch, jid)
    {:ok, jid} = Exq.enqueue(Exq, "default", FailureWorker, [2])
    {:ok, batch} = ExqBatch.add(batch, jid)
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
      ExqBatch.new(on_complete: %{queue: "default", class: CompletionWorker, args: ["complete"]})

    {:ok, jid} = Exq.enqueue(Exq, "default", SuccessWorker, [1])
    {:ok, batch} = ExqBatch.add(batch, jid)
    {:ok, jid} = Exq.enqueue(Exq, "default", SuccessWorker, [2])
    {:ok, batch} = ExqBatch.add(batch, jid)

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
        on_complete: %{queue: "default", class: CompletionWorker, args: ["complete"]}
      )

    {:ok, jid} = Exq.enqueue(Exq, "default", SuccessWorker, [1])
    {:ok, _batch} = ExqBatch.add(batch, jid)

    {:ok, batch} =
      ExqBatch.new(
        id: id,
        on_complete: %{queue: "default", class: CompletionWorker, args: ["complete"]}
      )

    {:ok, jid} = Exq.enqueue(Exq, "default", SuccessWorker, [1])
    {:ok, batch} = ExqBatch.add(batch, jid)
    {:ok, jid} = Exq.enqueue(Exq, "default", SuccessWorker, [2])
    {:ok, batch} = ExqBatch.add(batch, jid)
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
        ExqBatch.new(
          on_complete: %{queue: "default", class: CompletionWorker, args: ["complete"]}
        )

      {:ok, jid} = Exq.enqueue(Exq, "default", SuccessWorker, [1])
      {:ok, batch} = ExqBatch.add(batch, jid)
      {:ok, jid} = Exq.enqueue(Exq, "unknown", SuccessWorker, [2])
      {:ok, batch} = ExqBatch.add(batch, jid)
      {:ok, jid} = Exq.enqueue(Exq, "default", FailureWorker, [3])
      {:ok, batch} = ExqBatch.add(batch, jid)
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
