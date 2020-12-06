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
    def perform(id) do
      send(:runner, id)
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
    assert_receive "complete", 1000

    assert [] == Redix.command!(redix, ["KEYS", "exq_batch:*"])
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
    assert_receive "complete", 1000

    assert [] == Redix.command!(redix, ["KEYS", "exq_batch:*"])
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
    refute_receive "complete", 1000

    {:ok, _batch} = ExqBatch.create(batch)
    assert_receive "complete"

    assert [] == Redix.command!(redix, ["KEYS", "exq_batch:*"])
  end
end
