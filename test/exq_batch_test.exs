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
    :ok
  end

  test "all jobs succeeded" do
    {:ok, batch} =
      ExqBatch.new(on_complete: %{queue: "default", class: CompletionWorker, args: ["complete"]})

    {:ok, jid} = Exq.enqueue(Exq, "default", SuccessWorker, [1])
    {:ok, batch} = ExqBatch.add(batch, jid)
    {:ok, jid} = Exq.enqueue(Exq, "default", SuccessWorker, [2])
    {:ok, batch} = ExqBatch.add(batch, jid)
    {:ok, _batch} = ExqBatch.create(batch)

    assert_receive 1
    assert_receive 2
    assert_receive "complete"
  end

  test "some jobs failed" do
    {:ok, batch} =
      ExqBatch.new(on_complete: %{queue: "default", class: CompletionWorker, args: ["complete"]})

    {:ok, jid} = Exq.enqueue(Exq, "default", SuccessWorker, [1])
    {:ok, batch} = ExqBatch.add(batch, jid)
    {:ok, jid} = Exq.enqueue(Exq, "default", FailureWorker, [2])
    {:ok, batch} = ExqBatch.add(batch, jid)
    {:ok, _batch} = ExqBatch.create(batch)

    assert_receive 1
    assert_receive 2
    assert_receive 2
    assert_receive "complete"
  end

  test "all jobs completed before create" do
    {:ok, batch} =
      ExqBatch.new(on_complete: %{queue: "default", class: CompletionWorker, args: ["complete"]})

    {:ok, jid} = Exq.enqueue(Exq, "default", SuccessWorker, [1])
    {:ok, batch} = ExqBatch.add(batch, jid)
    {:ok, jid} = Exq.enqueue(Exq, "default", SuccessWorker, [2])
    {:ok, batch} = ExqBatch.add(batch, jid)

    assert_receive 1
    assert_receive 2
    refute_receive "complete"

    {:ok, _batch} = ExqBatch.create(batch)
    assert_receive "complete"
  end
end
