defmodule ExqBatch.Redis.Script do
  @moduledoc false

  require Logger

  defmacro compile(name) do
    path =
      Path.expand(
        "#{name}.lua",
        Path.dirname(__CALLER__.file)
      )

    source = File.read!(path)

    hash =
      :crypto.hash(:sha, source)
      |> Base.encode16(case: :lower)

    quote do
      Module.put_attribute(__MODULE__, :external_resource, unquote(path))
      Module.put_attribute(__MODULE__, unquote(name), unquote(Macro.escape({name, hash, source})))
    end
  end

  def eval(redis, {name, hash, source}, keys, args) do
    result =
      case Redix.command(redis, ["EVALSHA", hash, length(keys)] ++ keys ++ args) do
        {:error, %Redix.Error{message: "NOSCRIPT" <> _}} ->
          Redix.command(redis, ["EVAL", source, length(keys)] ++ keys ++ args)

        result ->
          result
      end

    Logger.debug(fn ->
      "eval script " <> inspect({name, keys, args}) <> " --> " <> inspect(result)
    end)

    result
  end
end
