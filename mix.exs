defmodule ExqBatch.MixProject do
  use Mix.Project

  def project do
    [
      app: :exq_batch,
      version: "0.1.0",
      elixir: "~> 1.8",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      docs: [
        main: "readme",
        extras: ["README.md"]
      ]
    ]
  end

  def application do
    [
      extra_applications: [:logger],
      mod: {ExqBatch.Application, []}
    ]
  end

  defp deps do
    [
      {:exq, "~> 0.14"},
      {:elixir_uuid, "~> 1.2"},
      {:redix, ">= 0.9.0"},
      {:telemetry, "~> 0.4"},
      {:jason, "~> 1.0"},
      {:dialyxir, "~> 1.0", only: :dev, runtime: false},
      {:ex_doc, "~> 0.22.0", only: :dev, runtime: false}
    ]
  end
end
