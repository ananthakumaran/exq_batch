defmodule ExqBatch.MixProject do
  use Mix.Project

  def project do
    [
      app: :exq_batch,
      version: "0.1.2",
      elixir: "~> 1.12",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      description: "Monitors group of Exq jobs",
      docs: [
        main: "readme",
        extras: ["README.md"]
      ],
      package: %{
        licenses: ["MIT"],
        links: %{"Github" => "https://github.com/ananthakumaran/exq_batch"},
        maintainers: ["ananthakumaran@gmail.com"]
      },
      source_url: "https://github.com/ananthakumaran/exq_batch"
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
      {:exq, "~> 0.16"},
      {:elixir_uuid, "~> 1.2"},
      {:redix, "~> 1.1"},
      {:telemetry, "~> 1.1"},
      {:jason, "~> 1.3"},
      {:dialyxir, "~> 1.2", only: :dev, runtime: false},
      {:ex_doc, "~> 0.28.4", only: :dev, runtime: false}
    ]
  end
end
