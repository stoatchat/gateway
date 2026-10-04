import Config

import_config "#{Mix.env()}.exs"

config :logger, :console, format: "$time $metadata[$level] $message\n"
