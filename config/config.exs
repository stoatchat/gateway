import Config

import_config "#{Mix.env()}.exs"

config :logger, :console,
  format: "$time [$level] $metadata $message\n",
  metadata: [:pid, :server_id, :user_id, :session_id, :span_id]
