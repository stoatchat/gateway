import Config

import_config "#{Mix.env()}.exs"

config :logger, :console,
  format: "$time $metadata[$level] $message\n",
  metadata: [:pid, :server_id, :user_id, :session_id, :span_id]
