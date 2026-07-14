# StoatGateway
Rewrite of [Bonfire](https://github.com/stoatchat/stoatchat/tree/main/crates/bonfire) in Elixir, built-to handle events at scale.

## Environment Variables
When running a prod release the following environment variables are required 

- `WS_PORT` - Port the Websocket server will run on (default: `4000`)
- `RABBITMQ_URI` - Full URI of the RabbitMQ server e.g, `amqp://rabbituser:rabbitpass@localhost`
- `MONGODB_URI` - Full URI of the MongoDB server
- `REDIS_URI` - Full URI of the Redis Server

Websockets are available on `/ws`

## Development
Uses `config/dev.exs` when using mix run.
Run `mix format` before push.
