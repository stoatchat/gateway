# StoatGateway
Rewrite of [Bonfire](https://github.com/stoatchat/stoatchat/tree/main/crates/bonfire) in Elixir, built-to handle events at scale.

## Configuration
When running a release either a Revolt.toml or Environment Variables are required.

### Environment Variables
- `WS_PORT` - Port the Websocket server will run on (default: `4000`)
- `MONGODB_URI` - Full URI of the MongoDB server
- `REDIS_URI` - Full URI of the Redis Server
- `RABBIT_HOST` - Host of the Rabbit Server
- `RABBIT_Port` - Port of the Rabbit Server
- `RABBIT_USERNAME` - Username of the Rabbit Server
- `RABBIT_PASSWORD` - Password of the Rabbit Server

By default the Websocket is available on port `14703` at `/` like Bonfire.

## Development
- Uses `config/dev.exs` when using mix run.
- Run `mix format` before push.
