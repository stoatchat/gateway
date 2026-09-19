FROM elixir:1.19-alpine AS build

RUN apk add --no-cache build-base git

WORKDIR /app
ENV MIX_ENV=prod

RUN mix local.hex --force && mix local.rebar --force

ADD mix.exs mix.lock /app/
RUN mix deps.get --only prod
RUN mix deps.compile

ADD . /app

RUN mix compile && \
    mix release 

FROM elixir:1.19-alpine

WORKDIR /app

COPY --from=build /app/_build/prod/rel/stoat_gateway ./

CMD ["bin/stoat_gateway", "start"]
