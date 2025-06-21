if [ ! -f ./.env ] && [ -f ./.env.sample ]; then
  cp ./.env.sample ./.env
fi
if [ ! -f ./.dlt/secrets.toml ] && [ -f ./.dlt/secrets.toml.sample ]; then
  cp ./.dlt/secrets.toml.sample ./.dlt/secrets.toml
fi
