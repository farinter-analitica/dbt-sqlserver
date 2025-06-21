if [ ! -f ./.env ] && [ -f ./.env.sample ]; then
  cp ./.env.sample ./.env
fi
