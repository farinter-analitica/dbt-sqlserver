analiticasu ALL=(ALL) NOPASSWD: /*/uv run --frozen /*/scripts/generate_dagster_service.py
analiticasu ALL=(ALL) NOPASSWD:  /bin/systemctl status dagster_*_webserver, \
  /bin/systemctl status dagster_*_daemon, \
  /bin/systemctl restart dagster_*_webserver, \
  /bin/systemctl restart dagster_*_daemon, \
  /bin/systemctl stop dagster_*_webserver, \
  /bin/systemctl stop dagster_*_daemon, \
  /bin/systemctl start dagster_*_webserver, \
  /bin/systemctl start dagster_*_daemon