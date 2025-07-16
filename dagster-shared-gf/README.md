
# dagster_shared_gf

This project contains shared resources used by other Dagster projects in this repository.


## Getting Started


First, install this Dagster code location as a Python package in editable mode using [uv](https://github.com/astral-sh/uv):

```bash
uv pip install -e ".[dev]"
```

Then, start the Dagster UI web server:

```bash
dagster dev
```

Open http://localhost:3000 in your browser to see the project.

You can start writing assets in `dagster_shared_gf/assets.py`. The assets are automatically loaded into the Dagster code location as you define them.


## Development

### Adding new Python dependencies


You can specify new Python dependencies in `pyproject.toml` and install them with:

```bash
uv pip install -r requirements.txt
```
or, for editable mode:
```bash
uv pip install -e ".[dev]"
```

### Unit testing

Tests are in the `dagster_shared_gf_tests` directory and you can run tests using `pytest`:

```bash
pytest dagster_shared_gf_tests
```

### Schedules and sensors

If you want to enable Dagster [Schedules](https://docs.dagster.io/concepts/partitions-schedules-sensors/schedules) or [Sensors](https://docs.dagster.io/concepts/partitions-schedules-sensors/sensors) for your jobs, the [Dagster Daemon](https://docs.dagster.io/deployment/dagster-daemon) process must be running. This is done automatically when you run `dagster dev`.

Once your Dagster Daemon is running, you can start enabling schedules and sensors for your jobs.

## Deploy on Dagster Cloud

The easiest way to deploy your Dagster project is to use Dagster Cloud.

Check out the [Dagster Cloud Documentation](https://docs.dagster.cloud) to learn more.

## Deploy on Dagster Cloud

The easiest way to deploy your Dagster project is to use Dagster Cloud.

Check out the [Dagster Cloud Documentation](https://docs.dagster.cloud) to learn more.
