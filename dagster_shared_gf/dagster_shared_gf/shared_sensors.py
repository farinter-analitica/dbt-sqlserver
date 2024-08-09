from dagster import (make_email_on_run_failure_sensor, DagsterInstance, SensorEvaluationContext, SensorDefinition,
                     JobDefinition , GraphDefinition , UnresolvedAssetJobDefinition , RepositorySelector , JobSelector, DefaultSensorStatus)
from datetime import datetime
from typing import List, Sequence
import os

def custom_email_body(context: SensorEvaluationContext):
    dagster_run = context.run
    failure_event = context.failure_event

    # Retrieve the stats snapshot for the current run_id
    instance = DagsterInstance.get()
    stats_snapshot = instance.get_run_stats(dagster_run.run_id)

    # Convert UNIX timestamps to datetime objects
    start_time = datetime.fromtimestamp(stats_snapshot.start_time) if stats_snapshot.start_time else None
    end_time = datetime.fromtimestamp(stats_snapshot.end_time) if stats_snapshot.end_time else None

    # Format datetime objects as strings if needed
    start_time_str = start_time.strftime('%Y-%m-%d %H:%M:%S') if start_time else 'N/A'
    end_time_str = end_time.strftime('%Y-%m-%d %H:%M:%S') if end_time else 'N/A'

    # Construct the URL to the run in Dagit (replace with your actual Dagit URL)
    dagit_url = f"http://dagit.mycompany.com/instance/runs/{dagster_run.run_id}"

    # Include more details in the email body
    email_body = f"""
    Job {dagster_run.job_name} failed!
    Run ID: {dagster_run.run_id}
    Pipeline Snapshot ID: {dagster_run.pipeline_snapshot_id}
    Start Time: {start_time_str}
    End Time: {end_time_str}
    Run Tags: {dagster_run.tags}
    Run Config: {dagster_run.run_config}
    Dagit Run Link: {dagit_url}

    Error Message:
    {failure_event.message}

    Stack Trace:
    {failure_event.error.stack_trace if failure_event.error else 'No stack trace available.'}
    """

    return email_body


def create_email_on_failure_sensor(email_to: List[str] = ["brian.padilla@farinter.com"],
                                    monitored_jobs: Sequence[JobDefinition | GraphDefinition | UnresolvedAssetJobDefinition | RepositorySelector | JobSelector] | None = None,
                                    name: str | None = None,
                                    default_status: DefaultSensorStatus = DefaultSensorStatus.STOPPED) -> SensorDefinition:
    """
    Create a job failure sensor that sends email via the SMTP protocol.

    Args:
        email_to (List[str]): The receipt email addresses to send the message to.
        name: (Optional[str]): The name of the sensor. Defaults to "email_on_job_failure".
        monitored_jobs (Optional[List[Union[JobDefinition, GraphDefinition, JobDefinition, RepositorySelector, JobSelector]]]):
            The jobs that will be monitored by this failure sensor. Defaults to None, which means the alert will be sent when any job in the repository fails. To monitor jobs in external repositories, use RepositorySelector and JobSelector.
        default_status (DefaultSensorStatus): Whether the sensor starts as running or not. The default
            status can be overridden from the Dagster UI or via the GraphQL API.

    Examples:
    """
    return make_email_on_run_failure_sensor(
        email_from=os.getenv('DAGSTER_EMAIL_ADDRESS'),
        email_password=os.getenv('DAGSTER_SECRET_EMAIL_PASSWORD'),  # Use environment variables for sensitive information
        email_to=email_to,
        email_subject_fn=lambda _: "Dagster Job Failure Alert",
        email_body_fn=custom_email_body,
        monitored_jobs=monitored_jobs,
        smtp_host="mail.farinter.com",
        smtp_port=26,
        smtp_type="STARTTLS",
        default_status=default_status,
        name=name
    )
