import os
import subprocess
from jinja2 import Environment, FileSystemLoader
from dotenv import load_dotenv

# Deploys on ubuntu server

# Set the directory where your templates are stored
template_dir = os.path.join(os.path.dirname(__file__), 'templates')
env_jinja = Environment(loader=FileSystemLoader(template_dir))

def deploy_service(instance_env: str, template_filename: str) -> None:
    """
    Render and deploy a service from a given template.

    Args:
        instance_env (str): The current instance environment e.g. "local", "prd", or "dev".
        template_filename (str): The template file name (e.g. "dagster_webserver.service.template").
    """
    # Load and render the template using current environment variables.
    template = env_jinja.get_template(template_filename)
    rendered_service = template.render(env=os.environ)
    
    # Derive the service name by removing the template extension and injecting the instance environment.
    # For example, "dagster_webserver.service.template" becomes "dagster_prd_webserver.service"
    base_service_name = template_filename.replace(".template", "")
    # Replace the default env in the name if present or inject it
    if "dagster_" in base_service_name:
        output_filename = base_service_name.replace("dagster_", f"dagster_{instance_env}_")
    else:
        output_filename = f"dagster_{instance_env}_{base_service_name}"
    
    # Create templates_render directory if it doesn't exist
    output_dir = os.path.join(os.path.dirname(__file__), 'templates_render')
    os.makedirs(output_dir, exist_ok=True)

    # Write the rendered service file to the templates_render folder.
    output_path = os.path.join(output_dir, output_filename)
    
    with open(output_path, "w") as f:
        f.write(rendered_service)
    print(f"Final service file written to: {output_path}")

    # Deploy only when the environment is "prd" or "dev"
    if instance_env in ["prd", "dev"]:
        print(f"Deploying {output_filename} to {instance_env} environment...")
        system_service_path = f"/etc/systemd/system/{output_filename}"

        def file_contents(path):
            try:
                with open(path, "r") as file:
                    return file.read()
            except FileNotFoundError:
                return None

        existing_service = file_contents(system_service_path)
        if existing_service is not None and existing_service == rendered_service:
            print(f"No deployment needed. {output_filename} is unchanged.")
        else:
            subprocess.run(["cp", output_path, system_service_path], check=True)
            subprocess.run(["systemctl", "daemon-reload"], check=True)
            subprocess.run(["systemctl", "enable", "--now", output_filename], check=True)
            print(f"Service {output_filename} deployed and enabled.")
    else:
        print(f"Skipping deployment of {output_filename} to {instance_env} environment.")

if __name__ == "__main__":
    # Determine the current instance, loading .env file if necessary
    instance_env = os.environ.get("DAGSTER_INSTANCE_CURRENT_ENV", "default")
    if instance_env == "default":
        print("Loading default environment variables from .env")
        repo_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ""))
        env_file = os.path.join(repo_root, '.env')
        load_dotenv(dotenv_path=env_file)
        instance_env = os.environ.get("DAGSTER_INSTANCE_CURRENT_ENV", "default")

    assert instance_env in ["local", "prd", "dev"], "DAGSTER_INSTANCE_CURRENT_ENV must be 'local', 'prd' or 'dev'"

    # Tuple of service template filenames to deploy.
    service_templates = (
        "dagster_webserver.service.template",
        "dagster_daemon.service.template",
    )

    # Call deploy_service for each template in the tuple.
    for tmpl in service_templates:
        deploy_service(instance_env, tmpl)
