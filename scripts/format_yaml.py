import subprocess
import os


def get_files_with_yamllint_errors(include_errors=False):
    result = subprocess.run(
        ["yamllint", "-f", "parsable", ".", "--no-warnings"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        check=False,
    )
    files = set()
    for line in result.stdout.splitlines():
        if line.strip():
            file_path = line if include_errors else line.split(":", 1)[0]
            files.add(file_path)
    return files


def format_with_yamlfix(file_path):
    try:
        result = subprocess.run(
            ["yamlfix", file_path],
            capture_output=True,
            text=True,
            check=True,
        )
        print(result.stdout)
    except subprocess.CalledProcessError as e:
        print(f"Error formatting {file_path}:\n{e.stderr}")
        raise


def main():
    files = list(get_files_with_yamllint_errors())
    skip_all = False
    for file_path in files:
        if os.path.isfile(file_path):
            print(f"Formatting {file_path}...")
            if not skip_all:
                while True:
                    user_input = (
                        input("(c)ontinue, (s)kip, (a)ll continue (q)uit? [c/s/a/q]: ")
                        .strip()
                        .lower()
                    )
                    if user_input == "c" or user_input == "":
                        break
                    elif user_input == "s":
                        file_path = None
                        break
                    elif user_input == "a":
                        skip_all = True
                        break
                    elif user_input == "q":
                        exit()
                    else:
                        print("Invalid input. Please enter 'c', 's', or 'a'.")
                if file_path is None:
                    continue
            try:
                format_with_yamlfix(file_path)
            except subprocess.CalledProcessError:
                pass


if __name__ == "__main__":
    main()
    restantes = get_files_with_yamllint_errors(include_errors=True)
    if restantes:
        print("Archivos restantes:")
        for archivo in restantes:
            print(archivo)
    else:
        print("No hay archivos restantes con errores de YAML.")
