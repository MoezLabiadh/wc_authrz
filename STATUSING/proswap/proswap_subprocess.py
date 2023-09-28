import subprocess

def run_proswap(env_name):
    command = f"proswap {env_name}"
    process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    output, error = process.communicate()

    if process.returncode != 0:
        print(f"Error occurred: {error.decode('utf-8')}")
    else:
        print(f"Output: {output.decode('utf-8')}")
