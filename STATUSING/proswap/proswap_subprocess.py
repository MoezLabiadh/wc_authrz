import subprocess

def run_proswap(proswap_path, env_path):
    """Runs proswap.bat to change Arcpro active python environment"""
    command = f"{proswap_path} {env_path}"
    process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    output, error = process.communicate()

    if process.returncode != 0:
        print(f"Error occurred: {error.decode('utf-8')}")
    else:
        print(f"SUCESS: {output.decode('utf-8')}")


proswap_path= r'E:\sw_nt\ArcGIS\Pro\bin\Python\Scripts\proswap.bat'
env_path= r'P:\corp\python_ast'
run_proswap(proswap_path, env_path)
