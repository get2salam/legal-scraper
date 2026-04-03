import subprocess
r = subprocess.run(
    ['wmic', 'process', 'where', 'name="python.exe"', 'get', 'ProcessId,CommandLine', '/format:list'],
    capture_output=True, text=True
)
print(r.stdout)
