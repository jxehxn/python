import subprocess

def run_script(script_name):
    result = subprocess.run(['python', script_name], capture_output=True, text=True)
    print(f"Running {script_name}")
    print("Output:")
    print(result.stdout)
    if result.stderr:
        print("Errors:")
        print(result.stderr)

scripts = [
    '과거7일치해수온도데이터.py',
    '과거7일치기상데이터.py', 
    '과거7일치대기질데이터.py',
    '데이터병합하자.py',
    '모델예측.py',
    'mysqldb연결후solar_env_info데이터삽입.py',
    'mysqldb연결후prediction_info데이터삽입.py'
]

for script in scripts:
    run_script(script)
