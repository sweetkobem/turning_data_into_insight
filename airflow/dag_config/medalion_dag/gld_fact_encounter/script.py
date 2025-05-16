import sys
import base64
import pickle
import os
import subprocess
from datetime import datetime

# Add folder pludgins/functions and import
paths = os.path.dirname(os.path.abspath(__file__)).split('/')
index_path = paths.index('dag_config')
sys.path.append(f"{'/'.join(paths[0:index_path])}/plugins")


def main(execution_date, airflow_connection, airflow_variable):
    # SCD Type 7
    execution_date = datetime.strptime(execution_date, '%Y-%m-%d %H:%M:%S').date()
    execution_date = execution_date.strftime('%Y-%m-%d')

    airflow_home_path = airflow_variable['airflow_home_path']
    table = 'fact_encounter'

    # This script to execute DBT in virtual environtment
    dbt_executable = os.path.join(airflow_home_path+'/venv', 'bin', 'dbt')

    dbt_command = [
        dbt_executable,
        'run', '--project-dir', airflow_home_path+'/medalion',
        '--select', table,
        '--vars', '{"execution_date": "'+execution_date+'"}'
    ]

    # Run the dbt command
    result = subprocess.run(dbt_command, capture_output=True, text=True)
    if result.returncode == 0:
        print("Command was successful!")
    else:
        if result.stdout:
            print(f"stdout:\n{result.stdout}")
        if result.stderr:
            print(f"stderr:\n{result.stderr}")
        sys.exit(1)


if __name__ == '__main__':
    execution_date = sys.argv[1].replace('T', ' ')
    airflow_connection = pickle.loads(base64.b64decode(sys.argv[2]))
    airflow_variable = pickle.loads(base64.b64decode(sys.argv[3]))

    main(execution_date, airflow_connection, airflow_variable)
