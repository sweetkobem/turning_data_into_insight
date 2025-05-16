import sys
import base64
import pickle
import os
import yaml
import subprocess
from datetime import datetime

# Add folder pludgins/functions and import
paths = os.path.dirname(os.path.abspath(__file__)).split('/')
index_path = paths.index('dag_config')
sys.path.append(f"{'/'.join(paths[0:index_path])}/plugins")
from functions import general as gnrl


def main(execution_date, airflow_connection, airflow_variable):
    # SCD Type 2
    execution_date = datetime.strptime(execution_date, '%Y-%m-%d %H:%M:%S').date()
    execution_date = execution_date.strftime('%Y-%m-%d')

    airflow_home_path = airflow_variable['airflow_home_path']
    table = 'dim_patient'

    key_destination = 'dim_patient_key'
    path_schema = airflow_home_path + '/medalion/models/'+table+'/schema.yml'
    schema_yaml = open(path_schema, 'r')
    column_yaml = yaml.load(schema_yaml, Loader=yaml.FullLoader)

    schema = {}
    for column in column_yaml['models'][0]['columns']:
        schema[column['name']] = column['data_type']

    columns = [column for column in schema.keys()]
    filtered_column = gnrl.get_filtered_column(key_destination, columns)
    md5_column = gnrl.encoded_column_by_sql(filtered_column)

    # This script to execute DBT in virtual environtment
    dbt_executable = os.path.join(airflow_home_path+'/venv', 'bin', 'dbt')

    dbt_command = [
        dbt_executable,
        'run', '--project-dir', airflow_home_path+'/medalion',
        '--select', table,
        '--vars', '{"md5_column": "'+md5_column+'", "execution_date": "'+execution_date+'"}'
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
