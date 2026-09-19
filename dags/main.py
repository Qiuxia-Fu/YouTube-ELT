from airflow import DAG
import pendulum
from datetime import datetime, timedelta
from api.video_stats import get_playlist_id, get_video_ids, extract_video_data, save_to_json
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from datawarehouse.dwh import staging_table, core_table
from dataquality.soda import yt_elt_data_quality
# define local timezone
local_tz = pendulum.timezone("Europe/Stockholm")

# default args
default_args = {
    "owner": "dataengineers",
    "depends_on_past": "False",
    "email_on_failure": "False",
    "email_on_retry": "False",
    "email": "data@engineers.com",
    # 'retries':1,
    # 'retry_delay':timedelta(minutes=5),
    "max_active_runs": 1,
    "dagrun_timeout": timedelta(hours=1),
    "start_date": datetime(2026, 1, 1, tzinfo=local_tz),
    # 'end_date':datetime(2030,12,31,tzinfo=local_tz)
}

# variables
staging_schema = "staging"
core_schema = "core"

# DAG 1: produce json
with DAG(
    dag_id='produce_json',
    default_args=default_args,
    description="DAG to produce JSON file with raw data",
    schedule='0 10 * * *',
    catchup=False
) as dag_produce:

    # define tasks
    playlist_id = get_playlist_id()
    video_ids = get_video_ids(playlist_id)
    extract_data = extract_video_data(video_ids)
    save_to_json_task = save_to_json(extract_data)

    trigger_update_db = TriggerDagRunOperator(
        task_id="trigger_update_db",
        trigger_dag_id="update_db",
    )

    # define dependencies
    playlist_id >> video_ids >> extract_data >> save_to_json_task >> trigger_update_db

# DAG 2: update db
with DAG(
    dag_id='update_db',
    default_args=default_args,
    description="DAG to process JSON file and insert data into both staging and core schemas",
    schedule=None,
    catchup=False
) as dag_update:

    # define tasks
    update_staging = staging_table()
    update_core = core_table()

    trigger_data_quality = TriggerDagRunOperator(
        task_id="trigger_data_quality",
        trigger_dag_id="data_quality",
    )

    # define dependencies
    update_staging >> update_core >> trigger_data_quality

# DAG 3: data quality
with DAG(
    dag_id='data_quality',
    default_args=default_args,
    description="DAG to check the data quality on both layers in the db",
    schedule=None,
    catchup=False
) as dag_quality:

    # define tasks
    soda_validate_staging = yt_elt_data_quality(staging_schema)
    soda_validate_core = yt_elt_data_quality(core_schema)

    # define dependencies
    soda_validate_staging >> soda_validate_core
