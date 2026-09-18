from airflow import DAG
import pendulum
from datetime import datetime, timedelta
from api.video_stats import get_playlist_id, get_video_ids, extract_video_data, save_to_json

from datawarehouse.dwh import staging_table, core_table
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

with DAG(
    dag_id='update_db',
    default_args=default_args,
    description="DAG to process JSON file and insert data into both staging and core scehmas",
    schedule='0 20 * * *',
    catchup=False
) as dag:

    # default tasks
    update_staging = staging_table()
    update_core = core_table()

    # define dependencies
    update_staging >> update_core
