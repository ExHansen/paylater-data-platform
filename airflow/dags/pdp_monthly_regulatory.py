"""Monthly regulatory report (previous month) on the 1st at 02:00 WIB."""

from datetime import timedelta

import pendulum
from airflow.sdk import dag, task

from pdp.tasks import record_task_failure

DEFAULT_ARGS = {
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": record_task_failure,
}


@dag(
    dag_id="pdp_monthly_regulatory",
    schedule="0 2 1 * *",
    start_date=pendulum.datetime(2026, 1, 1, tz="Asia/Jakarta"),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["pdp", "reporting"],
)
def pdp_monthly_regulatory():
    @task
    def regulatory_report(**context) -> str:
        from pdp.tasks import regulatory_previous_month

        moment = context.get("logical_date") or context.get("data_interval_end")
        return str(regulatory_previous_month(moment))

    regulatory_report()


pdp_monthly_regulatory()
