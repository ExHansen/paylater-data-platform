# Airflow 3.3 with the pdp package (Airflow's constraints) and dbt in its own virtualenv.
FROM apache/airflow:3.3.2-python3.11

USER root
RUN mkdir -p /opt/pdp && chown airflow:root /opt/pdp
USER airflow

COPY --chown=airflow:root pyproject.toml README.md /opt/pdp/
COPY --chown=airflow:root src /opt/pdp/src
COPY --chown=airflow:root infra /opt/pdp/infra
COPY --chown=airflow:root dbt /opt/pdp/dbt
COPY --chown=airflow:root airflow/dags /opt/airflow/dags

# Editable install keeps infra/contracts next to the package (used by the schema-drift check).
RUN pip install --no-cache-dir \
      --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-3.3.2/constraints-3.11.txt" \
      -e /opt/pdp

# dbt lives in its own virtualenv so its dependencies never fight Airflow's.
RUN python -m venv /opt/pdp/dbt-venv \
 && /opt/pdp/dbt-venv/bin/pip install --no-cache-dir "dbt-core==1.12.*" "dbt-postgres==1.11.*"
