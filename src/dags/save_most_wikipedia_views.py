from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

dag = DAG(
    "save_most_wikipedia_views_temp1",
    template_searchpath="/tmp",
    schedule_interval=None,
)


def _print_context(**context):
    print(context)


print_context = PythonOperator(
    task_id="print_context",
    python_callable=_print_context,
    dag=dag,
)


download_data = BashOperator(
    task_id="download_data",
    bash_command=(
        "curl -o /tmp/wikipedia_views_{{ ds_nodash }}{{ '{:02d}'.format(execution_date.hour) }}0000.gz "
        "https://dumps.wikimedia.org/other/pageviews/"
        "{{ execution_date.year }}/{{ execution_date.year }}"
        "-{{ '{:02d}'.format(execution_date.month) }}"
        "/pageviews-{{ execution_date.year }}"
        "{{ '{:02d}'.format(execution_date.month) }}"
        "{{ '{:02d}'.format(execution_date.day) }}"
        "-{{ '{:02d}'.format(execution_date.hour) }}0000.gz"
    ),
    dag=dag,
)

extract_data = BashOperator(
    task_id="extract_data",
    bash_command=(
        "gzip -qd -c /tmp/wikipedia_views_{{ ds_nodash }}{{ '{:02d}'.format(execution_date.hour) }}0000.gz "
        "> /tmp/wikipedia_views_{{ ds_nodash }}{{ '{:02d}'.format(execution_date.hour) }}0000"
    ),
    dag=dag,
)


def _fetch_most_page_views(ds, ds_nodash, execution_date, **context):
    target_page_names = ["Google", "Apple", "Microsoft"]
    result = []
    hour = "{:02d}".format(execution_date.hour)
    suffix = f"{ds_nodash}{hour}0000"
    with open(f"/tmp/wikipedia_views_{suffix}", "r") as f:
        for line in f:
            domain_code, page_title, view_count, _ = line.split(" ")
            if page_title in target_page_names and domain_code in {"en", "en.m"}:
                result.append((page_title, domain_code, int(view_count)))

    for page_title, domain_code, view_count in result:
        print(f"Page: {page_title}, Domain Code: {domain_code}, Views: {view_count}")

    output_file_name = f"page_views.sql"
    with open(f"/tmp/{output_file_name}", "w") as f:
        for page_title, domain_code, view_count in result:
            f.write(
                f"INSERT INTO application.wiki_pageview_counts (page_title, domain_code, datetime, view_count) VALUES ('{page_title}', '{domain_code}', '{ds}T{hour}:00:00', {view_count});\n"
            )


fetch_page_views = PythonOperator(
    task_id="fetch_most_page_views",
    python_callable=_fetch_most_page_views,
    dag=dag,
)


write_to_db = PostgresOperator(
    task_id="write_to_db",
    postgres_conn_id="postgres_default",
    sql="page_views.sql",
    dag=dag,
)

print_context >> download_data >> extract_data >> fetch_page_views >> write_to_db
