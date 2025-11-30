import time

import boto3

from common import resource_name

athena = boto3.client("athena")

GLUE_DATABASE = resource_name("ingesta")
OUTPUT_LOCATION = f"s3://{resource_name('ingesta')}/results"


def run_athena_query(sql: str):
    resp = athena.start_query_execution(
        QueryString=sql,
        QueryExecutionContext={"Database": GLUE_DATABASE},
        ResultConfiguration={"OutputLocation": OUTPUT_LOCATION},
    )
    exec_id = resp["QueryExecutionId"]

    while True:
        status = athena.get_query_execution(QueryExecutionId=exec_id)
        state = status["QueryExecution"]["Status"]["State"]

        if state in ("SUCCEEDED", "FAILED", "CANCELLED"):
            break

        time.sleep(0.5)

    if state != "SUCCEEDED":
        raise RuntimeError(f"Athena query failed with state={state}")

    return athena.get_query_results(QueryExecutionId=exec_id)
