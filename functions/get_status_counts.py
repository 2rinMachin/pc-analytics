from common import resource_name, response
from common.athena import run_athena_query


def handler(event, context):
    tenant_id = event["pathParameters"]["tenant_id"]

    sql = f"""
        select
            status,
            count(*) as count
        from "{resource_name('orders')}"
        where tenant_id = '{tenant_id}'
        group by status;
    """

    results = run_athena_query(sql)
    rows = results["ResultSet"]["Rows"]

    # First row is the header
    header = [col["VarCharValue"] for col in rows[0]["Data"]]

    out = {}
    for row in rows[1:]:
        values = [col.get("VarCharValue") for col in row["Data"]]
        record = dict(zip(header, values))
        out[record["status"]] = int(record["count"])

    return response(200, out)
