from common import resource_name, response
from common.athena import run_athena_query


def handler(event, context):
    tenant_id = event["pathParameters"]["tenant_id"]

    sql = f"""
        select
            i.product.product_id,
            max(p.name) as product_name,
            sum(i.quantity) as total_quantity
        from "{resource_name('orders')}" o
            cross join unnest(o.items) as t(i)
            join "{resource_name('products')}" p on p.product_id = i.product.product_id
        where o.tenant_id = '{tenant_id}'
        group by i.product.product_id
        order by total_quantity desc
        limit 10
    """

    results = run_athena_query(sql)
    rows = results["ResultSet"]["Rows"]
    headers = [col["VarCharValue"] for col in rows[0]["Data"]]
    out = [
        {
            headers[i]: cell.get("VarCharValue", None)
            for i, cell in enumerate(row["Data"])
        }
        for row in rows[1:]
    ]

    return response(200, out)
