from common import response


def handler(event, context):
    return response(200, {"ayo": "asdf"})
