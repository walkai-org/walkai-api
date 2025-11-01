import json
import time

from boto3.dynamodb.conditions import Attr

TTL_SECONDS = 600


def _key(state: str) -> dict:
    return {"pk": f"oauth#{state}"}


def save_oauth_tx(ddb_table, state: str, data: dict) -> None:
    item = {
        "pk": f"oauth#{state}",
        "data": json.dumps(data),
        "expires_at": int(time.time()) + TTL_SECONDS,
    }
    ddb_table.put_item(
        Item=item,
        ConditionExpression="attribute_not_exists(pk)",
    )


def load_oauth_tx(ddb_table, state: str) -> dict | None:
    now = int(time.time())
    resp = ddb_table.delete_item(
        Key=_key(state),
        ReturnValues="ALL_OLD",
        ConditionExpression=Attr("expires_at").gt(now),
    )
    attrs = resp.get("Attributes")
    if not attrs:
        return None
    return json.loads(attrs["data"])
