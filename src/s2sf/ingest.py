import json
import logging
import math
import os
import random
import string
from datetime import datetime
from numbers import Number
from typing import Any, Dict, List, NamedTuple, Optional

import click
import click_log
import requests
from splitgraph.cloud import GQLAPIClient, _handle_gql_errors
from splitgraph.commandline.cloud import wait_for_download

logger = logging.getLogger(__name__)
click_log.basic_config(logger)

SCHEMA = {
    "id": "TEXT",
    "domain": "TEXT",
    "name": "TEXT",
    "description": "TEXT",
    "updated_at": "TIMESTAMP",
    "created_at": "TIMESTAMP",
    "resource": "TEXT",
    "classification": "TEXT",
    "metadata": "TEXT",
    "permalink": "TEXT",
    "link": "TEXT",
    "owner": "TEXT",
    "sg_image_hash": "TEXT",
    "sg_image_tag": "TEXT",
    "sg_image_created": "TIMESTAMP",
}

SEAFOWL_SCHEMA = "socrata"
SEAFOWL_TABLE = "dataset_history"

ALL_IMAGES_AND_TAGS = """
query AllSocrataImages {
  images(orderBy:CREATED_DESC, condition:{namespace:"splitgraph", repository:"socrata"}) {
    nodes {
      created
      imageHash
      tagsByNamespaceAndRepositoryAndImageHash {
        nodes {
          tag
        }
      }
    }
  }
}
"""


class SocrataImage(NamedTuple):
    image_hash: str
    image_tag: str
    created: datetime


def emit_value(value: Any) -> str:
    if value is None:
        return "NULL"

    if isinstance(value, float):
        if math.isnan(value):
            return "NULL"
        return f"{value:.20f}"

    if isinstance(value, Number) and not isinstance(value, bool):
        return str(value)

    if isinstance(value, datetime):
        return f"'{value.isoformat()}'"

    quoted = str(value).replace("'", "''")
    return f"'{quoted}'"


START_EXPORT = """mutation StartExport($query: String!, $format: String! = "csv") {
  exportQuery(query: $query, exportFormat: $format) {
    id
  }
}
"""


# "vendored" from Splitgraph to add support for formats
def start_export(
    client: GQLAPIClient, query: str, export_format: Optional[str] = None
) -> str:
    variables = {"query": query}
    if export_format:
        variables["format"] = export_format
    response = client._gql(
        {
            "query": START_EXPORT,
            "operationName": "StartExport",
            "variables": variables,
        },
        handle_errors=True,
        anonymous_ok=True,
    )
    return str(response.json()["data"]["exportQuery"]["id"])


def get_socrata_download_url(client: GQLAPIClient, image_hash: str) -> str:
    task_id = start_export(
        client,
        query=f'SELECT * FROM "splitgraph/socrata:{image_hash}".datasets',
        export_format="parquet",
    )
    download_url = wait_for_download(client, task_id)
    return download_url


def _get_tag(image_node: Dict[str, Any]) -> Optional[str]:
    # Extract long-form tags, e.g. 20221027-120131
    tags = sorted(
        [
            t["tag"]
            for t in image_node["tagsByNamespaceAndRepositoryAndImageHash"]["nodes"]
            if len(t["tag"]) == 15
        ],
        reverse=True,
    )
    return tags[0] if tags else None


def get_socrata_images(client: GQLAPIClient) -> List[SocrataImage]:
    response = client._gql(
        {
            "operationName": "AllSocrataImages",
            "variables": {},
            "query": ALL_IMAGES_AND_TAGS,
        },
        anonymous_ok=True,
    )
    _handle_gql_errors(response)
    result = response.json()

    output: List[SocrataImage] = []
    for node in result["data"]["images"]["nodes"]:
        tag = _get_tag(node)
        if not tag:
            continue
        output.append(
            SocrataImage(
                image_hash=node["imageHash"],
                created=datetime.strptime(node["created"], "%Y-%m-%dT%H:%M:%S.%f"),
                image_tag=tag,
            )
        )

    return output


def query_seafowl(endpoint: str, sql: str, access_token: Optional[str] = None) -> Any:
    headers = {"Content-Type": "application/json"}
    if access_token:
        headers["Authorization"] = f"Bearer {access_token}"
    response = requests.post(f"{endpoint}/q", json={"query": sql}, headers=headers)

    if not response.ok:
        logger.error(response.text)
    response.raise_for_status()
    if response.text:
        return [json.loads(t) for t in response.text.strip().split("\n")]
    return None


def generate_insert(
    image_hash: str, image_tag: str, image_created: datetime, download_url: str
) -> str:
    table_name = random_table_name()
    staging_table = f"tmp_{table_name}"

    external_query = f"CREATE EXTERNAL TABLE {staging_table} STORED AS PARQUET LOCATION {emit_value(download_url)}"

    insert_query = (
        f"INSERT INTO {SEAFOWL_SCHEMA}.{SEAFOWL_TABLE} "
        f"SELECT *, {emit_value(image_hash)} AS sg_image_hash, "
        f"{emit_value(image_tag)} AS sg_image_tag, "
        f"{emit_value(image_created)} AS sg_image_created "
        f"FROM staging.{staging_table}"
    )

    return external_query + ";\n" + insert_query


def generate_create_seafowl_table() -> str:
    return (
        f"CREATE TABLE {SEAFOWL_SCHEMA}.{SEAFOWL_TABLE} ("
        + ",\n".join(f"{cname} {ctype}" for cname, ctype in SCHEMA.items())
        + ")"
    )


def ensure_seafowl_schema(seafowl: str, access_token: Optional[str]) -> None:
    schema_exists = query_seafowl(
        seafowl,
        f"SELECT 1 AS exists FROM information_schema.tables "
        f"WHERE table_schema = {emit_value(SEAFOWL_SCHEMA)}",
    )
    table_exists = query_seafowl(
        seafowl,
        f"SELECT 1 AS exists FROM information_schema.tables "
        f"WHERE table_schema = {emit_value(SEAFOWL_SCHEMA)} AND table_name = {emit_value(SEAFOWL_TABLE)}",
    )

    if not schema_exists:
        query_seafowl(
            seafowl, f"CREATE SCHEMA {SEAFOWL_SCHEMA}", access_token=access_token
        )
    if not table_exists:
        query_seafowl(
            seafowl, generate_create_seafowl_table(), access_token=access_token
        )


def get_seafowl_socrata_tags(
    seafowl: str, access_token: Optional[str]
) -> List[SocrataImage]:
    result = (
        query_seafowl(
            seafowl,
            f"SELECT DISTINCT sg_image_hash, sg_image_tag, sg_image_created "
            f"FROM {SEAFOWL_SCHEMA}.{SEAFOWL_TABLE} ORDER BY sg_image_created ASC",
            access_token,
        )
        or []
    )

    return [
        SocrataImage(
            image_hash=i["sg_image_hash"],
            image_tag=i["sg_image_tag"],
            created=datetime.strptime(i["sg_image_created"], "%Y-%m-%d %H:%M:%S.%f"),
        )
        for i in result
    ]


def random_table_name() -> str:
    return "".join(random.choices(string.ascii_lowercase, k=16))


def ingest_image_into_seafowl(
    client: GQLAPIClient, seafowl: str, image: SocrataImage, access_token: Optional[str]
) -> None:
    click.echo("Ingesting image %s" % (image,))
    download_url = get_socrata_download_url(client, image_hash=image.image_hash)
    query = generate_insert(
        image.image_hash, image.image_tag, image.created, download_url
    )
    query_seafowl(seafowl, query, access_token=access_token)


def run_ingestion(
    client: GQLAPIClient,
    seafowl: str,
    access_token: Optional[str],
    dry_run: bool = False,
) -> None:
    click.echo("Getting all current Socrata images")
    all_images = get_socrata_images(client)

    click.echo("Got %d image(s)" % len(all_images))

    click.echo("Connecting to Seafowl and getting the latest Socrata images")
    ensure_seafowl_schema(seafowl, access_token)
    seafowl_images = get_seafowl_socrata_tags(seafowl, access_token)
    latest_seafowl = (
        max(seafowl_images, key=lambda s: s.created) if seafowl_images else None
    )

    click.echo(
        "Seafowl contains %d image(s), latest %s"
        % (len(seafowl_images), latest_seafowl)
    )

    if not latest_seafowl:
        to_ingest = all_images
    else:
        to_ingest = [i for i in all_images if i.created > latest_seafowl.created]

    if not to_ingest:
        click.echo("Nothing to do.")
        return

    click.echo("Need to ingest %d image(s)" % len(to_ingest))

    if dry_run:
        click.echo("Dry run, returning.")
        return

    for image in to_ingest:
        ingest_image_into_seafowl(client, seafowl, image, access_token=access_token)

    click.echo("All done")


@click.command(name="ingest")
@click_log.simple_verbosity_option(logger)
@click.argument("splitgraph_remote", default="data.splitgraph.com")
@click.argument("seafowl", default="http://localhost:8080")
@click.option("-x", "--execute", is_flag=True)
def main(splitgraph_remote, seafowl, execute):
    access_token = os.getenv("SEAFOWL_PASSWORD")
    if not access_token:
        click.echo("Not using an access token")

    client = GQLAPIClient(splitgraph_remote)
    run_ingestion(client, seafowl, access_token, dry_run=not execute)


if __name__ == "__main__":
    main()
