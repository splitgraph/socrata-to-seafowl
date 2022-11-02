import itertools
import logging
import os
import random
import string
from datetime import datetime
from typing import Any, Dict, List, NamedTuple, Optional

import click
import click_log
from requests import HTTPError
from splitgraph.cloud import GQLAPIClient, _handle_gql_errors
from splitgraph.cloud.models import ExportJobStatus
from splitgraph.cloud.queries import EXPORT_JOB_STATUS
from splitgraph.commandline.common import wait_for_job
from tqdm import tqdm

from .notdbt import build_models
from .seafowl import SEAFOWL_SCHEMA, SEAFOWL_TABLE, emit_value, query_seafowl

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

ALL_IMAGES_AND_TAGS = """
query AllSocrataImages {
  images(orderBy:CREATED_ASC, condition:{namespace:"splitgraph", repository:"socrata"}) {
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


def get_export_job_status(
    client: GQLAPIClient, task_id: str
) -> Optional[ExportJobStatus]:
    response = client._gql(
        {
            "query": EXPORT_JOB_STATUS,
            "operationName": "ExportJobStatus",
            "variables": {"taskId": task_id},
        },
        handle_errors=True,
        anonymous_ok=True,
    )

    data = response.json()["data"]["exportJobStatus"]
    if not data:
        return None
    return ExportJobStatus(
        task_id=data["taskId"],
        started=data["started"],
        finished=data["finished"],
        status=data["status"],
        user_id=data["userId"],
        export_format=data["exportFormat"],
        output=data["output"],
    )


def wait_for_download(client: "GQLAPIClient", task_id: str) -> str:
    final_status = wait_for_job(task_id, lambda: get_export_job_status(client, task_id))
    if final_status.status == "SUCCESS":
        assert final_status.output
        return str(final_status.output["url"])
    else:
        raise ValueError(
            "Error running query. This could be due to a syntax error. "
            "Run the query interactively with `sgr cloud sql` to investigate the cause."
        )


def get_socrata_download_url(
    client: GQLAPIClient, image_hash: str, attempts: int = 3
) -> str:
    attempt = 1

    while True:
        try:
            task_id = start_export(
                client,
                query=f'SELECT * FROM "splitgraph/socrata:{image_hash}".datasets',
                export_format="parquet",
            )
            download_url = wait_for_download(client, task_id)
            return download_url
        except Exception as e:
            if attempt == attempts:
                raise
            logger.warning("Error preparing the download URL, retrying", exc_info=e)
            attempt += 1


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


def get_socrata_images(client: GQLAPIClient, daily: bool = True) -> List[SocrataImage]:
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

    # If we only care about daily data, leave the first image in any given day (taken shortly after
    # midnight). This is because then the daily image won't change throughout the day -- also
    # because then the image for 2022-10-31 will incorporate just the changes from the previous day,
    # not some changes from 2022-10-31.
    if daily:
        output = [
            next(g)
            for _, g in itertools.groupby(
                sorted(output, key=lambda o: o.created), key=lambda o: o.created.date()
            )
        ]

    return output


def generate_insert(
    image_hash: str, image_tag: str, image_created: datetime, download_url: str
) -> str:
    table_name = random_table_name()
    staging_table = f"tmp_{table_name}"

    external_query = f"CREATE EXTERNAL TABLE {staging_table} STORED AS PARQUET LOCATION {emit_value(download_url)}"

    insert_query = (
        f"INSERT INTO {SEAFOWL_SCHEMA}.{SEAFOWL_TABLE} "
        "(" + ", ".join(c for c in SCHEMA.keys()) + ") "
        "SELECT "
        + ", ".join(c for c in SCHEMA.keys() if not c.startswith("sg_"))
        + ", "
        f"{emit_value(image_hash)} AS sg_image_hash, "
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
    try:
        result = (
            query_seafowl(
                seafowl,
                f"SELECT DISTINCT sg_image_hash, sg_image_tag, sg_image_created "
                f"FROM {SEAFOWL_SCHEMA}.{SEAFOWL_TABLE} ORDER BY sg_image_created ASC",
                access_token,
            )
            or []
        )
    except HTTPError as e:
        if (
            "Internal error: CoalescePartitionsExec requires at least one input partition."
            in str(e.response.text)
        ):
            # TODO: weird bug that only manifests on fly.io (??!?!?!?!)
            logging.warning(
                "Saw CoalescePartitionsExec error, assuming the table is empty"
            )
            return []
        else:
            raise

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
    max_images: Optional[int] = None,
    daily: bool = True,
) -> None:
    click.echo("Getting all current Socrata images")
    all_images = get_socrata_images(client, daily=daily)

    click.echo(f"Got {len(all_images)} image(s)")

    click.echo("Connecting to Seafowl and getting the latest Socrata images")
    ensure_seafowl_schema(seafowl, access_token)
    seafowl_images = get_seafowl_socrata_tags(seafowl, access_token)
    latest_seafowl = (
        max(seafowl_images, key=lambda s: s.created) if seafowl_images else None
    )

    click.echo(
        f"Seafowl contains {len(seafowl_images)} image(s), latest {latest_seafowl}"
    )

    seafowl_image_tags = {i.image_tag for i in seafowl_images}

    to_ingest = [i for i in all_images if i.image_tag not in seafowl_image_tags]

    to_ingest = sorted(to_ingest, key=lambda i: i.created)[
        : (max_images or len(to_ingest))
    ]

    if not to_ingest:
        click.echo("Nothing to do.")
        return

    click.echo(
        f"Need to ingest {len(to_ingest)} image(s) ({to_ingest[0].image_tag} .. {to_ingest[-1].image_tag})"
    )

    if dry_run:
        click.echo("Dry run, returning.")
        return

    with tqdm(to_ingest) as pbar:
        for image in pbar:
            pbar.set_description(image.image_tag)
            ingest_image_into_seafowl(client, seafowl, image, access_token=access_token)

    click.echo("All done")


@click.command(name="ingest")
@click_log.simple_verbosity_option(logger)
@click.argument("splitgraph_remote", default="data.splitgraph.com")
@click.argument("seafowl", default="http://localhost:8080")
@click.option("-x", "--execute", is_flag=True)
@click.option(
    "--max-images", default=None, type=int, help="Maximum number of images to sync"
)
def ingest(splitgraph_remote, seafowl, execute, max_images):
    access_token = os.getenv("SEAFOWL_PASSWORD")
    if not access_token:
        click.echo("Not using an access token")

    client = GQLAPIClient(splitgraph_remote)
    run_ingestion(
        client, seafowl, access_token, dry_run=not execute, max_images=max_images
    )


@click.command(name="build")
@click.argument("seafowl", default="http://localhost:8080")
@click_log.simple_verbosity_option(logger)
def build_tables(seafowl):
    """Build frozen SQL tables"""
    access_token = os.getenv("SEAFOWL_PASSWORD")
    if not access_token:
        click.echo("Not using an access token")

    build_models(seafowl, access_token)


@click.group()
def main():
    pass


main.add_command(ingest)
main.add_command(build_tables)

if __name__ == "__main__":
    main()
