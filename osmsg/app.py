# This file is part of OSMSG (https://github.com/kshitijrajsharma/OSMSG).
# MIT License

# Copyright (c) 2023 Kshitij Raj Sharma

# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:

# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.

# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

import argparse
import concurrent.futures
import datetime as dt
import json
import os
import re
import shutil
import sys
import time
import urllib.parse
from datetime import datetime

import geopandas as gpd
import humanize
import matplotlib.pyplot as plt
import osmium
import pandas as pd
from matplotlib.font_manager import FontProperties
from tqdm import tqdm


from .__version__ import __version__
from .changefiles import (
    get_download_urls_changefiles,
    get_prev_hour,
    get_prev_year_dates,
    in_local_timezone,
    last_days_count,
    previous_day,
    previous_month,
    previous_week,
    seq_to_timestamp,
    strip_utc,
)
from .changesets import ChangesetToolKit
from .login import verify_me_osm
from .models import (
    Action,
    Changeset,
    ChangesetStats,
    TagValueStat,
    User,
)
from .db import (
    create_tables,
    get_connection,
    insert_changeset_stats,
    insert_changesets,
    insert_users,
    prepare_changeset_row,
    prepare_stats_row,
    BATCH_SIZE,
    bbox_to_wkt,
)
from .utils import (
    create_charts,
    create_profile_link,
    download_osm_files,
    extract_projects,
    generate_tm_stats,
    get_bbox_centroid,
    get_editors_name_strapped,
    get_file_path_from_url,
    process_boundary,
    sum_tags,
    update_stats,
    update_summary,
)

from .output import (
    get_user_stats,
    get_summary_by_day,
    export_csv,
    export_json,
    export_excel,
    export_text,
    export_image,
    apply_update_stats,
    apply_update_summary,
    enrich_with_tm_stats,
    export_charts,
    export_summary_md,
    export_metadata,
)

from .processor import (
    worker_init,
    process_changefiles_worker,
    process_changesets_worker,
)

db_conn = None
field_mapping_editors = [
    "streetcomplete",
    "vespucci",
    "go map",
    "every door android",
    "organic maps android",
    "osmand",
    "every door ios",
    "organic maps ios",
    "osmand maps",
]

whitelisted_users = []


def Initialize():
    global countries_df
    global geofabrik_countries

    print("Initializing ....")
    # read the GeoJSON file
    countries_df = gpd.read_file("https://raw.githubusercontent.com/osgeonepal/OSMSG/master/data/countries_un.geojson")
    geofabrik_countries = pd.read_csv("https://raw.githubusercontent.com/osgeonepal/OSMSG/master/data/countries.csv")


def auth(username, password):
    print("Authenticating...")
    try:
        cookies = verify_me_osm(username, password)
    except Exception:
        raise ValueError("OSM Authentication Failed")

    print("Authenticated !")
    return cookies


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--version", action="version", version=f"%(prog)s {__version__}")
    parser.add_argument(
        "--start_date",
        help="Start date in the format YYYY-MM-DD HH:M:Sz eg: 2023-01-28 17:43:09+05:45",
    )
    parser.add_argument(
        "--end_date",
        help="End date in the format YYYY-MM-DD HH:M:Sz eg: 2023-01-28 17:43:09+05:45",
        default=dt.datetime.now(),
    )
    parser.add_argument(
        "--username",
        default=None,
        help="Your OSM Username : Only required for Geofabrik Internal Changefiles",
    )
    parser.add_argument(
        "--password",
        default=None,
        help="Your OSM Password : Only required for Geofabrik Internal Changefiles",
    )
    parser.add_argument(
        "--timezone",
        default="UTC",
        choices=["Nepal", "UTC"],
        help="Your Timezone : Currently Supported Nepal, Default : UTC",
    )

    parser.add_argument(
        "--name",
        default="stats",
        help="Output stat file name",
    )
    parser.add_argument(
        "--country",
        nargs="+",
        default=None,
        help="List of country name to extract (get id from data/countries), It will use geofabrik countries updates so it will require OSM USERNAME. Only Available for Daily Updates",
    )

    parser.add_argument(
        "--tags",
        nargs="+",
        default=None,
        type=str,
        help="Additional stats to collect : List of tags key",
    )

    parser.add_argument(
        "--hashtags",
        nargs="+",
        default=None,
        type=str,
        help="Hashtags Statistics to Collect : List of hashtags , Limited until daily stats for now , Only lookups if hashtag is contained on the string , not a exact string lookup on beta",
    )
    parser.add_argument(
        "--length",
        nargs="+",
        default=None,
        type=str,
        help="Calculate length of osm features , Only Supported for way created features , Pass list of tags key to calculate eg : --length highway waterway , Unit is in Meters",
    )

    parser.add_argument(
        "--force",
        action="store_true",
        help="Force for the Hashtag Replication fetch if it is greater than a day interval",
        default=False,
    )

    parser.add_argument(
        "--field_mappers",
        action="store_true",
        help="Filter stats by field mapping editors",
        default=False,
    )

    parser.add_argument(
        "--meta",
        action="store_true",
        help="Generates stats_metadata.json including sequence info , start_data end_date , Will be useful when running daily/weekly/monthly by service/cron",
        default=False,
    )

    parser.add_argument(
        "--tm_stats",
        action="store_true",
        help="Includes Tasking Manager stats for users , TM Projects are filtered from hashtags used , Appends all time stats for user for project id produced from stats",
        default=False,
    )

    parser.add_argument(
        "--rows",
        type=int,
        default=None,
        help="No. of top rows to extract , to extract top 100 , pass 100",
    )

    parser.add_argument(
        "--users",
        type=str,
        nargs="+",
        default=None,
        help="List of user names to look for , You can use it to only produce stats for listed users or pass it with hashtags , it will act as and filter. Case sensitive use ' ' to enter names with space in between",
    )

    parser.add_argument(
        "--workers",
        type=int,
        default=None,
        help="No. of Parallel workers to assign : Default is no of cpu available , Be aware to use this max no of workers may cause overuse of resources",
    )

    parser.add_argument(
        "--url",
        nargs="+",
        default=["https://planet.openstreetmap.org/replication/minute"],
        help="Your public list of OSM Change Replication URL , 'minute,hour,day' option by default will translate to planet replciation url. You can supply multiple urls for geofabrik country updates , Url should not have trailing / at the end",
    )

    parser.add_argument(
        "--last_week",
        action="store_true",
        help="Extract stats for last week",
        default=False,
    )
    parser.add_argument(
        "--last_day",
        action="store_true",
        help="Extract Stats for last day",
        default=False,
    )
    parser.add_argument(
        "--last_month",
        action="store_true",
        help="Extract Stats for last Month",
        default=False,
    )
    parser.add_argument(
        "--last_year",
        action="store_true",
        help="Extract stats for last year",
        default=False,
    )
    parser.add_argument(
        "--last_hour",
        action="store_true",
        help="Extract stats for Last hour",
        default=False,
    )
    parser.add_argument(
        "--days",
        type=int,
        default=None,
        help="N nof of last days to extract , for eg if 3 is supplied script will generate stats for last 3 days",
    )
    parser.add_argument(
        "--charts",
        action="store_true",
        help="Exports Summary Charts along with stats",
        default=False,
    )
    parser.add_argument(
        "--summary",
        action="store_true",
        help="Produces Summary.md file with summary of Run and also a summary.csv which will have summary of stats per day",
        default=False,
    )
    parser.add_argument(
        "--exact_lookup",
        action="store_true",
        help="Exact lookup for hashtags to match exact hashtag supllied , without this hashtag search will search for the existence of text on hashtags and comments",
        default=False,
    )

    parser.add_argument(
        "--changeset",
        help="Include hashtag and country informations on the stats. It forces script to process changeset replciation , Careful to use this since changeset replication is minutely according to your internet speed and cpu cores",
        action="store_true",
        default=False,
    )

    parser.add_argument(
        "--all_tags",
        action="store_true",
        help="Extract statistics of all of the unique tags and its count",
        default=False,
    )

    parser.add_argument(
        "--key_value",
        action="store_true",
        help="Extracts stats for Unique combination of tags key and value by default it will count for unique key on --all_tags",
        default=False,
    )

    parser.add_argument(
        "--temp",
        action="store_true",
        help="Deletes downloaded osm files from machine after processing is done , if you want to run osmsg on same files again keep this option turn off",
        default=False,
    )

    parser.add_argument(
        "--format",
        nargs="+",
        choices=["csv", "json", "excel", "image", "text"],
        default="csv",
        help="Stats output format",
    )
    parser.add_argument(
        "--read_from_metadata",
        help="Location of metadata to pick start date from previous run's end_date , Generally used if you want to run bot on regular interval using cron/service",
    )
    parser.add_argument(
        "--boundary",
        type=str,
        default=None,
        help="Boundary geojson file path to filter stats, see data/example_boudnary for format of geojson",
    )

    parser.add_argument(
        "--update",
        action="store_true",
        default=False,
        help="Update the old dataset produced by osmsg , Very Experimental : There should be your name stats.csv and summary.csv in place where command is run",
    )
    args = parser.parse_args()
    return args


def main():
    print("After duckdb | pydantic")
    global db_conn
    args = parse_args()
    Initialize()

    fname = args.name
    full_path = os.path.abspath(os.path.join(os.getcwd(), os.path.join(os.getcwd(), f"{fname}.csv")))
    base_path = os.path.abspath(os.path.dirname(full_path))
    print(base_path)
    if not os.path.exists(base_path):
        os.makedirs(base_path)

    if args.key_value:
        args.all_tags = True
        print("Enabling all_tags option as key_value is passed")

    if args.update:
        if args.read_from_metadata:
            print("Error : Option not allowed : read_from_metadata along with --update")
            sys.exit()
        if args.start_date:
            print("Error : Start_date is not allowed during update it will read it from stats csv")
            sys.exit()
        if args.last_week or args.last_day or args.last_month or args.last_year or args.last_hour or args.days:
            print(
                "Error : Can't pass last_* parameters along with update , update will pick start date from old csv and try to update up to now / end_date"
            )
            sys.exit()
        old_csv_path = os.path.join(os.getcwd(), f"{fname}.csv")
        old_summary_path = os.path.join(os.getcwd(), f"{fname}_summary.csv")
        if not os.path.exists(old_csv_path) or not os.path.exists(old_summary_path):
            print(
                f"Error: Couldn't find old stats/summary csv at :{old_csv_path} hence changing update to false and extracting last day stats for default"
            )
            args.update = False
            args.last_day = True
        if args.update:
            old_df = pd.read_csv(old_csv_path, encoding="utf8")
            args.start_date = str(old_df.iloc[0]["end_date"])
            old_stats_start_date = str(old_df.iloc[0]["start_date"])

    if args.start_date:
        start_date = strip_utc(dt.datetime.strptime(args.start_date, "%Y-%m-%d %H:%M:%S%z"), args.timezone)

    if not args.start_date:
        if not (args.last_week or args.last_day or args.last_month or args.last_year or args.last_hour or args.days):
            print("ERR: Supply start_date or extraction parameters such as last_day , last_hour")
            sys.exit()

    if args.end_date:
        end_date = args.end_date
        if not isinstance(end_date, datetime):
            end_date = dt.datetime.strptime(args.end_date, "%Y-%m-%d %H:%M:%S%z")

        end_date = strip_utc(end_date, args.timezone)
    if args.country:
        osc_url_temp = []
        for ctr in args.country:
            if not geofabrik_countries["id"].isin([ctr.lower()]).any():
                print(f"Error : {ctr} doesn't exists : Refer to data/countries.csv id column")
                sys.exit()
            osc_url_temp.append(geofabrik_countries.loc[geofabrik_countries["id"] == ctr.lower(), "update_url"].values[0])
        print(f"Ignoring --url , and using Geofabrik Update URL for {args.country}")
        args.url = osc_url_temp

    if args.tm_stats:
        if not args.changeset and not args.hashtags:
            args.changeset = True  # changeset is required to extract tm project id from hashtags field

    if args.changeset:
        if args.hashtags:
            assert args.changeset, "You can not use include changeset meta option along with hashtags"

    start_time = time.time()

    global additional_tags
    global cookies
    global all_tags
    global key_value
    global hashtags
    global length
    global changeset_meta
    global exact_lookup
    global summary
    global collect_field_mappers_stats
    global geom_filter_df
    global geom_boundary
    global remove_temp_files

    all_tags = args.all_tags
    key_value = args.key_value
    additional_tags = args.tags
    hashtags = args.hashtags
    cookies = None
    exact_lookup = args.exact_lookup
    length = args.length
    summary = args.summary
    collect_field_mappers_stats = args.field_mappers
    geom_boundary = args.boundary
    remove_temp_files = args.temp

    # convert gdf to WKT for subprocess pickling
    geom_filter_wkt = None
    if args.boundary:
        if not args.changeset and not args.hashtags:
            args.changeset = True
        geom_filter_df = process_boundary(args.boundary)
        geom_filter_wkt = geom_filter_df.geometry[0].wkt

    if args.field_mappers:
        if not args.changeset and not args.hashtags:
            args.changeset = True
    changeset_meta = args.changeset

    if args.url:
        args.url = list(set(args.url))  # remove duplicates
        for url in args.url:
            if urllib.parse.urlparse(url).scheme == "":
                # The URL is not valid
                if url == "minute":
                    args.url = ["https://planet.openstreetmap.org/replication/minute"]
                elif url == "hour":
                    args.url = ["https://planet.openstreetmap.org/replication/hour"]
                elif url == "day":
                    args.url = ["https://planet.openstreetmap.org/replication/day"]
                else:
                    print(f"Invalid input for urls {url}")
                    sys.exit()
            if url.endswith("/"):
                print(f"{url} should not end with trailing /")
                sys.exit()

        if any("geofabrik" in url.lower() for url in args.url):
            if args.username is None:
                # print(os.environ.get("OSM_USERNAME"))
                args.username = os.environ.get("OSM_USERNAME")
            if args.password is None:
                args.password = os.environ.get("OSM_PASSWORD")

            if not (args.username and args.password):
                assert args.username and args.password, "OSM username and password are required for geofabrik url"
            cookies = auth(args.username, args.password)

    count = sum(
        [
            args.last_hour,
            args.last_year,
            args.last_month,
            args.last_day,
            args.last_week,
            bool(args.days),
        ]
    )
    if count > 1:
        print("Error: only one of --last_hour, --last_year, --last_month, --last_day, --last_week, or --days should be specified.")
        sys.exit()

    if args.users:
        for u in args.users:
            whitelisted_users.append(u)

    if args.last_hour:
        start_date, end_date = get_prev_hour(args.timezone)

    if args.last_year:
        start_date, end_date = get_prev_year_dates(args.timezone)

    if args.last_month:
        start_date, end_date = previous_month(args.timezone)

    if args.last_day:
        start_date, end_date = previous_day(args.timezone)
    if args.last_week:
        start_date, end_date = previous_week(args.timezone)

    if args.days:
        if args.days > 0:
            start_date, end_date = last_days_count(args.timezone, args.days)
        else:
            print(f"Error : {args.days} should be greater than 0")
            sys.exit()
    if args.read_from_metadata:
        if os.path.exists(args.read_from_metadata):
            with open(args.read_from_metadata, "r") as openfile:
                # Reading from json file
                meta_json = json.load(openfile)
            if "end_date" in meta_json:
                start_date = datetime.strptime(meta_json["end_date"], "%Y-%m-%d %H:%M:%S%z")

                print(f"Start date changed to {start_date} after reading from metajson")
            else:
                print("no end_date in meta json")
        else:
            print("couldn't read start_date from metajson")
    if start_date == end_date:
        print("Err: Start date and end date are equal")
        sys.exit()
    if ((end_date - start_date).days < 1 or args.last_hour) and args.country:
        print(
            "Args country has day difference lesser than 1 day , Remove args country simply process a day data with --changeset option"
        )
        sys.exit()
    if (end_date - start_date).days > 1:
        if args.hashtags:
            if not args.force:
                print(
                    "Warning : Replication for Changeset is minutely , To download more than day data it might take a while depending upon your internet speed, Use --force to ignore this warning"
                )
                sys.exit()
        for url in args.url:
            if "minute" in url and "geofabrik" not in url:
                print(
                    "Warning : To Process more than day data consider using daily/hourly replciation files to avoid downloading huge data . To use daily pass : --url day , for Hourly : --url hour "
                )
    print(f"Supplied start_date: {start_date} and end_date: {end_date}")

    # Initialize DuckDB connection
    db_path = "scratch.db"
    if os.path.exists(db_path):
        os.remove(db_path)
    db_conn = get_connection(db_path)
    create_tables(db_conn)
    print(f"DuckDB initialized: {db_path}")

    valid_changeset_ids = set()

    if args.hashtags or args.changeset:
        Changeset = ChangesetToolKit()
        (
            changeset_download_urls,
            changeset_start_seq,
            changeset_end_seq,
        ) = Changeset.get_download_urls(start_date, end_date)
        print(
            f"Processing Changeset from {strip_utc(Changeset.sequence_to_timestamp(changeset_start_seq),args.timezone)} to {strip_utc(Changeset.sequence_to_timestamp(changeset_end_seq),args.timezone)}"
        )

        temp_path = os.path.join(os.getcwd(), "temp/changeset", "changesets")
        if not os.path.exists(temp_path):
            os.makedirs(temp_path)

        max_workers = os.cpu_count() if not args.workers else args.workers
        print(f"Using {max_workers} Threads")
        print("Downloading Changeset files using https://planet.openstreetmap.org/replication/changesets/")
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            try:
                with tqdm(
                    total=len(changeset_download_urls),
                    unit_scale=True,
                    unit="changesets",
                    leave=True,
                ) as pbar:
                    for _ in executor.map(
                        lambda x: download_osm_files(x, mode="changeset", cookies=cookies),
                        changeset_download_urls,
                    ):
                        pbar.update(1)
            except Exception as e:
                print(f"An error occurred: {e}")
            finally:
                executor.shutdown(wait=True)

            # Bundle all dynamic variables into a config dictionary
        cs_config = {
            "hashtags": hashtags,
            "exact_lookup": exact_lookup,
            "changeset_meta": changeset_meta,
            "collect_field_mappers_stats": collect_field_mappers_stats,
            "remove_temp_files": remove_temp_files,
            "geom_filter_wkt": geom_filter_wkt,
            "field_mapping_editors": field_mapping_editors,
            "whitelisted_users": whitelisted_users,
        }

        import itertools

        users_buffer: list = []
        changesets_buffer: list = []

        print("Processing Changeset Files")
        with concurrent.futures.ProcessPoolExecutor(max_workers=max_workers) as executor:
            try:
                with tqdm(
                    total=len(changeset_download_urls),
                    unit_scale=True,
                    unit="changesets",
                    leave=True,
                ) as pbar:
                    results = executor.map(
                        process_changesets_worker, changeset_download_urls, itertools.repeat(cs_config), chunksize=10
                    )
                    for users, changesets in results:
                        users_buffer.extend(users)
                        changesets_buffer.extend(changesets)

                        if len(changesets_buffer) >= BATCH_SIZE:
                            try:
                                # Start a single transaction for the whole batch
                                db_conn.execute("BEGIN TRANSACTION")

                                if users_buffer:
                                    insert_users(db_conn, users_buffer)

                                insert_changesets(db_conn, changesets_buffer)

                                # Commit everything to disk at once
                                db_conn.execute("COMMIT")

                            except Exception as e:
                                # If something fails, undo the whole batch to keep DB clean
                                db_conn.execute("ROLLBACK")
                                print(f"Failed to flush batch: {e}")
                            finally:
                                users_buffer.clear()
                                changesets_buffer.clear()
                        pbar.update(1)
            except Exception as e:
                print(f"An error occurred: {e}")
            finally:
                executor.shutdown(wait=True)

        try:
            # Start a single transaction for the whole batch
            db_conn.execute("BEGIN TRANSACTION")

            if users_buffer:
                insert_users(db_conn, users_buffer)

            insert_changesets(db_conn, changesets_buffer)

            # Commit everything to disk at once
            db_conn.execute("COMMIT")

        except Exception as e:
            # If something fails, undo the whole batch to keep DB clean
            db_conn.execute("ROLLBACK")
            print(f"Failed to flush batch: {e}")
        finally:
            users_buffer.clear()
            changesets_buffer.clear()

        print("Changeset Processing Finished")

        # Build frozenset to filter OSM elements
        if hashtags or collect_field_mappers_stats or geom_boundary:
            cs_id_rows = db_conn.execute("SELECT changeset_id FROM changesets").fetchall()
            valid_changeset_ids = set(r[0] for r in cs_id_rows)

        end_seq_timestamp = Changeset.sequence_to_timestamp(changeset_end_seq)
        if end_date > end_seq_timestamp:
            end_date = strip_utc(end_seq_timestamp, args.timezone)

    cf_user_buf: list = []
    cf_stub_buf: list = []
    cf_stats_buf: list = []

    for url in args.url:
        print(f"Changefiles : Generating Download Urls Using {url}")
        (
            download_urls,
            server_ts,
            start_seq,
            end_seq,
            start_seq_url,
            end_seq_url,
        ) = get_download_urls_changefiles(start_date, end_date, url, args.timezone)
        if server_ts < end_date:
            print(f"Warning : End date data is not available at server, Changing to latest available date {server_ts}")
            end_date = server_ts
            if start_date >= server_ts:
                print("Err: Data is not available after start date ")
                sys.exit()
        global end_date_utc
        global start_date_utc

        start_date_utc = start_date.astimezone(dt.timezone.utc)
        end_date_utc = end_date.astimezone(dt.timezone.utc)
        print(f"Final UTC Date time to filter stats : {start_date_utc} to {end_date_utc}")

        # Use the ThreadPoolExecutor to download the images in parallel
        max_workers = os.cpu_count() if not args.workers else args.workers
        print(f"Using {max_workers} Threads")

        print("Downloading Changefiles")
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            try:
                with tqdm(
                    total=len(download_urls),
                    unit_scale=True,
                    unit="changefiles",
                    leave=True,
                ) as pbar:
                    for _ in executor.map(
                        lambda x: download_osm_files(x, mode="changefiles", cookies=cookies),
                        download_urls,
                    ):
                        pbar.update(1)
            except Exception as e:
                print(f"An error occurred: {e}")
            finally:
                executor.shutdown(wait=True)

        cf_config: dict = {
            "start_date_utc": start_date_utc,
            "end_date_utc": end_date_utc,
            "hashtags": hashtags,
            "additional_tags": additional_tags,
            "all_tags": all_tags,
            "key_value": key_value,
            "length": length,
            "changeset_meta": changeset_meta,
            "collect_field_mappers_stats": collect_field_mappers_stats,
            "field_mapping_editors": field_mapping_editors,
            "whitelisted_users": whitelisted_users,
            "geom_filter_wkt": None,
            "remove_temp_files": remove_temp_files,
        }

        print("Processing Changefiles")

        # 1. Dynamically set chunksize based on the URL type
        CF_CHUNKSIZE = 2 if "minute" in url.lower() else 1
        with concurrent.futures.ProcessPoolExecutor(
            max_workers=max_workers, initializer=worker_init, initargs=(valid_changeset_ids,)
        ) as executor:
            try:
                with tqdm(
                    total=len(download_urls),
                    unit_scale=True,
                    unit="changefiles",
                    leave=True,
                ) as pbar:
                    results = executor.map(
                        process_changefiles_worker, download_urls, itertools.repeat(cf_config), chunksize=CF_CHUNKSIZE
                    )
                    for users, changesets, changeset_stats in results:
                        cf_user_buf.extend(users)
                        cf_stub_buf.extend(changesets)
                        cf_stats_buf.extend(changeset_stats)

                        if len(cf_stats_buf) >= BATCH_SIZE:
                            try:
                                db_conn.execute("BEGIN TRANSACTION")

                                if cf_user_buf:
                                    insert_users(db_conn, cf_user_buf)
                                if cf_stub_buf:
                                    insert_changesets(db_conn, cf_stub_buf)

                                insert_changeset_stats(db_conn, cf_stats_buf)

                                db_conn.execute("COMMIT")
                            except Exception as e:
                                db_conn.execute("ROLLBACK")
                                print(f"Error during changefile flush: {e}")
                            finally:
                                cf_user_buf.clear()
                                cf_stats_buf.clear()

                        pbar.update(1)
            except Exception as e:
                print(f"An error occurred: {e}")
            finally:
                executor.shutdown(wait=True)

        print(f"Changefiles Processing Finished using {url}")
        # Flush any remaining batches to DuckDB
        if cf_user_buf:
            insert_users(db_conn, cf_user_buf)
        if cf_stub_buf:
            insert_changesets(db_conn, cf_stub_buf)
        if cf_stats_buf:
            insert_changeset_stats(db_conn, cf_stats_buf)
        print("All data flushed to DuckDB")
        cf_user_buf.clear()
        cf_stub_buf.clear()
        cf_stats_buf.clear()

    if valid_changeset_ids:
        valid_changeset_ids.clear()
    os.chdir(os.getcwd())
    if args.temp:
        shutil.rmtree("temp")

    # Core stats query
    rows = get_user_stats(
        conn=db_conn,
        include_metadata=bool(hashtags or changeset_meta),
        additional_tags=additional_tags,
        all_tags=all_tags,
        key_value=key_value,
        length_tags=length,
        top_n=args.rows,
        countries_gdf=countries_df if (hashtags or changeset_meta) else None,
    )

    if not rows:
        print("No data Found")
        sys.exit()

    # console display
    df_display = pd.DataFrame(rows)
    _DISPLAY_COL_ORDER = [
        "rank",
        "name",
        "profile",
        "uid",
        "changesets",
        "map_changes",
        "poi_create",
        "poi_modify",
        "nodes_create",
        "nodes_modify",
        "nodes_delete",
        "ways_create",
        "ways_modify",
        "ways_delete",
        "rels_create",
        "rels_modify",
        "rels_delete",
        "start_date",
        "end_date",
    ]
    # keep only columns that actually exist in this run
    ordered = [c for c in _DISPLAY_COL_ORDER if c in df_display.columns]
    # append any remaining columns not in the priority list
    ordered += [c for c in df_display.columns if c not in ordered]
    print(df_display[ordered])

    # update: merge with previous run's CSV
    if args.update:
        rows = apply_update_stats(old_csv_path, rows)

    # tm_stats: enrich rows with Tasking Manager contribution data
    if args.tm_stats:
        rows = enrich_with_tm_stats(rows)

    # Exports
    if "json" in args.format:
        export_json(rows, f"{fname}.json")

    if "csv" in args.format:
        export_csv(
            rows,
            f"{fname}.csv",
            start_date=old_stats_start_date if args.update else start_date_utc,
            end_date=end_date_utc,
            include_profile_link=True,
        )

    if "excel" in args.format:
        export_excel(rows, f"{fname}.xlsx")

    if "text" in args.format:
        export_text(
            rows,
            f"{fname}.txt",
            start_date=in_local_timezone(start_date_utc, args.timezone),
            end_date=in_local_timezone(end_date_utc, args.timezone),
            source_url=args.url,
        )

    if "image" in args.format:
        export_image(rows, fname)

    # Charts
    produced_charts: list = []
    if args.charts:
        produced_charts = export_charts(rows, fname, start_date_utc, end_date_utc)

    # Daily summary
    summary_rows = None
    if args.summary:
        summary_rows = get_summary_by_day(
            db_conn,
            additional_tags=additional_tags,
            all_tags=all_tags,
            key_value=key_value,
            length_tags=length,
        )
        if summary_rows:
            # --update: merge with previous summary CSV
            if args.update:
                old_summary_path = os.path.join(os.getcwd(), f"{fname}_summary.csv")
                if os.path.exists(old_summary_path):
                    summary_rows = apply_update_summary(old_summary_path, summary_rows)

            ok = export_csv(summary_rows, f"{fname}_summary.csv", include_profile_link=False)
            if ok:
                print(f"Daily summary exported to {fname}_summary.csv")
            else:
                print("Warning: failed to export daily summary CSV")

            # Summary markdown narrative file
            export_summary_md(
                rows=rows,
                summary_rows=summary_rows,
                fname=fname,
                start_date_display=old_stats_start_date if args.update else start_date_utc,
                end_date_display=end_date_utc,
                additional_tags=additional_tags,
                length_tags=length,
                all_tags=all_tags,
                tm_stats=args.tm_stats,
                produced_charts=produced_charts,
                base_path=base_path,
            )
        else:
            print("Warning: --summary requires --changeset or --hashtags flag to populate changeset timestamps")

    # Metadata JSON
    if args.meta:
        # Mask password in reproduced command string
        argv_copy = list(sys.argv)
        for i in range(len(argv_copy)):
            if argv_copy[i] == "--password" and i + 1 < len(argv_copy):
                argv_copy[i + 1] = "***"
        export_metadata(
            fname=fname,
            command=" ".join(argv_copy),
            source_url=args.url,
            start_date=in_local_timezone(start_date_utc, args.timezone),
            start_seq=start_seq,
            start_seq_url=start_seq_url,
            end_date=in_local_timezone(end_date_utc, args.timezone),
            end_seq=end_seq,
            end_seq_url=end_seq_url,
            timezone=args.timezone,
        )

    end_time = time.time()
    elapsed_time = end_time - start_time

    # convert elapsed time to hr:min:sec format
    hours, rem = divmod(elapsed_time, 3600)
    minutes, seconds = divmod(rem, 60)
    print("Script Completed in hr:min:sec = {:0>2}:{:0>2}:{:05.2f}".format(int(hours), int(minutes), seconds))


if __name__ == "__main__":
    try:
        main()
    except Exception as ex:
        print(ex)
        sys.exit()
