import osmium
import os
import re
import datetime as dt
from .models import (
    Action,
    Changeset,
    ChangesetStats,
    TagValueStat,
    User,
)

from .utils import get_bbox_centroid, get_editors_name_strapped, get_file_path_from_url

from .db import (
    prepare_changeset_row,
    prepare_stats_row,
    flush_rows_to_parquet,
)


GLOBAL_VALID_CS = set()
global_cs_config = None
global_cf_config = None

# running counter per worker process, makes parquet file names unique across batches of a process
_worker_batch_counter: int = 0


def cs_worker_init(config):
    """
    Initializer for ProcessPoolExecutor changeset workers.
    Runs exactly once per worker process.
    """
    global global_cs_config, _worker_batch_counter
    global_cs_config = config
    _worker_batch_counter = 0


def cf_worker_init(valid_cs, config):
    """
    Initializer for ProcessPoolExecutor changefile workers.
    Runs exactly once per worker process.
    """
    global GLOBAL_VALID_CS, global_cf_config, _worker_batch_counter
    GLOBAL_VALID_CS = valid_cs
    global_cf_config = config
    _worker_batch_counter = 0


class LocalChangesetHandler(osmium.SimpleHandler):
    def __init__(self, config: dict):
        super(LocalChangesetHandler, self).__init__()
        self.config = config

        self.local_users: dict[int, User] = {}
        self.local_changesets: dict[int, Changeset] = {}

        # reconstruct shapely geometry from WKT once per worker process
        self._geom = None
        if config.get("geom_filter_wkt"):
            from shapely import wkt

            self._geom = wkt.loads(config["geom_filter_wkt"])

    def changeset(self, c):
        if c.id in self.local_changesets:
            return

        config = self.config
        run_hashtag_check_logic = False

        if self._geom is not None:
            centroid = get_bbox_centroid(c.bounds)
            if not centroid:
                return
            if not self._geom.contains(centroid):
                return

        if config["collect_field_mappers_stats"]:
            if "created_by" in c.tags:
                editor = get_editors_name_strapped(c.tags["created_by"])
                if editor not in config["field_mapping_editors"]:
                    return

        if config["changeset_meta"] and not config["hashtags"]:
            run_hashtag_check_logic = True

        if config["hashtags"]:
            if "comment" in c.tags:
                if config["exact_lookup"]:
                    hashtags_comment = re.findall(r"#[\w-]+", c.tags["comment"])
                    if any(elem.lower() in map(str.lower, hashtags_comment) for elem in config["hashtags"]):
                        run_hashtag_check_logic = True
                elif any(elem.lower() in c.tags["comment"].lower() for elem in config["hashtags"]):
                    run_hashtag_check_logic = True

        if run_hashtag_check_logic and config["whitelisted_users"]:
            run_hashtag_check_logic = c.user in config["whitelisted_users"]

        if run_hashtag_check_logic or config["collect_field_mappers_stats"]:
            # Extract hashtags from comment
            hashtags_list = []
            if "comment" in c.tags:
                hashtags_list = re.findall(r"#[\w-]+", c.tags["comment"])

            # Extract editor
            editor = c.tags.get("created_by", None)

            # Extract bbox
            bbox = None
            if c.bounds.valid():
                bbox = (c.bounds.bottom_left.lon, c.bounds.bottom_left.lat, c.bounds.top_right.lon, c.bounds.top_right.lat)

            # Create Changeset object
            self.local_users[c.uid] = User(uid=c.uid, username=c.user)
            self.local_changesets[c.id] = Changeset(
                changeset_id=c.id,
                uid=c.uid,
                created_at=(c.created_at.replace(tzinfo=dt.timezone.utc) if c.created_at.tzinfo is None else c.created_at),
                hashtags=hashtags_list,
                editor=editor,
                bbox=bbox,
            )


def process_changesets_worker(url):
    """
    Process one changeset replication file and write the results to Parquet.
    """
    global global_cs_config, _worker_batch_counter
    cs_config = global_cs_config

    file_path = get_file_path_from_url(url, "changeset")
    handler = LocalChangesetHandler(cs_config)
    try:
        handler.apply_file(file_path[:-3])
    except Exception as ex:
        print(f"File may be corrupt : Error at {url} : {ex}")

    user_rows = [(u.uid, u.username) for u in handler.local_users.values()]
    changeset_rows = [prepare_changeset_row(c) for c in handler.local_changesets.values()]

    pid = os.getpid()
    _worker_batch_counter += 1
    parquet_dir = cs_config.get("parquet_dir", ".")

    u_path, cs_path, _ = flush_rows_to_parquet(user_rows, changeset_rows, None, pid, _worker_batch_counter, parquet_dir)

    if cs_config.get("remove_temp_files"):
        try:
            os.remove(file_path[:-3])
        except OSError:
            pass

    return u_path, cs_path


class LocalChangefileHandler(osmium.SimpleHandler):
    def __init__(self, config: dict, sequence_id: str):
        super(LocalChangefileHandler, self).__init__()
        self.config = config
        self.start_date_utc = config["start_date_utc"]
        self.end_utc = config["end_date_utc"]
        self.seq_id = sequence_id

        self.users: dict[int, User] = {}
        self.stubs: dict[int, Changeset] = {}
        self.changeset_stats: dict[int, ChangesetStats] = {}

    def should_collect(self, uid, uname, changeset) -> bool:
        global GLOBAL_VALID_CS
        if GLOBAL_VALID_CS:
            return changeset in GLOBAL_VALID_CS
        if self.config["whitelisted_users"]:
            return uname in self.config["whitelisted_users"]
        return True  # collect everything

    def record(self, uid, uname, changeset) -> None:
        if uid not in self.users:
            self.users[uid] = User(uid=uid, username=uname)
        if changeset not in self.stubs:
            self.stubs[changeset] = Changeset(changeset_id=changeset, uid=uid)

    def accumulate(self, uid, uname, changeset, version, tags, osm_type, osm_obj_nodes=None):
        config = self.config
        # Determine action
        if version == 0:
            action = Action.DELETE.value
        elif version == 1:
            action = Action.CREATE.value
        elif version > 1:
            action = Action.MODIFY.value

        # Calculate length if needed
        len_feature = 0.0
        if config["length"] and osm_obj_nodes:
            try:
                len_feature = osmium.geom.haversine_distance(osm_obj_nodes)
            except:
                pass

        # Add to users and changesets dicts
        self.record(uid, uname, changeset)

        # Initialize changeset stats if needed
        if changeset not in self.changeset_stats:
            self.changeset_stats[changeset] = ChangesetStats(changeset_id=changeset, uid=uid, seq_id=self.seq_id)
        stats = self.changeset_stats[changeset]

        # osm element count
        if osm_type == "nodes":
            stats.nodes.add(action)
            # POI logic: nodes with tags that aren't deleted
            if tags and action != Action.DELETE.value:
                if action == Action.CREATE.value:
                    stats.poi_created += 1
                elif action == Action.MODIFY.value:
                    stats.poi_modified += 1
        elif osm_type == "ways":
            stats.ways.add(action)
        elif osm_type == "relations":
            stats.rels.add(action)

        # Process tags
        if tags and action != Action.DELETE.value:
            # All tags collection
            if config["all_tags"]:
                for key, value in tags:
                    # nested dict structure: tag_stats = {key: {value: TagValueStat()}}
                    if key not in stats.tag_stats:
                        stats.tag_stats[key] = {}

                    if value not in stats.tag_stats[key]:
                        stats.tag_stats[key][value] = TagValueStat()

                    # Add action (create/modify)
                    stats.tag_stats[key][value].add_action(action)

                    # Add length
                    if config["length"] and (key in config["length"]) and len_feature > 0 and action == Action.CREATE.value:
                        stats.tag_stats[key][value].add_length(len_feature)

            # for user supplied tags
            elif config["additional_tags"]:
                for tag_key in config["additional_tags"]:
                    if tag_key in tags:
                        if tag_key not in stats.tag_stats:
                            stats.tag_stats[tag_key] = {}

                        tag_value = tags[tag_key]
                        if tag_value not in stats.tag_stats[tag_key]:
                            stats.tag_stats[tag_key][tag_value] = TagValueStat()

                        # Add action (create/modify)
                        stats.tag_stats[tag_key][tag_value].add_action(action)

                        if config["length"] and tag_key in config["length"] and len_feature > 0 and action == Action.CREATE.value:
                            stats.tag_stats[tag_key][tag_value].add_length(len_feature)

    def node(self, n):
        if self.start_date_utc <= n.timestamp < self.end_utc:
            if not self.should_collect(n.uid, n.user, n.changeset):
                return
            version = 0 if n.deleted else n.version

            self.accumulate(n.uid, n.user, n.changeset, version, n.tags, "nodes")

    def way(self, w):
        if self.start_date_utc <= w.timestamp < self.end_utc:
            if not self.should_collect(w.uid, w.user, w.changeset):
                return
            version = 0 if w.deleted else w.version

            self.accumulate(
                w.uid,
                w.user,
                w.changeset,
                version,
                w.tags,
                "ways",
                w.nodes if self.config["length"] else None,
            )

    def relation(self, r):
        if self.start_date_utc <= r.timestamp < self.end_utc:
            if not self.should_collect(r.uid, r.user, r.changeset):
                return
            version = 0 if r.deleted else r.version

            self.accumulate(r.uid, r.user, r.changeset, version, r.tags, "relations")


def process_changefiles_worker(url):
    """
    Process one OSC changefile and write the results to Parquet.
    """
    global global_cf_config, _worker_batch_counter
    cf_config = global_cf_config

    if "minute" not in url:
        print(f"Processing {url}")

    file_path = get_file_path_from_url(url, "changefiles")

    try:
        raw_seq = "".join(url.split("/")[-3:]).split(".")[0]
        sequence_id = int(raw_seq)
    except (ValueError, IndexError):
        print(f"Error: Could not parse sequence ID from URL: {url}")
        return None, None, None

    handler = LocalChangefileHandler(cf_config, sequence_id)
    try:
        if cf_config["length"]:
            handler.apply_file(file_path[:-3], locations=True)
        else:
            handler.apply_file(file_path[:-3])
    except Exception as ex:
        print(f"File may be corrupt : Error at {url} : {ex}")

    users_rows = [(u.uid, u.username) for u in handler.users.values()]
    stubs_rows = [prepare_changeset_row(c) for c in handler.stubs.values()]
    stats_rows = [prepare_stats_row(s) for s in handler.changeset_stats.values()]

    pid = os.getpid()
    _worker_batch_counter += 1
    parquet_dir = cf_config.get("parquet_dir", ".")

    u_path, cs_path, st_path = flush_rows_to_parquet(users_rows, stubs_rows, stats_rows, pid, _worker_batch_counter, parquet_dir)

    if cf_config.get("remove_temp_files"):
        try:
            os.remove(file_path[:-3])
        except OSError:
            pass

    return u_path, cs_path, st_path
