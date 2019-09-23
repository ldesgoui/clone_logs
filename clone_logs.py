#!/usr/bin/env python3


SCHEMA = r"""
create table log
    ( id                        integer primary key
    , date                      datetime
    , title                     text
    , map                       text
    , duration                  integer
    -- INFO
    , supplemental              bool
    , has_real_damage           bool
    , has_weapon_damage         bool
    , has_accuracy              bool
    , has_medkit_pickups        bool
    , has_medkit_health         bool
    , has_headshot_kills        bool
    , has_headshot_hits         bool
    , has_backstabs             bool
    , has_point_captures        bool
    , has_sentries_built        bool
    , has_damage_taken          bool
    , has_airshots              bool
    , has_heals_received        bool
    , has_intel_captures        bool
    , scoring_attack_defense    bool
    , uploader_steam_id         unsigned big int
    , uploader_name             text
    , uploader_info             text
    -- TEAMS
    , red_score                 integer
    , red_kills                 integer
    , red_deaths                integer
    , red_damage                integer
    , red_first_caps            integer
    , red_caps                  integer
    , red_charges               integer
    , red_drops                 integer
    , blu_score                 integer
    , blu_kills                 integer
    , blu_deaths                integer
    , blu_damage                integer
    , blu_first_caps            integer
    , blu_caps                  integer
    , blu_charges               integer
    , blu_drops                 integer
    );

create table killstreak
    ( log_id    integer not null references logs(id)
    , steam_id  unsigned big int
    , streak    integer
    , time      integer
    );

create table chat
    ( log_id    integer not null references logs(id)
    , idx       integer not null
    , steam_id  unsigned big int
    , name      text
    , message   text
    );

create table round
    ( log_id        integer not null references logs(id)
    , idx           integer not null
    , start         datetime
    , duration      integer
    , first_cap     text
    , winner        text
    , red_score     integer
    , red_kills     integer
    , red_damage    integer
    , red_charges   integer
    , blu_score     integer
    , blu_kills     integer
    , blu_damage    integer
    , blu_charges   integer
    );

create table round_event
    ( log_id            integer not null references logs(id)
    , round_idx         integer not null
    , time              integer
    , type              text
    , team              text
    , point             integer
    , medigun           text
    , steam_id          unsigned big int
    , killer_steam_id   unsigned big int
    );

create table round_player
    ( log_id    integer not null references logs(id)
    , round_idx integer not null
    , steam_id  unsigned big int
    , kills     integer
    , damage    integer
    );

create table player
    ( log_id                        integer not null references logs(id)
    , steam_id                      unsigned big int
    , name                          text
    , team                          text
    , kills                         integer
    , assists                       integer
    , deaths                        integer
    , suicides                      integer
    , damage                        integer
    , damage_real                   integer
    , damage_taken                  integer
    , damage_taken_real             integer
    , heals_received                integer
    , longest_killstreak            integer
    , airshots                      integer
    , medkit_pickup                 integer
    , medkit_health                 integer
    , backstabs                     integer
    , headshot_kills                integer
    , headshots                     integer
    , sentries                      integer
    , point_captures                integer
    , intel_captures                integer

    -- MEDIC
    , charges                       integer
    , charges_uber                  integer
    , charges_kritzkrieg            integer
    , charges_vaccinator            integer
    , charges_quickfix              integer
    , drops                         integer
    , advantages_lost               integer
    , biggest_advantage_lost        integer
    , deaths_with_95_uber           integer
    , deaths_within_20s_after_uber  integer
    , average_time_before_healing   float
    , average_time_to_build         float
    , average_time_before_using     float
    , average_charge_length         float

    -- CLASS STATS
    , time_as_scout                 integer
    , kills_as_scout                integer
    , assists_as_scout              integer
    , deaths_as_scout               integer
    , damage_as_scout               integer

    , time_as_soldier               integer
    , kills_as_soldier              integer
    , assists_as_soldier            integer
    , deaths_as_soldier             integer
    , damage_as_soldier             integer

    , time_as_pyro                  integer
    , kills_as_pyro                 integer
    , assists_as_pyro               integer
    , deaths_as_pyro                integer
    , damage_as_pyro                integer

    , time_as_demoman               integer
    , kills_as_demoman              integer
    , assists_as_demoman            integer
    , deaths_as_demoman             integer
    , damage_as_demoman             integer

    , time_as_heavy                 integer
    , kills_as_heavy                integer
    , assists_as_heavy              integer
    , deaths_as_heavy               integer
    , damage_as_heavy               integer

    , time_as_engineer              integer
    , kills_as_engineer             integer
    , assists_as_engineer           integer
    , deaths_as_engineer            integer
    , damage_as_engineer            integer

    , time_as_medic                 integer
    , kills_as_medic                integer
    , assists_as_medic              integer
    , deaths_as_medic               integer
    , damage_as_medic               integer

    , time_as_sniper                integer
    , kills_as_sniper               integer
    , assists_as_sniper             integer
    , deaths_as_sniper              integer
    , damage_as_sniper              integer

    , time_as_spy                   integer
    , kills_as_spy                  integer
    , assists_as_spy                integer
    , deaths_as_spy                 integer
    , damage_as_spy                 integer


    -- TARGET STATS
    , scout_kills                   integer
    , scout_assists                 integer
    , scout_deaths                  integer

    , soldier_kills                 integer
    , soldier_assists               integer
    , soldier_deaths                integer

    , pyro_kills                    integer
    , pyro_assists                  integer
    , pyro_deaths                   integer

    , demoman_kills                 integer
    , demoman_assists               integer
    , demoman_deaths                integer

    , heavy_kills                   integer
    , heavy_assists                 integer
    , heavy_deaths                  integer

    , engineer_kills                integer
    , engineer_assists              integer
    , engineer_deaths               integer

    , medic_kills                   integer
    , medic_assists                 integer
    , medic_deaths                  integer

    , sniper_kills                  integer
    , sniper_assists                integer
    , sniper_deaths                 integer

    , spy_kills                     integer
    , spy_assists                   integer
    , spy_deaths                    integer
    );

create table player_weapon
    ( log_id            integer not null references logs(id)
    , steam_id          unsigned big int
    , class             text
    , weapon            text
    , kills             integer
    , damage            integer
    , average_damage    float
    , shots             integer
    , hits              integer
    );

create table heal_spread
    ( log_id            integer not null references logs(id)
    , healer_steam_id   unsigned big int
    , target_steam_id   unsigned big int
    , heal_amount       integer
    );
"""

IMPORT = """
insert or ignore into main.log              select * from other.log;
insert or ignore into main.killstreak       select * from other.killstreak;
insert or ignore into main.chat             select * from other.chat;
insert or ignore into main.round            select * from other.round;
insert or ignore into main.round_event      select * from other.round_event;
insert or ignore into main.round_player     select * from other.round_player;
insert or ignore into main.player           select * from other.player;
insert or ignore into main.player_weapon    select * from other.player_weapon;
insert or ignore into main.heal_spread      select * from other.heal_spread;
"""


from datetime import datetime, timedelta
import argparse
import contextlib
import itertools
import json
import logging
import pathlib
import sqlite3
import textwrap
import time
import urllib.parse
import urllib.request


logging.basicConfig(
    level=logging.DEBUG, format="%(asctime)-15s %(levelname)-8s %(message)s"
)
log = logging.getLogger(__name__)


def main(database, imports, ids, **search_opts):
    log.info("program entry")

    if not pathlib.Path(database).exists():
        log.warning(f"no database at path {database!r}, creating")

    with contextlib.closing(sqlite3.connect(database)) as db:
        initialize(database, db)

        import_databases(db, imports)
        db.commit()

        fetch_logs(db, itertools.chain(ids, search(**search_opts)))
        db.commit()

    log.info("program exit")


def initialize(database, db):
    try:
        db.execute("select * from main.log where 0")
    except sqlite3.OperationalError:
        log.warning(f"missing schema in {database!r}, initializing")
        db.executescript(SCHEMA)


def import_databases(db, imports):
    for path in imports:
        if not pathlib.Path(path).exists():
            log.error(f"cannot import, no database at path {path!r}")
            continue

        log.info(f"importing {path!r}")

        cursor = db.execute("attach database ? as other", (path,))

        try:
            cursor.executescript(IMPORT)
        except sqlite3.OperationalError:
            log.error(f"cannot import, invalid schema in {path!r}")
        finally:
            cursor.execute("detach database other")


def fetch_logs(db, logs):
    cursor = db.cursor()
    for log_id in logs:
        cursor.execute("select id from log where id = ?", (log_id,))
        if cursor.fetchone() is not None:
            log.info(f"ignoring known log {log_id}")
            continue

        log.info(f"fetching {log_id}")

        with urllib.request.urlopen(f"https://logs.tf/api/v1/log/{log_id}") as resp:
            insert(cursor, log_id, json.load(resp))

        time.sleep(0.5)


def insert(cursor, log_id, data):
    d = dict_path(data)

    cursor.execute(
        f"insert into log values ( { ','.join('?' * 40) } )",
        (
            log_id,
            datetime.utcfromtimestamp(d("info.date")) if d("info.date") else None,
            d("info.title"),
            d("info.map"),
            d("info.total_length"),
            # info
            d("info.supplemental"),
            d("info.hasRealDamage"),
            d("info.hasWeaponDamage"),
            d("info.hasAccuracy"),
            d("info.hasHP"),
            d("info.hasHP_real"),
            d("info.hasHS"),
            d("info.hasHS_hit"),
            d("info.hasBS"),
            d("info.hasCP"),
            d("info.hasSB"),
            d("info.hasDT"),
            d("info.hasAS"),
            d("info.hasHR"),
            d("info.hasIntel"),
            d("info.AD_scoring"),
            d("info.uploader.id"),
            d("info.uploader.name"),
            d("info.uploader.info"),
            # teams
            d("teams.Red.score"),
            d("teams.Red.kills"),
            d("teams.Red.deaths"),
            d("teams.Red.dmg"),
            d("teams.Red.firstcaps"),
            d("teams.Red.caps"),
            d("teams.Red.charges"),
            d("teams.Red.drops"),
            d("teams.Blue.score"),
            d("teams.Blue.kills"),
            d("teams.Blue.deaths"),
            d("teams.Blue.dmg"),
            d("teams.Blue.firstcaps"),
            d("teams.Blue.caps"),
            d("teams.Blue.charges"),
            d("teams.Blue.drops"),
        ),
    )

    cursor.executemany(
        f"insert into killstreak values ( { ','.join('?' * 4) } )",
        (
            (log_id, as_steamid64(s("steamid")), s("streak"), s("time"))
            for s in map(dict_path, d("killstreaks") or [])
        ),
    )

    cursor.executemany(
        f"insert into chat values ( { ','.join('?' * 5) } )",
        (
            (log_id, idx, as_steamid64(s("steamid")), s("name"), s("msg"))
            for idx, s in enumerate(map(dict_path, d("chat")))
        ),
    )

    cursor.executemany(
        f"insert into round values ( { ','.join('?' * 14) } )",
        (
            (
                log_id,
                idx,
                datetime.utcfromtimestamp(r("start_time")) if r("start_time") else None,
                r("length"),
                r("firstcap"),
                r("winner"),
                r("team.Red.score"),
                r("team.Red.kills"),
                r("team.Red.dmg"),
                r("team.Red.ubers"),
                r("team.Blue.score"),
                r("team.Blue.kills"),
                r("team.Blue.dmg"),
                r("team.Blue.ubers"),
            )
            for idx, r in enumerate(map(dict_path, d("rounds") or []))
        ),
    )

    cursor.executemany(
        f"insert into round_event values ( { ','.join('?' * 9) } )",
        (
            (
                log_id,
                idx,
                e("time"),
                e("type"),
                e("team"),
                e("point"),
                e("medigun"),
                as_steamid64(e("steamid")),
                as_steamid64(e("killer")),
            )
            for idx, r in enumerate(d("rounds") or [])
            for e in map(dict_path, r["events"])
        ),
    )

    cursor.executemany(
        f"insert into round_player values ( { ','.join('?' * 5) } )",
        (
            (log_id, idx, as_steamid64(id), p("kills"), p("dmg"))
            for idx, r in enumerate(d("rounds") or [])
            for id, p in ((id, dict_path(p)) for id, p in r["players"].items())
        ),
    )

    players = []

    for id, p in d("players").items():
        p["class_stats"] = {c["type"]: c for c in p["class_stats"]}
        players.append((id, dict_path(p)))

    cursor.executemany(
        f"insert into player values ( { ','.join('?' * 109) } )",
        (
            (
                log_id,
                as_steamid64(id),
                d(f"names.{id}"),
                p("team"),
                p("kills"),
                p("assists"),
                p("deaths"),
                p("suicides"),
                p("dmg"),
                p("dmg_real"),
                p("dt"),
                p("dt_real"),
                p("hr"),
                p("lks"),
                p("as"),
                p("medkits"),
                p("medkits_hp"),
                p("backstabs"),
                p("headshots"),
                p("headshots_hit"),
                p("sentries"),
                p("cpc"),
                p("ic"),
                # medic
                p("ubers"),
                p("ubertypes.medigun"),
                p("ubertypes.kritzkrieg"),
                p("ubertypes.vaccinator"),
                p("ubertypes.quickfix"),
                p("drops"),
                p("medicstats.advantages_lost"),
                p("medicstats.biggest_advantage_lost"),
                p("medicstats.deaths_with_95_99_uber"),
                p("medicstats.deaths_within_20s_after_uber"),
                p("medicstats.avg_time_before_healing"),
                p("medicstats.avg_time_to_build"),
                p("medicstats.avg_time_before_using"),
                p("medicstats.avg_uber_length"),
                # class stats
                p("class_stats.scout.total_time"),
                p("class_stats.scout.kills"),
                p("class_stats.scout.assists"),
                p("class_stats.scout.deaths"),
                p("class_stats.scout.dmg"),
                p("class_stats.soldier.total_time"),
                p("class_stats.soldier.kills"),
                p("class_stats.soldier.assists"),
                p("class_stats.soldier.deaths"),
                p("class_stats.soldier.dmg"),
                p("class_stats.pyro.total_time"),
                p("class_stats.pyro.kills"),
                p("class_stats.pyro.assists"),
                p("class_stats.pyro.deaths"),
                p("class_stats.pyro.dmg"),
                p("class_stats.demoman.total_time"),
                p("class_stats.demoman.kills"),
                p("class_stats.demoman.assists"),
                p("class_stats.demoman.deaths"),
                p("class_stats.demoman.dmg"),
                p("class_stats.heavy.total_time"),
                p("class_stats.heavy.kills"),
                p("class_stats.heavy.assists"),
                p("class_stats.heavy.deaths"),
                p("class_stats.heavy.dmg"),
                p("class_stats.engineer.total_time"),
                p("class_stats.engineer.kills"),
                p("class_stats.engineer.assists"),
                p("class_stats.engineer.deaths"),
                p("class_stats.engineer.dmg"),
                p("class_stats.medic.total_time"),
                p("class_stats.medic.kills"),
                p("class_stats.medic.assists"),
                p("class_stats.medic.deaths"),
                p("class_stats.medic.dmg"),
                p("class_stats.sniper.total_time"),
                p("class_stats.sniper.kills"),
                p("class_stats.sniper.assists"),
                p("class_stats.sniper.deaths"),
                p("class_stats.sniper.dmg"),
                p("class_stats.spy.total_time"),
                p("class_stats.spy.kills"),
                p("class_stats.spy.assists"),
                p("class_stats.spy.deaths"),
                p("class_stats.spy.dmg"),
                # target stats
                d(f"classkills.{id}.scout"),
                d(f"classkillassists.{id}.scout"),
                d(f"classdeaths.{id}.scout"),
                d(f"classkills.{id}.soldier"),
                d(f"classkillassists.{id}.soldier"),
                d(f"classdeaths.{id}.soldier"),
                d(f"classkills.{id}.pyro"),
                d(f"classkillassists.{id}.pyro"),
                d(f"classdeaths.{id}.pyro"),
                d(f"classkills.{id}.demoman"),
                d(f"classkillassists.{id}.demoman"),
                d(f"classdeaths.{id}.demoman"),
                d(f"classkills.{id}.heavy"),
                d(f"classkillassists.{id}.heavy"),
                d(f"classdeaths.{id}.heavy"),
                d(f"classkills.{id}.engineer"),
                d(f"classkillassists.{id}.engineer"),
                d(f"classdeaths.{id}.engineer"),
                d(f"classkills.{id}.medic"),
                d(f"classkillassists.{id}.medic"),
                d(f"classdeaths.{id}.medic"),
                d(f"classkills.{id}.sniper"),
                d(f"classkillassists.{id}.sniper"),
                d(f"classdeaths.{id}.sniper"),
                d(f"classkills.{id}.spy"),
                d(f"classkillassists.{id}.spy"),
                d(f"classdeaths.{id}.spy"),
            )
            for id, p in players
        ),
    )

    cursor.executemany(
        f"insert into player_weapon values ( { ','.join('?' * 9) } )",
        (
            (
                log_id,
                as_steamid64(id),
                cls,
                weapon,
                w("kills"),
                w("dmg"),
                w("avg_dmg"),
                w("shots"),
                w("hits"),
            )
            for id, p in players
            for cls, c in p("class_stats").items()
            for weapon, w in (
                (weapon, dict_path(w)) for weapon, w in c.get("weapon", {}).items()
            )
        ),
    )

    cursor.executemany(
        f"insert into heal_spread values ( { ','.join('?' * 4) } )",
        (
            (log_id, as_steamid64(healer), as_steamid64(target), amount)
            for healer, targets in d("healspread").items()
            for target, amount in targets.items()
        ),
    )


def as_steamid64(txt):
    ident = 76561197960265728

    try:
        if "U" in txt:
            return ident + int(txt.replace("[", "").replace("]", "").split(":")[2])

        if "STEAM_" in txt:
            return ident + int(txt.split(":")[2]) * 2 + int(txt.split(":")[1])
    except:
        return None


def dict_path(root):
    def dict_path(path):
        val = root
        try:
            for key in path.split("."):
                val = val[key]
            if val == 811111111111111100000:
                return 0
            return val
        except:
            pass
        return None

    return dict_path


def search(limit, skip, players, uploader, title, map):
    if limit is None:
        return []

    page = 500
    base_params = {}

    if players:
        base_params["player"] = players.join(",")

    if uploader is not None:
        base_params["uploader"] = uploader

    if title is not None:
        base_params["title"] = title

    if map is not None:
        base_params["map"] = map

    while limit > 0:
        params = urllib.parse.urlencode(
            dict(limit=min(page, limit), offset=skip, **base_params)
        )

        with urllib.request.urlopen(f"https://logs.tf/api/v1/log?{params}") as resp:
            data = json.load(resp)

            age_check = datetime.utcnow() - timedelta(minutes=30)
            for item in data["logs"]:
                then = datetime.utcfromtimestamp(item["date"])
                if age_check < then:
                    log.info(f"skipping log {item['id']} because it is too young")
                    continue

                yield item["id"]

        limit -= page
        skip += page

    return []


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description="clone logs.tf data into a normalized sqlite3 database",
        epilog=textwrap.dedent(
            """
            duplicates are ignored, known logs are not fetched.
            log fetches are rate limited to not ruin zoob's service.
            logs posted less than 30 minutes ago are ignored.
            it is recommended to import an existing database rather than to download the entire history.
            """
        ),
        allow_abbrev=True,
    )

    parser.add_argument(
        "-d",
        "--database",
        default="logs.sqlite3",
        help="write data to database at PATH (default: logs.sqlite3)",
        metavar="PATH",
    )

    parser.add_argument(
        "-g",
        "--get",
        nargs="+",
        default=[],
        type=int,
        help="directly download ID (can be repeated)",
        metavar="ID",
        dest="ids",
    )

    parser.add_argument(
        "-l", "--limit", type=int, help="download at most N logs", metavar="N"
    )

    parser.add_argument(
        "--skip", default=0, type=int, help="skip the last N logs", metavar="N"
    )

    parser.add_argument(
        "-p",
        "--player",
        nargs="+",
        default=[],
        type=int,
        help="find logs containing this player (can be repeated)",
        metavar="STEAMID64",
        dest="players",
    )

    parser.add_argument(
        "--uploader",
        type=int,
        help="find logs uploaded by this steam user",
        metavar="STEAMID64",
    )

    parser.add_argument(
        "--title", help="find logs matching this title (minimum 2 characters)"
    )

    parser.add_argument("--map", help="find logs matching this map name exactly")

    parser.add_argument(
        "--import",
        nargs="+",
        default=[],
        help="import data from database at PATH (can be repeated)",
        metavar="PATH",
        dest="imports",
    )

    args = parser.parse_args()
    main(**vars(args))
