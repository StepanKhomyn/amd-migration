"""
Migration script: Old DB (project 1 + project 2) → New unified DB + MinIO
Database: MariaDB / MySQL

Requirements:
    pip install sqlalchemy pymysql minio alembic

Usage:
    python migrate.py

Environment variables (set in .env or export before running):
    # Databases
    OLD1_DB_URL          — DSN старого ML-проекту        (project 1)
    OLD2_DB_URL          — DSN старого Recognition-проекту (project 2)
    NEW_DB_URL           — DSN нової об'єднаної бази

    # Old local storage
    LOCAL_FILE_DIR       — корінь старого сховища
                           e.g. /stor/data/amd/datasets

    # MinIO
    MINIO_ENDPOINT       — host:port  e.g. localhost:9000
    MINIO_ACCESS_KEY     — access key
    MINIO_SECRET_KEY     — secret key
    MINIO_BUCKET         — назва бакету  e.g. amd-train
    MINIO_SECURE         — true/false (default: false)
    MINIO_DATASET_PREFIX — префікс всередині бакету (default: datasets)

Example DSN:
    mysql+pymysql://user:pass@host:3306/dbname?charset=utf8mb4
"""
import importlib.util
import sys
import time
import uuid
import json
from datetime import datetime
from pathlib import Path

from minio import Minio
from minio.error import S3Error
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

# Alembic programmatic API
from alembic.runtime.migration import MigrationContext
from alembic.operations import Operations


# ──────────────────────────────────────────────────────────────────────────────
# 0. Config
# ──────────────────────────────────────────────────────────────────────────────

# ── MySQL connection base (no database selected) ──────────────────────────────
_DB_HOST = "localhost"
_DB_PORT = 3306
_DB_USER = "root"
_DB_PASS = "rootroot"
_DB_OPTS = "charset=utf8mb4"

def _dsn(db_name: str) -> str:
    return f"mysql+pymysql://{_DB_USER}:{_DB_PASS}@{_DB_HOST}:{_DB_PORT}/{db_name}?{_DB_OPTS}"

def _server_dsn() -> str:
    """DSN without a database — used to CREATE DATABASE."""
    return f"mysql+pymysql://{_DB_USER}:{_DB_PASS}@{_DB_HOST}:{_DB_PORT}/?{_DB_OPTS}"

# ── Source databases ──────────────────────────────────────────────────────────
OLD1_URL = _dsn("amd_train")
OLD2_URL = _dsn("amd")

# ── Target database — change only this name ───────────────────────────────────
NEW_DB_NAME = "amd_new_1"
NEW_URL     = _dsn(NEW_DB_NAME)

# ── Local file storage ────────────────────────────────────────────────────────
LOCAL_FILE_DIR       = "/stor/data/amd/datasets"

# ── MinIO ─────────────────────────────────────────────────────────────────────
MINIO_ENDPOINT       = ""
MINIO_ACCESS_KEY     = "minioadmin"
MINIO_SECRET_KEY     = ""
MINIO_BUCKET         = "amd-train"
MINIO_SECURE         = False
MINIO_DATASET_PREFIX = "datasets"

# ── Migration file (sits next to migrate.py) ──────────────────────────────────
MIGRATION_FILE = Path(__file__).parent / "d8bd4ddc8854_initial.py"


# ──────────────────────────────────────────────────────────────────────────────
# 1. Auto-create DB + apply migration programmatically
# ──────────────────────────────────────────────────────────────────────────────

def create_database_if_not_exists():
    print(f"[DB SETUP] Checking / creating database '{NEW_DB_NAME}' …")
    engine = create_engine(_server_dsn(), echo=False)
    with engine.connect() as conn:
        conn.execute(text(
            f"CREATE DATABASE IF NOT EXISTS `{NEW_DB_NAME}` "
            f"CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci"
        ))
    engine.dispose()
    print(f"[DB SETUP] Database '{NEW_DB_NAME}' is ready.")


def run_migration_programmatically():
    """
    Load d8bd4ddc8854_initial.py from the same directory as this script and
    call its upgrade() function via Alembic's Operations API, then stamp the
    alembic_version table so the migration is not re-applied on next run.

    How it works:
      - MigrationContext is configured on a live connection
      - Operations instance is pushed onto Alembic's internal stack so that
        all op.create_table / op.add_column … calls inside upgrade() resolve
        to our connection automatically
      - After upgrade(), the revision is written to alembic_version
    """
    print(f"[MIGRATION] Loading {MIGRATION_FILE.name} …")

    if not MIGRATION_FILE.exists():
        raise FileNotFoundError(
            f"Migration file not found: {MIGRATION_FILE}\n"
            "Make sure d8bd4ddc8854_initial.py is in the same directory as migrate.py."
        )

    # ── dynamically import the migration module ───────────────────────────────
    spec   = importlib.util.spec_from_file_location("initial_migration", MIGRATION_FILE)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    revision: str = module.revision  # 'd8bd4ddc8854'

    engine = create_engine(NEW_URL, echo=False)
    with engine.connect() as conn:
        ctx = MigrationContext.configure(conn)

        # ── skip if already applied ───────────────────────────────────────────
        current = ctx.get_current_revision()
        if current == revision:
            print(f"[MIGRATION] Already at revision {revision}, skipping.")
            engine.dispose()
            return

        print(f"[MIGRATION] Applying revision {revision} …")

        # ── push Operations onto Alembic's op stack ───────────────────────────
        # op.* calls inside upgrade() go through alembic.op module-level proxy;
        # Operations.invoke_for_target() sets that proxy for the duration of
        # the with-block and resets it on exit.
        from alembic import op

        ctx = MigrationContext.configure(conn)

        # прив'язуємо proxy op до нашого context
        op_obj = Operations(ctx)

        # 🔑 магія: підміняємо op._proxy
        from alembic import op as alembic_op
        alembic_op._proxy = op_obj

        try:
            module.upgrade()
        finally:
            alembic_op._proxy = None
        # ── stamp alembic_version so the migration is not re-run ──────────────
        conn.execute(text(
            "CREATE TABLE IF NOT EXISTS alembic_version "
            "(version_num VARCHAR(32) NOT NULL PRIMARY KEY)"
        ))
        conn.execute(text(
            "INSERT INTO alembic_version (version_num) VALUES (:rev) "
            "ON DUPLICATE KEY UPDATE version_num = :rev"
        ), {"rev": revision})
        conn.commit()

    engine.dispose()
    print(f"[MIGRATION] Revision {revision} applied and stamped successfully.")


# ──────────────────────────────────────────────────────────────────────────────
# 2. Engines
# ──────────────────────────────────────────────────────────────────────────────

old1_engine = create_engine(OLD1_URL, echo=False)
old2_engine = create_engine(OLD2_URL, echo=False)
new_engine  = create_engine(NEW_URL,  echo=False)

NewSession = sessionmaker(bind=new_engine)

# ──────────────────────────────────────────────────────────────────────────────
# 3. MinIO client
# ──────────────────────────────────────────────────────────────────────────────

minio_client = Minio(
    MINIO_ENDPOINT,
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    secure=MINIO_SECURE,
)


def ensure_bucket():
    if not minio_client.bucket_exists(MINIO_BUCKET):
        minio_client.make_bucket(MINIO_BUCKET)
        print(f"[MINIO] Bucket '{MINIO_BUCKET}' created")
    else:
        print(f"[MINIO] Bucket '{MINIO_BUCKET}' already exists")

# ──────────────────────────────────────────────────────────────────────────────
# 4. Generic DB helpers
# ──────────────────────────────────────────────────────────────────────────────

def fetch_all(engine, sql, **params):
    with engine.connect() as conn:
        result = conn.execute(text(sql), params)
        keys = list(result.keys())
        return [dict(zip(keys, row)) for row in result]


def fetch_one(engine, sql, **params):
    rows = fetch_all(engine, sql, **params)
    return rows[0] if rows else None


def gen_uuid():
    return str(uuid.uuid4())


def now():
    return datetime.utcnow()


def insert_get_id(sess, table: str, row: dict) -> int:
    cols = ", ".join(f"`{k}`" for k in row.keys())
    vals = ", ".join(f":{k}" for k in row.keys())
    sess.execute(text(f"INSERT INTO `{table}` ({cols}) VALUES ({vals})"), row)
    return sess.execute(text("SELECT LAST_INSERT_ID()")).scalar()


def find_id(sess, table: str, col: str, val) -> int | None:
    row = sess.execute(
        text(f"SELECT id FROM `{table}` WHERE `{col}` = :val LIMIT 1"),
        {"val": val}
    ).fetchone()
    return row[0] if row else None


# ──────────────────────────────────────────────────────────────────────────────
# 5. Permission / status mappings
# ──────────────────────────────────────────────────────────────────────────────

OLD2_PERM_MAP = {
    1:  "list_users",
    2:  "manage_users",
    3:  "manage_users",
    4:  "manage_users",
    6:  "list_recognitions",
    7:  "manage_recognitions",
    8:  None,
    9:  "list_models",
    10: "manage_models",
    11: "manage_models",
    12: "manage_models",
}

TRAIN_STATUS_MAP = {
    "RUNNING":   "RUNNING",
    "QUEUED":    "QUEUED",
    "COMPLETED": "COMPLETED",
    "FAILED":    "FAILED",
    None:        "NONE",
    "":          "NONE",
}


def map_old_permissions(old_list: list) -> list:
    result = set()
    for p in old_list:
        mapped = OLD2_PERM_MAP.get(int(p))
        if mapped:
            result.add(mapped)
    return list(result)


# ──────────────────────────────────────────────────────────────────────────────
# 6. Roles
# ──────────────────────────────────────────────────────────────────────────────

def migrate_roles(sess) -> dict:
    print("── Migrating roles ──")
    old_roles = fetch_all(old2_engine, "SELECT * FROM user_role")
    old_to_new = {}

    for r in old_roles:
        name = (r["name"] or "user").strip().lower().replace(" ", "_")

        if "admin" in name:
            permissions = [
                "list_users", "manage_users", "list_roles", "manage_roles",
                "manage_personal_recognitions", "manage_recognitions", "list_recognitions",
                "list_datasets", "manage_datasets", "list_labels", "manage_labels",
                "list_models", "manage_models",
            ]
        else:
            permissions = ["list_recognitions", "manage_personal_recognitions"]

        existing = find_id(sess, "role", "name", name)
        if existing:
            old_to_new[r["id"]] = existing
            print(f"  Role '{name}' already exists → id={existing}")
            continue

        new_id = insert_get_id(sess, "role", {
            "name":        name,
            "default":     0,
            "permissions": json.dumps(permissions),
        })
        old_to_new[r["id"]] = new_id
        print(f"  Role '{name}' → new id={new_id}")

    sess.flush()
    return old_to_new


def seed_notations(sess):
    print("── Seeding notations ──")

    sess.execute(text("""
        INSERT INTO notation (id, name)
        VALUES
            (1, 'human'),
            (2, 'voicemail'),
            (3, 'ring')
        ON DUPLICATE KEY UPDATE name = VALUES(name)
    """))

    sess.flush()

# ──────────────────────────────────────────────────────────────────────────────
# 7. Users
# ──────────────────────────────────────────────────────────────────────────────

def migrate_users_from_old2(sess, role_map: dict) -> dict:
    print("── Migrating users from project-2 ──")
    old_users  = fetch_all(old2_engine, "SELECT * FROM `user`")
    old2_to_new = {}

    for u in old_users:
        new_role_id = role_map.get(u.get("role_id"))
        username    = (u.get("username") or f"user_{u['id']}").strip()

        existing = find_id(sess, "user", "username", username)
        if existing:
            old2_to_new[u["id"]] = existing
            print(f"  User '{username}' already exists → id={existing}")
            continue

        new_id = insert_get_id(sess, "user", {
            "created_date": u.get("created_date") or now(),
            "updated_date": u.get("updated_date") or now(),
            "username":     username,
            "password":     u.get("password") or "",
            "first_name":   u.get("first_name"),
            "last_name":    u.get("last_name"),
            "email":        u.get("email"),
            "api_key":      u.get("api_key"),
            "uuid":         u.get("uuid") or gen_uuid(),
            "role_id":      new_role_id,
        })
        old2_to_new[u["id"]] = new_id
        print(f"  User '{username}' (old2 id={u['id']}) → new id={new_id}")

    sess.flush()
    return old2_to_new


def migrate_users_from_old1(sess, old2_to_new: dict) -> dict:
    print("── Migrating users from project-1 ──")
    old_users   = fetch_all(old1_engine, "SELECT * FROM `user`")
    old1_to_new = {}

    for u in old_users:
        username = (u.get("username") or f"p1_user_{u['id']}").strip()

        existing = find_id(sess, "user", "username", username)
        if existing:
            old1_to_new[u["id"]] = existing
            print(f"  User '{username}' already exists → id={existing}")
            continue

        new_id = insert_get_id(sess, "user", {
            "created_date": u.get("created_date") or now(),
            "updated_date": u.get("updated_date") or now(),
            "username":     username,
            "password":     u.get("password") or "",
            "first_name":   u.get("first_name"),
            "last_name":    u.get("last_name"),
            "email":        u.get("email"),
            "uuid":         gen_uuid(),
        })
        old1_to_new[u["id"]] = new_id
        print(f"  User '{username}' (old1 id={u['id']}) → new id={new_id}")

    sess.flush()
    return old1_to_new


# ──────────────────────────────────────────────────────────────────────────────
# 8. Datasets
# ──────────────────────────────────────────────────────────────────────────────

def migrate_datasets(sess, old1_user_map: dict) -> dict:
    print("── Migrating datasets ──")
    rows       = fetch_all(old1_engine, "SELECT * FROM dataset")
    old_to_new = {}

    for r in rows:
        new_id = insert_get_id(sess, "dataset", {
            "created_date": r.get("created_date") or now(),
            "updated_date": r.get("updated_date") or now(),
            "name":         f"Dataset {r['id']}",
            "country":      r.get("country") or "UA",
            "uuid":         gen_uuid(),
            "user_id":      old1_user_map.get(r.get("user_id")),
        })
        old_to_new[r["id"]] = new_id
        print(f"  Dataset old_id={r['id']} → new_id={new_id}")

    sess.flush()
    return old_to_new


# ──────────────────────────────────────────────────────────────────────────────
# 9. Labels
# ──────────────────────────────────────────────────────────────────────────────

def migrate_labels(sess, old1_user_map: dict) -> dict:
    print("── Migrating labels ──")
    rows       = fetch_all(old1_engine, "SELECT * FROM label")
    old_to_new = {}

    for r in rows:
        country = "UA"
        if r.get("dataset_id"):
            ds_row = fetch_one(
                old1_engine,
                "SELECT country FROM dataset WHERE id = :did",
                did=r["dataset_id"]
            )
            if ds_row and ds_row.get("country"):
                country = ds_row["country"]

        new_id = insert_get_id(sess, "label", {
            "created_date": now(),
            "updated_date": now(),
            "name":         r["name"],
            "description":  r.get("description"),
            "notation_id":  r.get("notation_id"),
            "user_id":      old1_user_map.get(r.get("user_id")),
            "country":      country,
        })
        old_to_new[r["id"]] = new_id
        print(f"  Label '{r['name']}' → {new_id}")

    sess.flush()
    return old_to_new


# ──────────────────────────────────────────────────────────────────────────────
# 10. Audio files (DB rows)
# ──────────────────────────────────────────────────────────────────────────────

def migrate_audio_files(sess, label_map: dict, dataset_map: dict) -> dict:
    """
    Returns mapping: old_audio_file.id → {new_id, uuid, extension, dataset_id, notation_id}
    needed later for file migration to MinIO.
    """
    print("── Migrating audio files (DB rows) ──")
    rows       = fetch_all(old1_engine, "SELECT * FROM audio_file")
    old_to_new = {}

    for r in rows:
        file_uuid = gen_uuid()
        new_id = insert_get_id(sess, "audio_file", {
            "created_date": now(),
            "extension":    r.get("extension"),
            "uuid":         file_uuid,
            "label_id":     label_map.get(r.get("label_id")),
            "dataset_id":   dataset_map.get(r.get("dataset_id")),
            "notation_id":  r.get("notation_id"),
            "storage_key":  None,
        })
        old_to_new[r["id"]] = {
            "new_id":        new_id,
            "uuid":          file_uuid,
            "extension":     r.get("extension") or ".wav",
            "dataset_id":    dataset_map.get(r.get("dataset_id")),
            "notation_id":   r.get("notation_id"),
            "old_dataset_id": r.get("dataset_id"),
        }

    print(f"  Audio file rows migrated: {len(rows)}")
    sess.flush()
    return old_to_new


# ──────────────────────────────────────────────────────────────────────────────
# 11. Tariffs
# ──────────────────────────────────────────────────────────────────────────────

def migrate_tariffs(sess, old2_user_map: dict):
    print("── Migrating tariffs ──")
    rows = fetch_all(old2_engine, "SELECT * FROM tariff")

    for r in rows:
        new_user_id = old2_user_map.get(r.get("user_id"))
        if not new_user_id:
            continue

        existing = sess.execute(
            text("SELECT id FROM tariff WHERE user_id = :uid"),
            {"uid": new_user_id}
        ).fetchone()

        if existing:
            sess.execute(text("""
                UPDATE tariff
                SET active=:active,
                    total=:total,
                    negative=:negative,
                    updated_date=:updated_date
                WHERE user_id = :uid
            """), {
                "active":       1 if r.get("active") else 0,
                "total":        r.get("total", 0),
                "negative":     r.get("negative", 0),
                "updated_date": r.get("updated_date") or now(),
                "uid":          new_user_id,
            })
        else:
            insert_get_id(sess, "tariff", {
                "created_date": r.get("created_date") or now(),
                "updated_date": r.get("updated_date") or now(),
                "active":       1 if r.get("active") else 0,
                "total":        r.get("total", 0),
                "negative":     r.get("negative", 0),
                "user_id":      new_user_id,
            })

    print(f"  Tariffs migrated: {len(rows)}")
    sess.flush()


# ──────────────────────────────────────────────────────────────────────────────
# 12. Recognition Configuration + Rules
# ──────────────────────────────────────────────────────────────────────────────

def _insert_rule(sess, cfg_id: int, rule_index: int, intervals: list, result: str):
    rule_id = insert_get_id(sess, "prediction_rule", {
        "configuration_id": cfg_id,
        "rule_index":       rule_index,
        "result":           result,
    })
    for iv_idx, val in enumerate(intervals, start=1):
        insert_get_id(sess, "prediction_rule_interval", {
            "rule_id":        rule_id,
            "interval_index": iv_idx,
            "value":          1 if val else 0,
        })


def migrate_recognition_config(sess, old2_user_map: dict):
    print("── Migrating recognition configurations ──")
    rows = fetch_all(old2_engine, "SELECT * FROM recognition_configuration")

    for r in rows:
        new_user_id = old2_user_map.get(r.get("user_id"))
        if not new_user_id:
            continue

        existing_cfg = sess.execute(
            text("SELECT id FROM recognition_configuration WHERE user_id = :uid"),
            {"uid": new_user_id}
        ).fetchone()

        if existing_cfg:
            cfg_id = existing_cfg[0]
        else:
            cfg_id = insert_get_id(sess, "recognition_configuration", {
                "encoding":        r.get("encoding") or "PCMU",
                "rate":            r.get("rate") or 8000,
                "interval_length": r.get("interval_length") or 2,
                "predictions":     r.get("predictions") or 2,
                "user_id":         new_user_id,
            })

        raw      = r.get("prediction_criteria")
        criteria = None
        if raw:
            try:
                criteria = json.loads(raw) if isinstance(raw, str) else raw
            except Exception:
                print(f"  WARNING: Cannot parse prediction_criteria for user_id={r['user_id']}: {raw!r}")

        if not criteria or not isinstance(criteria, list):
            _insert_rule(sess, cfg_id, rule_index=1, intervals=[True], result="human")
            continue

        for rule_idx, interval_values in enumerate(criteria, start=1):
            if not isinstance(interval_values, list):
                continue
            result_val = "human" if all(interval_values) else "voicemail"
            _insert_rule(sess, cfg_id, rule_index=rule_idx,
                         intervals=interval_values, result=result_val)

    print(f"  Recognition configs migrated: {len(rows)}")
    sess.flush()


# ──────────────────────────────────────────────────────────────────────────────
# 13. Recognitions
# ──────────────────────────────────────────────────────────────────────────────

def migrate_recognitions(sess, old2_user_map: dict):
    print("── Migrating recognitions ──")
    BATCH_SIZE = 2000
    LOG_EVERY  = 10000
    start_time = time.time()
    batch      = []
    i          = 0

    with old2_engine.connect() as conn:
        result = conn.execution_options(stream_results=True).execute(
            text("SELECT * FROM recognition")
        )

        for i, row in enumerate(result, 1):
            r = dict(row._mapping)
            batch.append({
                "created_date":   r.get("created_date") or now(),
                "final":          1 if r.get("final", True) else 0,
                "request_uuid":   r.get("request_uuid"),
                "audio_uuid":     r.get("audio_uuid"),
                "confidence":     r.get("confidence"),
                "prediction":     r.get("prediction"),
                "extension":      r.get("extension"),
                "company_id":     r.get("company_id"),
                "campaign_id":    r.get("campaign_id"),
                "application_id": r.get("application_id"),
                "user_id":        old2_user_map.get(r.get("user_id")),
            })

            if len(batch) >= BATCH_SIZE:
                sess.execute(text("""
                    INSERT INTO recognition
                    (created_date, final, request_uuid, audio_uuid, confidence,
                     prediction, extension, company_id, campaign_id, application_id, user_id)
                    VALUES (:created_date, :final, :request_uuid, :audio_uuid, :confidence,
                            :prediction, :extension, :company_id, :campaign_id, :application_id, :user_id)
                """), batch)
                sess.commit()
                batch.clear()

            if i % LOG_EVERY == 0:
                elapsed = time.time() - start_time
                rate    = i / elapsed
                print(f"  {i} rows | {rate:.0f} rows/sec | {(elapsed / 60):.1f} min elapsed")

        if batch:
            sess.execute(text("""
                INSERT INTO recognition
                (created_date, final, request_uuid, audio_uuid, confidence,
                 prediction, extension, company_id, campaign_id, application_id, user_id)
                VALUES (:created_date, :final, :request_uuid, :audio_uuid, :confidence,
                        :prediction, :extension, :company_id, :campaign_id, :application_id, :user_id)
            """), batch)
            sess.commit()

    print(f"  Recognitions migrated: {i}")


# ──────────────────────────────────────────────────────────────────────────────
# 14. Files → MinIO
# ──────────────────────────────────────────────────────────────────────────────

def _notation_folder(notation_name: str) -> str:
    return notation_name.lower().strip()


def migrate_files_to_minio(sess, audio_file_map: dict):
    print("── Migrating audio files → MinIO ──")

    notation_rows  = sess.execute(text("SELECT id, name FROM notation")).mappings().all()
    notation_map   = {r["id"]: r["name"] for r in notation_rows}

    dataset_rows    = sess.execute(
        text("SELECT id, uuid, country, user_id FROM dataset")
    ).mappings().all()
    new_dataset_info = {r["id"]: dict(r) for r in dataset_rows}

    old_datasets     = fetch_all(old1_engine, "SELECT id, user_id, country FROM dataset")
    old_dataset_info = {r["id"]: r for r in old_datasets}

    ok = skipped = failed = deleted = 0
    total = len(audio_file_map)

    for i, (old_audio_id, info) in enumerate(audio_file_map.items(), 1):

        def delete_audio_record(reason: str):
            print(f"  [{i}/{total}] WARNING: {reason} — deleting audio_file id={info['new_id']}")
            sess.execute(
                text("DELETE FROM audio_file WHERE id = :id"),
                {"id": info["new_id"]},
            )

        if not info["notation_id"]:
            delete_audio_record(f"No notation_id for old_audio_id={old_audio_id}")
            deleted += 1
            continue

        notation_name = notation_map.get(info["notation_id"])
        if not notation_name:
            delete_audio_record(f"Unknown notation_id={info['notation_id']} for old_audio_id={old_audio_id}")
            deleted += 1
            continue

        notation_folder = _notation_folder(notation_name)
        extension       = info["extension"]
        if not extension.startswith("."):
            extension = f".{extension}"

        old_ds = old_dataset_info.get(info["old_dataset_id"])
        if not old_ds:
            print(f"  [{i}/{total}] WARNING: Old dataset not found for old_audio_id={old_audio_id}")
            skipped += 1
            continue

        local_path = (
            Path(LOCAL_FILE_DIR)
            / str(old_ds["user_id"])
            / str(old_ds["country"])
            / notation_folder
            / f"{old_audio_id}{extension}"
        )

        if not local_path.exists():
            delete_audio_record(f"File not found on disk: {local_path}")
            deleted += 1
            continue

        new_ds = new_dataset_info.get(info["dataset_id"])
        if not new_ds:
            print(f"  [{i}/{total}] WARNING: New dataset not found for old_audio_id={old_audio_id}")
            skipped += 1
            continue

        minio_key = (
            f"{MINIO_DATASET_PREFIX}/{new_ds['uuid']}"
            f"/{notation_folder}/{info['new_id']}{extension}"
        )

        print(f"  [{i}/{total}] {local_path.name} → {minio_key}")

        try:
            minio_client.fput_object(
                bucket_name=MINIO_BUCKET,
                object_name=minio_key,
                file_path=str(local_path),
            )
        except S3Error as e:
            print(f"  [{i}/{total}] ERROR MinIO: {e}")
            failed += 1
            continue

        ok += 1

    sess.commit()
    print(f"  Files → MinIO: OK={ok}  DELETED={deleted}  SKIPPED={skipped}  FAILED={failed}")


# ──────────────────────────────────────────────────────────────────────────────
# 15. Main
# ──────────────────────────────────────────────────────────────────────────────

def run():
    # ── Step 0: create DB + apply migration programmatically ─────────────────
    create_database_if_not_exists()
    run_migration_programmatically()

    # ── Step 1: MinIO bucket ──────────────────────────────────────────────────
    ensure_bucket()

    sess = NewSession()
    try:
        print("═══════════════════════════════════════════")
        print("  Starting migration  (MariaDB → MariaDB + MinIO)")
        print("═══════════════════════════════════════════")

        seed_notations(sess)
        role_map        = migrate_roles(sess)
        old2_user_map   = migrate_users_from_old2(sess, role_map)
        old1_user_map   = migrate_users_from_old1(sess, old2_user_map)
        migrate_tariffs(sess, old2_user_map)

        dataset_map     = migrate_datasets(sess, old1_user_map)
        label_map       = migrate_labels(sess, old1_user_map)

        audio_file_map  = migrate_audio_files(sess, label_map, dataset_map)

        migrate_recognition_config(sess, old2_user_map)
        # migrate_recognitions(sess, old2_user_map)

        sess.commit()

        migrate_files_to_minio(sess, audio_file_map)

        sess.commit()
        print("═══════════════════════════════════════════")
        print("  Migration COMPLETED successfully ✓")
        print("═══════════════════════════════════════════")

    except Exception as e:
        sess.rollback()
        print(f"ERROR: Migration FAILED: {e}", file=sys.stderr)
        raise
    finally:
        sess.close()


if __name__ == "__main__":
    run()