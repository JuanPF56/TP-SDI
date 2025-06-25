"""Storage module for handling batch messages on disk."""

import os
import json
import time
import fcntl
from pathlib import Path
from dataclasses import asdict

from batch_message import BatchMessage
from common.logger import get_logger

logger = get_logger("Storage")


def load_clients_from_disk():
    """Load client IDs from the storage directory."""
    storage_dir = Path("storage")
    if not storage_dir.is_dir():
        logger.info("No storage directory found")
        return []

    clients_id = []
    for client_dir in storage_dir.iterdir():
        if client_dir.is_dir() and not client_dir.name.startswith("."):
            clients_id.append(client_dir.name)
    logger.debug("Loaded clients from disk: %s", clients_id)
    return clients_id


def custom_asdict(obj):
    try:
        if isinstance(obj, list):
            return [custom_asdict(i) for i in obj]
        elif hasattr(obj, "__dict__") or hasattr(obj, "__dataclass_fields__"):
            return {k: custom_asdict(v) for k, v in asdict(obj).items()}
        else:
            return obj
    except Exception as e:
        logger.error("Error in custom_asdict serialization: %s", e)
        return str(obj)


def save_batch_to_disk(batch: BatchMessage):
    client_dir = Path("storage") / batch.client_id
    client_dir.mkdir(parents=True, exist_ok=True)
    final_path = client_dir / f"{batch.message_code}_{batch.current_batch}.json"
    temp_path = (
        client_dir
        / f".tmp_{batch.message_code}_{batch.current_batch}_{int(time.time() * 1000)}.json"
    )
    try:
        with open(temp_path, "w", encoding="utf-8") as tmp_file:
            fcntl.flock(tmp_file.fileno(), fcntl.LOCK_EX)
            json.dump(
                custom_asdict(batch),
                tmp_file,
                ensure_ascii=False,
                indent=2,
                separators=(",", ": "),
            )
            tmp_file.flush()
            os.fsync(tmp_file.fileno())
        temp_path.rename(final_path)
        logger.debug("Saved batch to disk: %s", final_path)
    except Exception:
        if temp_path.exists():
            temp_path.unlink(missing_ok=True)
        raise


def safe_delete_batch_file(batch: BatchMessage):
    path = (
        Path("storage")
        / batch.client_id
        / f"{batch.message_code}_{batch.current_batch}.json"
    )
    if not path.exists():
        return
    for attempt in range(3):
        try:
            path.unlink()
            logger.debug("Deleted batch file: %s", path)
            return
        except OSError as e:
            if attempt < 2:
                time.sleep(0.1 * (attempt + 1))
            else:
                logger.error(
                    "Failed to delete batch file %s after retries: %s", path, e
                )


def load_batches_from_disk(client_id: str):
    client_dir = Path("storage") / client_id
    if not client_dir.is_dir():
        logger.info("No storage dir for client %s", client_id)
        return []

    batches = []
    json_files = [
        f
        for f in client_dir.iterdir()
        if f.is_file() and f.suffix == ".json" and not f.name.startswith(".tmp_")
    ]
    json_files.sort(key=lambda x: x.name)
    for file_path in json_files:
        try:
            if file_path.stat().st_size < 10:
                move_corrupted_file(file_path)
                continue
            with open(file_path, "r", encoding="utf-8") as f:
                fcntl.flock(f.fileno(), fcntl.LOCK_SH)
                data = json.load(f)
            if not validate_batch_data(data):
                move_corrupted_file(file_path)
                continue
            batch = BatchMessage.from_json_with_casting(data)
            batches.append(batch)
        except Exception as e:
            logger.error("Error loading batch file %s: %s", file_path, e)
            move_corrupted_file(file_path)
    return batches


def move_corrupted_file(file_path: Path):
    corrupted_dir = file_path.parent / "corrupted"
    corrupted_dir.mkdir(exist_ok=True)
    timestamp = int(time.time() * 1000)
    new_name = f"{file_path.stem}_{timestamp}{file_path.suffix}"
    corrupted_path = corrupted_dir / new_name
    try:
        file_path.rename(corrupted_path)
        logger.info("Moved corrupted file %s to %s", file_path, corrupted_path)
    except Exception as e:
        logger.error("Failed to move corrupted file %s: %s", file_path, e)
        try:
            file_path.unlink()
        except Exception:
            logger.error("Failed to delete corrupted file %s", file_path)


def validate_batch_data(data):
    if not isinstance(data, dict):
        return False
    required = [
        "message_id",
        "message_code",
        "client_id",
        "current_batch",
        "is_last_batch",
    ]
    return all(field in data for field in required)
