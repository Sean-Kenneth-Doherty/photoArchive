import copy
import os
import threading
import time
import traceback

import settings

_install_lock = threading.Lock()
_install_state = {
    "running": False,
    "status": "idle",
    "message": "",
    "model_id": "",
    "revision": "",
    "model_dir": "",
    "started_at": None,
    "finished_at": None,
    "last_error": "",
}


def _snapshot_config() -> dict:
    cfg = settings.get_settings()
    return {
        "model_id": cfg["embed_model_id"],
        "revision": cfg["embed_model_revision"],
        "model_dir": cfg["embed_model_dir"],
    }


def _copy_state() -> dict:
    with _install_lock:
        return copy.deepcopy(_install_state)


def _set_state(**updates):
    with _install_lock:
        _install_state.update(updates)


def model_files_present(model_dir: str) -> bool:
    if not model_dir or not os.path.isdir(model_dir):
        return False

    sentinels = [
        "modules.json",
        "config_sentence_transformers.json",
        "sentence_bert_config.json",
        "config.json",
    ]
    if any(os.path.exists(os.path.join(model_dir, name)) for name in sentinels):
        return True

    for root, _dirs, files in os.walk(model_dir):
        for filename in files:
            if filename.endswith((".safetensors", ".bin")):
                return True
    return False


def get_model_status() -> dict:
    config = _snapshot_config()
    state = _copy_state()
    return {
        **config,
        "installed": model_files_present(config["model_dir"]),
        "install": state,
    }


def _install_model_sync(model_id: str, revision: str, model_dir: str):
    try:
        from huggingface_hub import snapshot_download

        os.makedirs(model_dir, exist_ok=True)
        _set_state(
            running=True,
            status="downloading",
            message=f"Downloading {model_id}",
            model_id=model_id,
            revision=revision,
            model_dir=model_dir,
            started_at=time.time(),
            finished_at=None,
            last_error="",
        )
        snapshot_download(
            repo_id=model_id,
            revision=revision or "main",
            local_dir=model_dir,
        )
        _set_state(
            running=False,
            status="installed",
            message=f"Installed {model_id}",
            finished_at=time.time(),
            last_error="",
        )
    except Exception as exc:
        _set_state(
            running=False,
            status="error",
            message=str(exc),
            finished_at=time.time(),
            last_error=traceback.format_exc(),
        )


def start_model_install() -> dict:
    config = _snapshot_config()
    with _install_lock:
        if _install_state["running"]:
            return copy.deepcopy(_install_state)

        thread = threading.Thread(
            target=_install_model_sync,
            args=(config["model_id"], config["revision"], config["model_dir"]),
            daemon=True,
            name="photoarchive-model-install",
        )
        thread.start()
        _install_state["running"] = True
        _install_state["status"] = "queued"
        _install_state["message"] = f"Starting install for {config['model_id']}"
        _install_state["model_id"] = config["model_id"]
        _install_state["revision"] = config["revision"]
        _install_state["model_dir"] = config["model_dir"]
        _install_state["started_at"] = time.time()
        _install_state["finished_at"] = None
        _install_state["last_error"] = ""
        return copy.deepcopy(_install_state)
