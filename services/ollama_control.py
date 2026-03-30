import os
import shutil
import subprocess
import threading
import time
from typing import Any, Dict, List, Optional, Tuple

import httpx


def v1_base_to_origin(llm_v1_base: str) -> str:
    """OpenAI 호환 베이스 URL(http://host:11434/v1) → Ollama 네이티브 오리진(http://host:11434)."""
    b = (llm_v1_base or "").strip().rstrip("/")
    if b.lower().endswith("/v1"):
        b = b[:-3].rstrip("/")
    return b or "http://127.0.0.1:11434"


def ollama_ping(origin: str, timeout: float = 2.0) -> Tuple[bool, Optional[str]]:
    origin = origin.rstrip("/")
    try:
        r = httpx.get(f"{origin}/api/tags", timeout=timeout)
        if r.status_code == 200:
            return True, None
        return False, f"HTTP {r.status_code}"
    except Exception as e:
        return False, str(e)


def ollama_version(origin: str) -> Optional[str]:
    origin = origin.rstrip("/")
    try:
        r = httpx.get(f"{origin}/api/version", timeout=5.0)
        if r.status_code == 200:
            return r.json().get("version")
    except Exception:
        pass
    return None


def ollama_list_models(origin: str) -> List[Dict[str, Any]]:
    origin = origin.rstrip("/")
    r = httpx.get(f"{origin}/api/tags", timeout=30.0)
    r.raise_for_status()
    data = r.json()
    models = data.get("models") or []
    out = []
    for m in models:
        name = m.get("name") or m.get("model")
        if not name:
            continue
        out.append({
            "name": name,
            "size": m.get("size"),
            "digest": m.get("digest"),
            "modified_at": m.get("modified_at"),
        })
    out.sort(key=lambda x: (x["name"] or "").lower())
    return out


def try_start_ollama_server(origin: str, wait_seconds: float = 30.0) -> dict[str, Any]:
    """
    ollama serve 를 백그라운드로 띄우고 API 응답을 잠시 폴링한다.
    보안: OLLAMA_ALLOW_WEB_START=0 이면 거부.
    """
    if os.getenv("OLLAMA_ALLOW_WEB_START", "1").lower() in ("0", "false", "no"):
        return {
            "ok": False,
            "error": "웹에서 Ollama 시작이 비활성화되어 있습니다(OLLAMA_ALLOW_WEB_START=0).",
        }
    ok, err = ollama_ping(origin)
    if ok:
        ver = ollama_version(origin)
        return {"ok": True, "already_running": True, "version": ver}
    exe = shutil.which("ollama")
    if not exe:
        return {
            "ok": False,
            "error": "PATH에 ollama 가 없습니다. https://ollama.com 에 시스템에 맞게 설치하세요.",
        }
    try:
        subprocess.Popen(
            [exe, "serve"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            start_new_session=True,
        )
    except Exception as e:
        return {"ok": False, "error": str(e)}
    deadline = time.time() + wait_seconds
    while time.time() < deadline:
        ok, _ = ollama_ping(origin, timeout=1.5)
        if ok:
            return {
                "ok": True,
                "started": True,
                "version": ollama_version(origin),
            }
        time.sleep(0.35)
    return {
        "ok": False,
        "error": f"{int(wait_seconds)}초 안에 {origin} 에 연결하지 못했습니다. 방화벽/포트 또는 수동으로 ollama serve 를 확인하세요.",
    }


_pull_lock = threading.Lock()
_pull_state: dict[str, Any] = {
    "active": False,
    "model": None,
    "error": None,
    "log_tail": "",
}


def pull_status() -> dict[str, Any]:
    with _pull_lock:
        return {
            "active": _pull_state["active"],
            "model": _pull_state["model"],
            "error": _pull_state["error"],
            "log_tail": _pull_state.get("log_tail") or "",
        }


def start_pull_in_thread(model_name: str) -> Tuple[bool, Optional[str]]:
    """ollama pull <name> 를 백그라운드에서 실행 (Ollama 데몬이 떠 있어야 함)."""
    name = (model_name or "").strip()
    if not name:
        return False, "모델 이름이 비었습니다."
    with _pull_lock:
        if _pull_state["active"]:
            return False, "이미 다른 모델 pull 이 진행 중입니다."
        _pull_state["active"] = True
        _pull_state["model"] = name
        _pull_state["error"] = None
        _pull_state["log_tail"] = ""

    def job():
        try:
            exe = shutil.which("ollama")
            if not exe:
                raise RuntimeError("ollama CLI 가 PATH 에 없습니다.")
            p = subprocess.run(
                [exe, "pull", name],
                capture_output=True,
                text=True,
                timeout=7200,
            )
            tail = ((p.stderr or "") + "\n" + (p.stdout or ""))[-2500:]
            with _pull_lock:
                _pull_state["log_tail"] = tail.strip()
            if p.returncode != 0:
                raise RuntimeError(
                    (p.stderr or p.stdout or "").strip() or f"exit {p.returncode}"
                )
        except Exception as e:
            with _pull_lock:
                _pull_state["error"] = str(e)
        finally:
            with _pull_lock:
                _pull_state["active"] = False

    threading.Thread(target=job, daemon=True).start()
    return True, None
