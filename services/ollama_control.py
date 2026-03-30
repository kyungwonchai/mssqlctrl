import os
import shutil
import subprocess
import threading
import time
from typing import Any, Dict, List, Optional, Set, Tuple

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


def try_start_ollama_server(origin: str, wait_seconds: float = 30.0) -> Dict[str, Any]:
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


# 웹에서 체크해 설치하기 쉬운 인기 태그 (Ollama 라이브러리 기준, 필요 시 수정)
SUGGESTED_PULL_MODELS: List[Dict[str, str]] = [
    {"name": "qwen2.5:7b", "label": "Qwen 2.5 7B (가벼움)"},
    {"name": "qwen2.5:14b", "label": "Qwen 2.5 14B"},
    {"name": "qwen2.5:32b", "label": "Qwen 2.5 32B"},
    {"name": "qwen3:8b", "label": "Qwen3 8B"},
    {"name": "llama3.2:3b", "label": "Llama 3.2 3B"},
    {"name": "gemma3:4b", "label": "Gemma 3 4B"},
]


def suggested_models_catalog() -> List[Dict[str, str]]:
    return list(SUGGESTED_PULL_MODELS)


_pull_lock = threading.Lock()
_pull_state: Dict[str, Any] = {
    "active": False,
    "model": None,
    "error": None,
    "log_tail": "",
    "queue": [],
    "queue_index": 0,
    "queue_total": 0,
}


def pull_status() -> Dict[str, Any]:
    with _pull_lock:
        return {
            "active": _pull_state["active"],
            "model": _pull_state["model"],
            "error": _pull_state["error"],
            "log_tail": _pull_state.get("log_tail") or "",
            "queue": list(_pull_state.get("queue") or []),
            "queue_index": int(_pull_state.get("queue_index") or 0),
            "queue_total": int(_pull_state.get("queue_total") or 0),
        }


def start_pull_sequence_in_thread(model_names: List[str]) -> Tuple[bool, Optional[str]]:
    """선택한 모델 이름들을 순서대로 ollama pull (한 번에 하나씩, 백그라운드)."""
    raw = [str(n).strip() for n in model_names if n and str(n).strip()]
    uniq: List[str] = []
    seen: Set[str] = set()
    for n in raw:
        if n not in seen:
            seen.add(n)
            uniq.append(n)
    if not uniq:
        return False, "모델 목록이 비었습니다."
    with _pull_lock:
        if _pull_state["active"]:
            return False, "이미 모델 설치(pull) 작업이 진행 중입니다."
        _pull_state["active"] = True
        _pull_state["error"] = None
        _pull_state["log_tail"] = ""
        _pull_state["queue"] = uniq
        _pull_state["queue_total"] = len(uniq)
        _pull_state["queue_index"] = 0
        _pull_state["model"] = uniq[0]

    def job():
        exe = shutil.which("ollama")
        try:
            if not exe:
                raise RuntimeError("ollama CLI 가 PATH 에 없습니다.")
            all_tail = []
            for i, name in enumerate(uniq):
                with _pull_lock:
                    _pull_state["model"] = name
                    _pull_state["queue_index"] = i + 1
                p = subprocess.run(
                    [exe, "pull", name],
                    capture_output=True,
                    text=True,
                    timeout=7200,
                )
                tail = ((p.stderr or "") + "\n" + (p.stdout or ""))[-1200:]
                all_tail.append(f"=== {name} ===\n{tail}")
                if p.returncode != 0:
                    raise RuntimeError(
                        f"{name}: "
                        + ((p.stderr or p.stdout or "").strip() or f"exit {p.returncode}")
                    )
            with _pull_lock:
                _pull_state["log_tail"] = "\n".join(all_tail)[-3500:]
        except Exception as e:
            with _pull_lock:
                _pull_state["error"] = str(e)
        finally:
            with _pull_lock:
                _pull_state["active"] = False
                _pull_state["queue"] = []
                _pull_state["queue_index"] = 0
                _pull_state["queue_total"] = 0

    threading.Thread(target=job, daemon=True).start()
    return True, None


def start_pull_in_thread(model_name: str) -> Tuple[bool, Optional[str]]:
    """단일 ollama pull (백그라운드)."""
    return start_pull_sequence_in_thread([model_name])
