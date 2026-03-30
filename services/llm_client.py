import httpx
from typing import Any, Optional


def chat_completions(
    base_url: str,
    model: str,
    messages: list[Any],
    api_key: Optional[str] = None,
    timeout: float = 420.0,
    temperature: float = 0.35,
):
    """
    OpenAI 호환 Chat Completions (Ollama: base_url=http://127.0.0.1:11434/v1, model 예: qwen2.5:14b).
    vLLM도 동일 엔드포인트.
    """
    url = base_url.rstrip("/") + "/chat/completions"
    headers = {"Content-Type": "application/json"}
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"
    payload = {
        "model": model,
        "messages": messages,
        "temperature": temperature,
    }
    with httpx.Client(timeout=timeout) as client:
        r = client.post(url, headers=headers, json=payload)
        r.raise_for_status()
        data = r.json()
    return data["choices"][0]["message"]["content"]
