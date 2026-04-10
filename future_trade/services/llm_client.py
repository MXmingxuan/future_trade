"""
MiniMax LLM API 客户端封装

支持通过 prompt 字符串调用 MiniMax API，返回结构化 JSON。
"""
import json
import logging
from typing import Any

import requests

from config.postgres_config import get_settings

logger = logging.getLogger(__name__)


class LLMClient:
    """MiniMax API 调用封装"""

    def __init__(self, api_base: str = "http://127.0.0.1:5001/v1"):
        self.api_base = api_base.rstrip("/")
        self.settings = get_settings()

    def analyze_notice(self, prompt: str) -> dict[str, Any]:
        """
        调用 LLM 分析单篇公告，返回结构化 JSON。

        Args:
            prompt: 格式化后的提示词

        Returns:
            dict: 解析后的 JSON 结果

        Raises:
            RuntimeError: API 调用失败或返回非 JSON
        """
        return self._call(prompt)

    def prompt(self, prompt: str, system_prompt: str | None = None) -> dict[str, Any]:
        """
        通用 prompt 接口，返回结构化 JSON。

        Args:
            prompt: 用户 prompt
            system_prompt: 系统 prompt（可选）

        Returns:
            dict: 解析后的 JSON 结果
        """
        return self._call(prompt, system_prompt=system_prompt)

    def _call(self, prompt: str, system_prompt: str | None = None) -> dict[str, Any]:
        """
        实际调用 MiniMax proxy API。

        Args:
            prompt: 用户 prompt
            system_prompt: 系统 prompt（可选）

        Returns:
            dict: 解析后的 JSON
        """
        messages = []
        if system_prompt:
            messages.append({"role": "system", "content": system_prompt})
        messages.append({"role": "user", "content": prompt})

        payload = {
            "model": "MiniMax-M2.7",
            "messages": messages,
            "temperature": 0.3,
            "max_tokens": 2048,
        }

        try:
            resp = requests.post(
                f"{self.api_base}/chat/completions",
                json=payload,
                timeout=120,
                headers={"Content-Type": "application/json"},
            )
            resp.raise_for_status()
            data = resp.json()
        except requests.RequestException as e:
            logger.error(f"LLM API call failed: {e}")
            raise RuntimeError(f"LLM API call failed: {e}") from e

        try:
            content = data["choices"][0]["message"]["content"]
        except (KeyError, IndexError) as e:
            logger.error(f"Unexpected API response format: {data}")
            raise RuntimeError(f"Unexpected API response: {e}") from e

        # 尝试提取 JSON（可能包含在 markdown 代码块中）
        text = content.strip()
        if text.startswith("```json"):
            text = text[7:]
        elif text.startswith("```"):
            text = text[3:]
        if text.endswith("```"):
            text = text[:-3]

        try:
            result = json.loads(text.strip())
            return result
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse JSON from LLM response: {text[:200]}")
            raise RuntimeError(f"LLM returned non-JSON: {e}") from e


# 全局单例
_llm_client: LLMClient | None = None


def get_llm_client() -> LLMClient:
    """获取全局 LLM 客户端实例"""
    global _llm_client
    if _llm_client is None:
        _llm_client = LLMClient()
    return _llm_client
