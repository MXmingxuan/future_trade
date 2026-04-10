"""
MiniMax LLM API 客户端封装

支持通过 prompt 字符串调用 MiniMax API，返回结构化 JSON。
处理 MiniMax-M2.7 的 thinking 块输出。
"""
import json
import logging
import re
from typing import Any

import requests

logger = logging.getLogger(__name__)

# 默认 API 配置
DEFAULT_API_BASE = "http://127.0.0.1:5001/v1"
DEFAULT_MODEL = "MiniMax-M2.7"


class LLMClient:
    """MiniMax API 调用封装"""

    def __init__(self, api_base: str = DEFAULT_API_BASE, model: str = DEFAULT_MODEL):
        self.api_base = api_base.rstrip("/")
        self.model = model

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
            "model": self.model,
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

        # 解析 JSON，处理 thinking 块和 markdown 代码块
        result = self._extract_json(content)
        return result

    def _extract_json(self, text: str) -> dict[str, Any]:
        """
        从 LLM 输出中提取 JSON。

        处理：
        1. MiniMax thinking 块：<think>...</think>
        2. Markdown 代码块：```json ... ```
        3. 直接输出的 JSON

        Args:
            text: LLM 原始输出

        Returns:
            dict: 解析后的 JSON

        Raises:
            RuntimeError: 无法从输出中提取 JSON
        """
        # 步骤1：去除 thinking 块
        text = re.sub(r'<think>.*?</think>', '', text, flags=re.DOTALL).strip()

        # 步骤2：去除 markdown 代码块
        if text.startswith('```json'):
            text = text[7:]
        elif text.startswith('```'):
            text = text[3:]
        if text.endswith('```'):
            text = text[:-3]
        text = text.strip()

        # 步骤3：尝试直接解析
        try:
            return json.loads(text)
        except json.JSONDecodeError:
            pass

        # 步骤4：尝试用正则提取第一个 JSON 对象
        # 匹配 { ... } 结构，能处理嵌套
        try:
            # 先找最外层的 {
            start = text.index('{')
            # 从后往前找最后一个 }
            end = text.rindex('}') + 1
            json_str = text[start:end]
            return json.loads(json_str)
        except (ValueError, json.JSONDecodeError) as e:
            logger.error(f"Failed to parse JSON from LLM response: {text[:300]}")
            raise RuntimeError(
                f"LLM returned non-JSON (tried strip, regex): {e}\n"
                f"Response preview: {text[:500]}"
            ) from e


# 全局单例
_llm_client: LLMClient | None = None


def get_llm_client() -> LLMClient:
    """获取全局 LLM 客户端实例"""
    global _llm_client
    if _llm_client is None:
        _llm_client = LLMClient()
    return _llm_client
