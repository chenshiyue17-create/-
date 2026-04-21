"""
LLM客户端封装
统一使用OpenAI格式调用
"""

import json
import re
import subprocess
import tempfile
from pathlib import Path
from typing import Optional, Dict, Any, List

from ..config import Config


class LLMClient:
    """LLM客户端"""
    
    def __init__(
        self,
        api_key: Optional[str] = None,
        base_url: Optional[str] = None,
        model: Optional[str] = None
    ):
        self.api_key = api_key or Config.LLM_API_KEY
        self.base_url = base_url or Config.LLM_BASE_URL
        self.model = model or Config.LLM_MODEL_NAME
        self.codex_command = Config.MIROFISH_CODEX_COMMAND
        self.codex_model = Config.MIROFISH_CODEX_MODEL
        self.timeout_seconds = Config.MIROFISH_CODEX_TIMEOUT_SECONDS
        self.use_codex = Config.use_codex_llm() and not self.api_key

        if self.use_codex:
            if not self.codex_command:
                raise ValueError("Codex CLI 未配置")
            self.client = None
        else:
            if not self.api_key:
                raise ValueError("LLM_API_KEY 未配置")
            try:
                from openai import OpenAI
            except Exception as exc:
                raise RuntimeError(f"OpenAI SDK 不可用: {exc}") from exc
            self.client = OpenAI(
                api_key=self.api_key,
                base_url=self.base_url
            )
    
    def chat(
        self,
        messages: List[Dict[str, str]],
        temperature: float = 0.7,
        max_tokens: int = 4096,
        response_format: Optional[Dict] = None
    ) -> str:
        """
        发送聊天请求
        
        Args:
            messages: 消息列表
            temperature: 温度参数
            max_tokens: 最大token数
            response_format: 响应格式（如JSON模式）
            
        Returns:
            模型响应文本
        """
        if self.use_codex:
            return self._chat_with_codex(messages=messages, response_format=response_format)

        kwargs = {
            "model": self.model,
            "messages": messages,
            "temperature": temperature,
            "max_tokens": max_tokens,
        }

        if response_format:
            kwargs["response_format"] = response_format

        response = self.client.chat.completions.create(**kwargs)
        content = response.choices[0].message.content
        content = re.sub(r'<think>[\s\S]*?</think>', '', content).strip()
        return content
    
    def chat_json(
        self,
        messages: List[Dict[str, str]],
        temperature: float = 0.3,
        max_tokens: int = 4096
    ) -> Dict[str, Any]:
        """
        发送聊天请求并返回JSON
        
        Args:
            messages: 消息列表
            temperature: 温度参数
            max_tokens: 最大token数
            
        Returns:
            解析后的JSON对象
        """
        response = self.chat(
            messages=messages,
            temperature=temperature,
            max_tokens=max_tokens,
            response_format={"type": "json_object"}
        )
        # 清理markdown代码块标记
        cleaned_response = response.strip()
        cleaned_response = re.sub(r'^```(?:json)?\s*\n?', '', cleaned_response, flags=re.IGNORECASE)
        cleaned_response = re.sub(r'\n?```\s*$', '', cleaned_response)
        cleaned_response = cleaned_response.strip()

        try:
            return json.loads(cleaned_response)
        except json.JSONDecodeError:
            raise ValueError(f"LLM返回的JSON格式无效: {cleaned_response}")

    def _chat_with_codex(
        self,
        messages: List[Dict[str, str]],
        response_format: Optional[Dict[str, Any]] = None,
    ) -> str:
        prompt_parts: List[str] = []
        for message in messages:
            role = str(message.get("role") or "user").upper()
            prompt_parts.append(f"{role}:\n{message.get('content', '')}".strip())
        if response_format and response_format.get("type") == "json_object":
            prompt_parts.append("FINAL REQUIREMENT:\nReturn only one valid JSON object. Do not add markdown fences or commentary.")
        prompt = "\n\n".join(prompt_parts).strip()

        with tempfile.NamedTemporaryFile(prefix="mirofish-codex-", suffix=".txt", delete=False) as handle:
            output_path = Path(handle.name)
        try:
            command = [
                self.codex_command,
                "exec",
                "--skip-git-repo-check",
                "--sandbox",
                "danger-full-access",
                "--cd",
                str(Path(__file__).resolve().parents[3]),
                "--output-last-message",
                str(output_path),
                "--color",
                "never",
            ]
            if self.codex_model:
                command.extend(["--model", self.codex_model])
            result = subprocess.run(
                command,
                input=prompt,
                text=True,
                capture_output=True,
                timeout=self.timeout_seconds,
            )
            if result.returncode != 0:
                stderr = (result.stderr or result.stdout or "").strip()
                raise RuntimeError(self._format_codex_error(stderr or "Codex CLI 执行失败"))
            content = output_path.read_text(encoding="utf-8", errors="ignore").strip()
            content = re.sub(r'<think>[\s\S]*?</think>', '', content).strip()
            if not content:
                raise RuntimeError("Codex CLI 未返回有效内容，请检查额度、登录状态或改用 LLM_API_KEY。")
            return content
        finally:
            try:
                output_path.unlink(missing_ok=True)
            except Exception:
                pass

    def _format_codex_error(self, raw_text: str) -> str:
        text = str(raw_text or "").strip()
        lowered = text.lower()
        if "usage limit" in lowered or "upgrade to pro" in lowered or "purchase more credits" in lowered:
            return "本地 Codex CLI 当前额度不足，无法继续生成智能推演结果。请在 Codex 中补充额度，或在 MiroFish/.env 配置可用的 LLM_API_KEY。"
        if "failed to open state db" in lowered or "failed to initialize state runtime" in lowered:
            return "本地 Codex CLI 状态库异常，请重启 Codex 或清理 ~/.codex 状态缓存后重试。"
        if not text:
            return "Codex CLI 执行失败"
        lines = [line.strip() for line in text.splitlines() if line.strip()]
        if len(lines) > 8:
            lines = lines[-8:]
        return "\n".join(lines)
