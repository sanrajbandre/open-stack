"""
Recommendation Engine
=====================

This module wraps the OpenAI ChatCompletion API to generate tuning
recommendations based on job analysis results.  It prompts the model
with a summary of the job's average CPU and memory usage, its SLA
category and utilisation status, then asks for a sensible Spark
configuration (executors, cores and memory per executor) and any other
observations.

If the OpenAI API key is not provided or the ``openai`` package is not
installed, the engine falls back to a rule‑based heuristic which uses
the computed averages to suggest conservative settings.  This makes the
system usable even offline.

To use the ChatCompletion API you must set ``openai_api_key`` on the
configuration object.  The API is called via ``openai.ChatCompletion.create``.
Due to environmental constraints in this sandbox, external network
requests will not succeed when executed here.  The code is provided
for completeness and should be run in a network‑enabled environment.
"""

from __future__ import annotations

import logging
from typing import Optional, Tuple

from .config import Config
from .analyzer import AnalysisResult

LOGGER = logging.getLogger(__name__)


class RecommendationEngine:
    """Generate resource tuning recommendations using OpenAI or heuristics."""

    def __init__(self, config: Config) -> None:
        self.config = config

    def recommend(self, analysis: AnalysisResult) -> Tuple[int, float, int, str]:
        """Return recommended executors, memory (MB) per executor, cores per executor and notes.

        :param analysis: AnalysisResult containing average CPU and memory usage and status.
        :returns: Tuple (executors, memory_mb, cores, notes).
        """
        # If no OpenAI key, use fallback heuristic
        if not self.config.openai_api_key:
            LOGGER.warning("OPENAI_API_KEY not provided; using heuristic recommendations")
            return self._heuristic_recommendation(analysis)
        # Try to call OpenAI API; handle import and runtime errors gracefully
        try:
            import openai  # type: ignore
        except ImportError:
            LOGGER.warning("openai package not installed; using heuristic recommendations")
            return self._heuristic_recommendation(analysis)
        openai.api_key = self.config.openai_api_key
        prompt = self._build_prompt(analysis)
        try:
            response = openai.ChatCompletion.create(
                model="gpt-4",
                messages=prompt,
                temperature=0.2,
                max_tokens=200,
            )
            message = response["choices"][0]["message"]["content"]  # type: ignore
            return self._parse_recommendation(message, analysis)
        except Exception as exc:  # broad catch, network or API error
            LOGGER.error("Failed to call OpenAI API: %s", exc)
            return self._heuristic_recommendation(analysis)

    def _build_prompt(self, analysis: AnalysisResult) -> list:
        """Construct the chat messages for the OpenAI API.

        The prompt frames the conversation for the assistant to return a
        concise, structured recommendation.
        """
        user_content = (
            f"We have a Spark job with ID {analysis.app_id}. "
            f"It is categorised as {analysis.category} based on its SLA. "
            f"The average CPU usage is {analysis.avg_cpu:.2f} vcores and "
            f"average memory usage is {analysis.avg_memory:.2f} MB if available. "
            f"The utilisation status is {analysis.status}. "
            "Based on these metrics, suggest the number of executors, cores per executor, "
            "and memory per executor (in MB) that would be appropriate. Keep the response "
            "succinct and in the format: 'executors=<num>, cores=<num>, memory=<num>MB' "
            "followed by a brief note explaining the reasoning."
        )
        return [
            {"role": "system", "content": "You are an expert Spark performance engineer."},
            {"role": "user", "content": user_content},
        ]

    def _parse_recommendation(
        self, message: str, analysis: AnalysisResult
    ) -> Tuple[int, float, int, str]:
        """Parse the model's reply to extract numbers.

        This parser is lenient: it looks for integers and floats in the
        assistant's message and assigns them to executors, cores and memory
        respectively.  If the model returns incomplete data, fall back to
        heuristics.
        """
        import re

        numbers = list(map(float, re.findall(r"\d+\.?\d*", message)))
        if len(numbers) >= 3:
            executors, cores, memory = int(numbers[0]), int(numbers[1]), numbers[2]
            note = message
            return executors, memory, cores, note
        # Fallback if parsing fails
        LOGGER.warning("Could not parse model response: %s", message)
        return self._heuristic_recommendation(analysis)

    def _heuristic_recommendation(self, analysis: AnalysisResult) -> Tuple[int, float, int, str]:
        """Provide simple heuristics for resource recommendations.

        The heuristics are intentionally conservative: allocate one executor per
        CPU core observed (rounded up), memory equal to average usage plus 50%,
        and one core per executor.  If metrics are missing, default to 2
        executors with 1024 MB and 1 core.
        """
        if analysis.avg_cpu and analysis.avg_memory:
            executors = max(1, int(round(analysis.avg_cpu)))
            cores = 1  # one core per executor
            memory = max(512, analysis.avg_memory * 1.5)  # safety margin
            note = "Heuristic recommendation based on average usage"
            return executors, memory, cores, note
        # default
        return 2, 1024.0, 1, "Default recommendation due to missing metrics"
