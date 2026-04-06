"""POST /api/wizard/chat — AI-powered HIA methodology assistant."""

from __future__ import annotations

import json
import logging
import os
from pathlib import Path
from typing import Any

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/wizard", tags=["wizard"])

SYSTEM_PROMPT_PATH = Path(__file__).resolve().parent.parent / "wizard" / "system_prompt.txt"

STEP_LABELS = {
    1: "Study Area — defining geographic boundaries, pollutant, and time period",
    2: "Air Quality — specifying baseline and control concentrations",
    3: "Population — entering exposed population and age distribution",
    4: "Health Data — providing baseline incidence rates for health endpoints",
    5: "CRFs — selecting concentration-response functions from the epidemiological literature",
    6: "Run — reviewing inputs and running the HIA computation",
    7: "Valuation — assigning economic values to estimated health impacts",
}


# ── Request / response models ──────────────────────────────────────


class ChatMessage(BaseModel):
    role: str
    content: str


class WizardChatRequest(BaseModel):
    message: str
    conversationHistory: list[ChatMessage] = Field(default_factory=list)
    context: dict[str, Any] = Field(default_factory=dict)


class WizardChatResponse(BaseModel):
    response: str


# ── Helpers ─────────────────────────────────────────────────────────


def _load_system_prompt() -> str:
    """Read the system prompt from disk."""
    if SYSTEM_PROMPT_PATH.exists():
        return SYSTEM_PROMPT_PATH.read_text(encoding="utf-8").strip()
    logger.warning("System prompt file not found at %s, using fallback.", SYSTEM_PROMPT_PATH)
    return (
        "You are the HIA Wizard, an expert assistant for health impact assessment. "
        "Help users with methodology, data choices, and interpretation of results."
    )


def _build_context_message(context: dict[str, Any]) -> str:
    """Format the user's current wizard state as additional context."""
    parts = []

    step = context.get("currentStep")
    if step:
        label = STEP_LABELS.get(step, f"Step {step}")
        parts.append(f"The user is currently on Step {step}: {label}.")

    config = context.get("analysisConfig")
    if config and isinstance(config, dict):
        # Extract key selections without dumping the entire config
        summaries = []

        step1 = config.get("step1", {})
        if step1.get("pollutant"):
            summaries.append(f"Pollutant: {step1['pollutant']}")
        if step1.get("studyArea", {}).get("name"):
            summaries.append(f"Study area: {step1['studyArea']['name']}")

        step2 = config.get("step2", {})
        baseline_val = step2.get("baseline", {}).get("value")
        control_val = step2.get("control", {}).get("value")
        if baseline_val is not None:
            summaries.append(f"Baseline concentration: {baseline_val}")
        if control_val is not None:
            summaries.append(f"Control concentration: {control_val}")

        step3 = config.get("step3", {})
        if step3.get("totalPopulation"):
            summaries.append(f"Population: {step3['totalPopulation']:,}")

        step5 = config.get("step5", {})
        n_crfs = len(step5.get("selectedCRFs", []))
        if n_crfs:
            summaries.append(f"Selected CRFs: {n_crfs}")

        step6 = config.get("step6", {})
        if step6.get("monteCarloIterations"):
            summaries.append(f"Monte Carlo iterations: {step6['monteCarloIterations']}")

        if summaries:
            parts.append("Current analysis configuration:\n- " + "\n- ".join(summaries))

    return "\n\n".join(parts) if parts else ""


# ── Endpoint ────────────────────────────────────────────────────────


@router.post("/chat", response_model=WizardChatResponse)
async def wizard_chat(req: WizardChatRequest) -> WizardChatResponse:
    """Send a message to the HIA Wizard and receive a response.

    Calls the Anthropic API with the system prompt, conversation
    history, and contextual information about the user's current
    wizard state.
    """
    api_key = os.getenv("ANTHROPIC_API_KEY")
    if not api_key:
        return WizardChatResponse(
            response=(
                "The HIA Wizard is not configured yet. To enable it, set the "
                "`ANTHROPIC_API_KEY` environment variable with your Anthropic API key.\n\n"
                "You can get one at https://console.anthropic.com/settings/keys"
            )
        )

    try:
        import anthropic
    except ImportError:
        raise HTTPException(
            status_code=500,
            detail="The anthropic Python package is not installed. Run: pip install anthropic",
        )

    # Build system prompt with context
    system_prompt = _load_system_prompt()
    context_text = _build_context_message(req.context)
    if context_text:
        system_prompt = f"{system_prompt}\n\n---\n\nCurrent user context:\n{context_text}"

    # Build message history for the API
    api_messages = []
    for msg in req.conversationHistory:
        if msg.role in ("user", "assistant"):
            api_messages.append({"role": msg.role, "content": msg.content})

    # Ensure the latest user message is included
    if not api_messages or api_messages[-1].get("content") != req.message:
        api_messages.append({"role": "user", "content": req.message})

    # Ensure messages alternate properly (Anthropic API requirement)
    cleaned = []
    for msg in api_messages:
        if cleaned and cleaned[-1]["role"] == msg["role"]:
            # Merge consecutive same-role messages
            cleaned[-1]["content"] += "\n\n" + msg["content"]
        else:
            cleaned.append(msg)

    # Ensure first message is from user
    if cleaned and cleaned[0]["role"] != "user":
        cleaned = cleaned[1:]

    if not cleaned:
        cleaned = [{"role": "user", "content": req.message}]

    try:
        client = anthropic.Anthropic(api_key=api_key)
        response = client.messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=1000,
            system=system_prompt,
            messages=cleaned,
        )

        reply = ""
        for block in response.content:
            if hasattr(block, "text"):
                reply += block.text

        return WizardChatResponse(response=reply or "I'm sorry, I wasn't able to generate a response.")

    except anthropic.AuthenticationError:
        return WizardChatResponse(
            response=(
                "The API key appears to be invalid. Please check your "
                "`ANTHROPIC_API_KEY` environment variable."
            )
        )
    except anthropic.RateLimitError:
        return WizardChatResponse(
            response="The AI service is temporarily rate-limited. Please try again in a moment."
        )
    except Exception as e:
        logger.exception("Wizard chat error")
        return WizardChatResponse(
            response=f"An error occurred while contacting the AI service: {str(e)}"
        )
