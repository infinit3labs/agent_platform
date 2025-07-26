from __future__ import annotations

import yaml
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List

from transformers import pipeline


def load_config(path: str | Path) -> Dict:
    """Load YAML configuration."""
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


@dataclass
class Agent:
    name: str
    model: str
    prompt_prefix: str

    def __post_init__(self):
        self.generator = pipeline("text-generation", model=self.model)

    def run(self, input_text: str) -> str:
        prompt = f"{self.prompt_prefix} {input_text}"
        result = self.generator(prompt, max_new_tokens=50, num_return_sequences=1)
        return result[0]["generated_text"].strip()


def build_agents(config: Dict) -> Dict[str, Agent]:
    return {a["name"]: Agent(**a) for a in config.get("agents", [])}


def run_workflow(config: Dict, agents: Dict[str, Agent], seed_text: str) -> List[str]:
    """Run the workflow defined in the config starting with seed_text."""
    outputs = []
    text = seed_text
    for step in config.get("workflow", []):
        agent = agents[step["agent"]]
        text = agent.run(text)
        outputs.append(text)
    return outputs
