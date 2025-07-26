import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

from agent_platform.orchestrator import load_config, build_agents, run_workflow


def test_run_workflow(tmp_path, monkeypatch):
    config_file = tmp_path / "config.yaml"
    config_file.write_text(
        """
agents:
  - name: coder
    model: distilgpt2
    prompt_prefix: 'Write Python code to'
workflow:
  - agent: coder
        """
    )
    config = load_config(config_file)

    class FakePipeline:
        def __call__(self, prompt, max_new_tokens=50, num_return_sequences=1):
            return [{"generated_text": prompt + " result"}]

    monkeypatch.setattr("agent_platform.orchestrator.pipeline", lambda *args, **kwargs: FakePipeline())

    agents = build_agents(config)
    outputs = run_workflow(config, agents, "do something")
    assert outputs == ["Write Python code to do something result"]
