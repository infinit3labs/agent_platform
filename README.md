# Agent Platform

This repository provides a minimal configuration-driven multi-agent orchestrator using open-weight models from the Hugging Face ecosystem. Agents are defined in `config.yaml` and executed sequentially.

## Setup

```bash
poetry install
```

## Running the Demo

```bash
poetry run python demo.py
```

## Testing

```bash
poetry run pytest
```

## Best Practices for Multi-Agent Orchestration

- **Configuration Driven Design**: Keep all agent behaviors and models configurable via YAML or similar formats to simplify experimentation.
- **Open Weight Models**: Prefer models that offer open weights (e.g., via Hugging Face) to ensure transparency and reproducibility.
- **Dependency Management**: Use a tool like Poetry to lock dependencies and create reproducible environments.
- **Testing**: Mock heavy model loading in tests to keep test runs fast.
- **Documentation and Examples**: Provide demos and clear instructions on running the workflow to lower the barrier for contributors.
