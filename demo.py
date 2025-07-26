from agent_platform.orchestrator import load_config, build_agents, run_workflow


def main():
    config = load_config("config.yaml")
    agents = build_agents(config)
    outputs = run_workflow(config, agents, "create a Fibonacci sequence function")
    print("Workflow outputs:")
    for i, out in enumerate(outputs, 1):
        print(f"Step {i}:\n{out}\n")


if __name__ == "__main__":
    main()
