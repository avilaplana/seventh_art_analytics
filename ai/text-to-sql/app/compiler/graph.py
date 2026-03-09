from langgraph.graph import StateGraph, END
from .state import RunnerState
from .nodes import (
    load_prompt_configuration_node,
    sanitize_sql_query_node,
    generate_llm_response_node,
    execute_sql_query_node,
    repair_sql_query_node,
)

MAX_RETRIES = 0 # To manage retries logic later on

def build_graph():
    graph = StateGraph(RunnerState)
    graph.add_node("load_prompt_configuration", load_prompt_configuration_node)
    graph.add_node("generate", generate_llm_response_node)
    graph.add_node("sanitize", sanitize_sql_query_node)
    graph.add_node("execute", execute_sql_query_node)
    graph.add_node("repair", repair_sql_query_node)

    graph.set_entry_point("load_prompt_configuration")
    graph.add_edge("load_prompt_configuration", "generate")
    graph.add_edge("generate", "sanitize")
    graph.add_edge("sanitize", "execute")
    
    graph.add_conditional_edges(
        "execute",
        lambda s: "repair" if s.get("error") and s["retry_count"] < MAX_RETRIES else END,
    )

    graph.add_edge("repair", "generate")

    return graph.compile()