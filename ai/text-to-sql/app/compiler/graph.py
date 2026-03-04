from langgraph.graph import StateGraph, END
from .state import RunnerState
from .nodes import sanitize_sql_query, generate_sql_query_by_LLM, execute_sql_query, repair_sql_query

MAX_RETRIES = 2

def build_graph():
    graph = StateGraph(RunnerState)
    graph.add_node("generate", generate_sql_query_by_LLM)
    graph.add_node("sanitize", sanitize_sql_query)
    graph.add_node("execute", execute_sql_query)
    graph.add_node("repair", repair_sql_query)

    graph.set_entry_point("generate")
    graph.add_edge("generate", "sanitize")
    graph.add_edge("sanitize", "execute")
    
    graph.add_conditional_edges(
        "execute",
        lambda s: "repair" if s.get("error") and s["retry_count"] < MAX_RETRIES else END,
    )

    graph.add_edge("repair", "generate")

    return graph.compile()