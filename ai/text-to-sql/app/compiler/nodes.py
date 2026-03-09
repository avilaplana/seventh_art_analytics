from .state import RunnerState
from . import capabilities

def generate_llm_response_node(state: RunnerState) -> dict:
    llm_response = capabilities.generate_sql_query_by_LLM(state["user_query"], state["prompt_config"])
    return {"llm_response": llm_response}

def load_prompt_configuration_node(state: RunnerState) -> dict:
    prompt_config = capabilities.load_prompt_configuration(state["model"], state["version"])
    return {"prompt_config": prompt_config}

def execute_sql_query_node(state: RunnerState) -> dict:
    try:
        db_result = capabilities.execute_sql_query(state["sql_sanitised"])
        return {"db_result": db_result}
    except Exception as e:
        return {"error": str(e)}

def repair_sql_query_node(state: RunnerState) -> dict:
    history = state["history"] + [{"sql": state["sql"], "error": state["error"]}]
    sql = capabilities.retry_sql(state["user_query"], history)
    return {
        "sql": sql,
        "history": history,
        "retry_count": state["retry_count"] + 1,
        "error": None,
    }

def sanitize_sql_query_node(state: RunnerState) -> dict:
    """
    Strip ```sql``` blocks, leading/trailing whitespace, etc.
    Add 'LIMIT 10' if no LIMIT is present in the query.
    """
    sql = state["llm_response"]["message"]["content"].strip()
    
    # Remove ```sql ... ``` fences
    if sql.startswith("```sql"):
        sql = "\n".join(sql.splitlines()[1:-1])
    
    sql = sql.strip()
    
    # Check if LIMIT exists anywhere (case-insensitive)
    if "LIMIT" not in sql.upper():
        # Preserve existing semicolon if present
        ends_with_semicolon = sql.strip().endswith(";")
        sql = sql.rstrip("; \t\n") + " LIMIT 10"
        if ends_with_semicolon:
            sql += ";"
    
    return {"sql_sanitised": sql}