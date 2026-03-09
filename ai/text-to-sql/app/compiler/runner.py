from .graph import build_graph
from .state import RunnerState


class SQLAgent:
    def __init__(self):
        self.graph = build_graph()

    def run(self, question: str, model: str, version: int) -> dict:
        state: RunnerState = {
            "user_query": question,
            "model": model,
            "version": version,
            "prompt_config": None,
            "llm_response": None,
            "sql_sanitised": None,
            "db_result": None,
            "error": None,
            "history": [],
            "retry_count": 0,
        }

        final_state = self.graph.invoke(state)

        metrics = self._extract_llm_metrics(final_state.get("llm_response") or {})

        if final_state.get("error"):
            return {
            "sql": final_state["sql_sanitised"],
            "error": final_state["error"],
            "metrics": metrics,
        }
        
        return {
            "sql": final_state["sql_sanitised"],
            "result": final_state["db_result"],
            "metrics": metrics,
        }

    def _extract_llm_metrics(self, response: dict) -> dict:

        if not response:
            return {}

        model = response.get("model")

        prompt_tokens = response.get("prompt_eval_count", 0)
        generated_tokens = response.get("eval_count", 0)

        total_time = response.get("total_duration", 0) / 1e9
        load_time = response.get("load_duration", 0) / 1e9
        prompt_time = response.get("prompt_eval_duration", 0) / 1e9
        generation_time = response.get("eval_duration", 0) / 1e9

        tokens_per_second = (
            generated_tokens / generation_time if generation_time > 0 else 0
        )

        return {
            "model": model,
            "prompt_tokens": prompt_tokens,
            "generated_tokens": generated_tokens,
            "tokens_per_second": round(tokens_per_second, 2),
            "total_time_sec": round(total_time, 2),
            "load_time_sec": round(load_time, 2),
            "prompt_processing_time_sec": round(prompt_time, 2),
            "generation_time_sec": round(generation_time, 2),
        }