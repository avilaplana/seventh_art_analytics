import json
from ollama import chat
from rich import print

# -----------------------------
# Load schemas
# -----------------------------
with open("physical_schema.json") as f:
    physical_schema = json.load(f)

with open("semantic_layer.json") as f:
    semantic_layer = json.load(f)

# -----------------------------
# Build prompt for LLM
# -----------------------------
def build_prompt(physical, semantic, user_query):
    return f"""
You are a SQL expert. Generate SQL based ONLY on the following database schema.
Do not hallucinate tables or columns. Use the semantic layer to interpret human-friendly terms.

Physical schema:
{json.dumps(physical, indent=2)}

Semantic layer:
{json.dumps(semantic, indent=2)}

User request:
{user_query}

Instructions:
- Only use tables and columns from the physical schema.
- Resolve enums, roles, languages, and canonical patterns using the semantic layer.
- Infer necessary joins automatically.
- Output valid SQL only.
- Do not include explanations or extra text.
- Enclose the SQL in triple backticks ```sql ... ```.

SQL:
"""

# -----------------------------
# Generate SQL from NL
# -----------------------------
def generate_sql(user_query):
    prompt = build_prompt(physical_schema, semantic_layer, user_query)
    response = chat(
        model="llama3:latest",
        messages=[{"role": "user", "content": prompt}]
    )
    return response.message.content  # This will be the SQL code

# -----------------------------
# CLI loop
# -----------------------------
if __name__ == "__main__":
    print("[bold cyan]NL → SQL Playground[/bold cyan]")
    while True:
        user_query = input("\nEnter your natural language query (or 'exit'): ").strip()
        if user_query.lower() in ["exit", "quit"]:
            break
        try:
            sql = generate_sql(user_query)
            print("\n[bold green]Generated SQL:[/bold green]\n")
            print(sql)
        except Exception as e:
            print(f"[bold red]Error:[/bold red] {e}")