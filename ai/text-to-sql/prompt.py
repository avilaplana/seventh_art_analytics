import json
from ollama import chat
from rich import print

# Load schema
with open("schema_summary.json") as f:
    schema_json = json.load(f)

def build_prompt(schema, user_query):
    return f"""
You are a SQL expert. Generate SQL based ONLY on the following database schema.
Do not hallucinate tables or columns.

Schema JSON:
{json.dumps(schema, indent=2)}

User request:
{user_query}

Instructions for SQL generation:
- Only use the tables and columns in the provided schema JSON.
- Infer necessary joins automatically.
- Output valid SQL only.
- Do not include explanations or extra text.
- Enclose the SQL in triple backticks ```sql ... ```.

SQL:
"""

def generate_sql(user_query):
    prompt = build_prompt(schema_json, user_query)
    response = chat(
        model="llama2:latest",
        messages=[{"role": "user", "content": prompt}]
    )
    return response.message.content  # <-- just the SQL string

if __name__ == "__main__":
    print("[bold cyan]NL → SQL Playground[/bold cyan]")
    while True:
        user_query = input("\nEnter your natural language query (or 'exit'): ").strip()
        if user_query.lower() in ["exit", "quit"]:
            break
        sql = generate_sql(user_query)
        print("\n[bold green]Generated SQL:[/bold green]\n")
        print(sql)