# AI — Embeddings, LLM, RAG, Extraction & Tool Calling

Single `AI` class wraps all AI capabilities. Provider details are internal — users never see them.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│  AI(api_key="...")                                               │
│                                                                  │
│  ai.generate("prompt")        → LLMResponse                     │
│  ai.stream("prompt")          → Generator[str]                   │
│  ai.ask(q, documents=ms)      → RAGResult (retrieve + generate) │
│  ai.extract(text, schema)     → ExtractionResult                 │
│  ai.run_tool_loop(msgs, ...)  → LLMResponse (with tool calls)   │
│  ai.search_tools(ms)          → tool declarations for LLM       │
│                                                                  │
│  ┌──────────┐  ┌──────────────┐  ┌─────────────────────────┐    │
│  │ Embeddings│  │ LLM Client   │  │ RAG Pipeline            │    │
│  │ (768-dim) │  │ (Gemini 3)   │  │ retrieve → augment →    │    │
│  │           │  │              │  │ generate with citations  │    │
│  └──────────┘  └──────────────┘  └─────────────────────────┘    │
└─────────────────────────┬───────────────────────────────────────┘
                          │
┌─────────────────────────▼───────────────────────────────────────┐
│  MediaStore(ai=ai)                                               │
│                                                                  │
│  ms.upload(file)          → auto-chunk + embed                   │
│  ms.search("keywords")   → full-text (tsvector)                 │
│  ms.semantic_search(q)    → vector (pgvector cosine)             │
│  ms.hybrid_search(q)     → RRF fusion (best of both)            │
└─────────────────────────────────────────────────────────────────┘
```

| Component | Technology | Purpose |
|-----------|-----------|---------|
| **Embeddings** | Gemini `gemini-embedding-001` (768-dim) | Document + query vectors |
| **LLM** | Gemini `gemini-3-flash-preview` | Generation, tool calling, extraction |
| **Vector search** | pgvector HNSW (cosine) | Semantic similarity |
| **Text search** | PG tsvector + GIN | Keyword matching |
| **Hybrid search** | RRF (k=60) | Fuses text + vector rankings |
| **Chunking** | Sentence-aware, 512-token, 50-token overlap | Split docs for embedding |

---

## Quick Start

```python
from ai import AI, Message
from media import MediaStore

ai = AI()                                           # reads GEMINI_API_KEY env var
ms = MediaStore(s3_endpoint="localhost:9002", ai=ai) # auto-embeds on upload
```

### Upload & Search

```python
doc = ms.upload("reports/q1.pdf", title="Q1 Report", tags=["finance"])

ms.search("interest rate swap")           # full-text (keywords)
ms.semantic_search("risk transfer")       # vector (meaning)
ms.hybrid_search("credit derivatives")    # RRF fusion (best)
```

### RAG — Document-Grounded Q&A

```python
result = ai.ask("What are credit default swaps?", documents=ms)
print(result.answer)    # Grounded answer with citations
print(result.sources)   # Retrieved chunks used as context
```

Options:
```python
result = ai.ask(
    "How does DV01 work?",
    documents=ms,
    search_mode="semantic",    # "hybrid" (default), "semantic", or "text"
    limit=5,                   # chunks to retrieve (default: 5)
    temperature=0.3,           # LLM temperature (default: 0.3)
    system_prompt="...",       # custom system prompt
)
```

### Structured Extraction

```python
result = ai.extract(
    text="Goldman Sachs Q3 2024: revenue $12.7B, EPS $8.40, ROE 10.4%.",
    schema={
        "type": "object",
        "properties": {
            "company": {"type": "string"},
            "revenue_billions": {"type": "number"},
            "eps": {"type": "number"},
            "roe_pct": {"type": "number"},
        },
        "required": ["company"],
    },
)
print(result.data)
# {"company": "Goldman Sachs", "revenue_billions": 12.7, "eps": 8.40, "roe_pct": 10.4}
```

With dataclass instantiation:
```python
from dataclasses import dataclass

@dataclass
class Earnings:
    company: str
    revenue_billions: float = 0.0
    eps: float = 0.0

result = ai.extract(text, schema=schema, model_class=Earnings)
print(result.data.company)  # "Goldman Sachs"
```

### Direct Generation

```python
# String shorthand (single user message)
response = ai.generate("Explain convexity in fixed income.")
print(response.content)

# Full conversation
response = ai.generate([
    Message(role="system", content="You are a quant analyst."),
    Message(role="user", content="Explain gamma hedging."),
])
```

### Streaming

```python
for chunk in ai.stream("Explain Black-Scholes assumptions"):
    print(chunk, end="", flush=True)
```

### Tool Calling

The LLM can search your documents autonomously:

```python
# Get tool declarations
tools = ai.search_tools(ms)

# LLM decides when to call tools, results fed back automatically
response = ai.run_tool_loop(
    "Find documents about Basel III and summarize the capital requirements.",
    tools=tools,
)
print(response.content)
```

Available search tools (auto-generated from MediaStore):
- `search_documents` — full-text keyword search
- `semantic_search` — vector similarity search
- `hybrid_search` — RRF fusion search
- `list_documents` — browse available documents

Custom tools:
```python
from ai import Tool

my_tool = Tool(
    name="get_price",
    description="Get the current price of a stock.",
    parameters={
        "type": "object",
        "properties": {"symbol": {"type": "string"}},
        "required": ["symbol"],
    },
    fn=lambda symbol: '{"price": 150.25}',
)

response = ai.run_tool_loop(
    "What is AAPL trading at?",
    tools=[{"name": my_tool.name, "description": my_tool.description, "parameters": my_tool.parameters}],
    execute_tool=lambda name, args: my_tool.fn(**args),
)
```

---

## Public API

7 symbols exported from `ai/`:

| Symbol | Kind | Description |
|--------|------|-------------|
| `AI` | class | Single entry point for all AI capabilities |
| `Message` | dataclass | Conversation message (role, content) |
| `LLMResponse` | dataclass | Response from generate/run_tool_loop |
| `ToolCall` | dataclass | Tool call in LLMResponse.tool_calls |
| `RAGResult` | dataclass | Response from ask() — answer + sources |
| `ExtractionResult` | dataclass | Response from extract() — structured data |
| `Tool` | dataclass | Custom tool definition (name, schema, function) |

### AI Constructor

```python
AI(
    api_key=None,          # Falls back to GEMINI_API_KEY env var
    provider="gemini",     # Currently only "gemini" supported
    embedding_dim=768,     # Embedding vector dimension
    model=None,            # LLM model (default: gemini-3-flash-preview)
)
```

### AI Methods

| Method | Signature | Returns | Description |
|--------|-----------|---------|-------------|
| `.generate()` | `(messages, tools=, temperature=, max_tokens=)` | `LLMResponse` | Generate text. `messages` can be a string or list[Message]. |
| `.stream()` | `(messages, tools=, temperature=, max_tokens=)` | `Generator[str]` | Stream response chunks. |
| `.ask()` | `(question, documents=, search_mode=, limit=, temperature=, system_prompt=)` | `RAGResult` | RAG: retrieve + generate with citations. |
| `.extract()` | `(text, schema, model_class=, system_prompt=, temperature=)` | `ExtractionResult` | Extract structured data from text. |
| `.run_tool_loop()` | `(messages, tools=, execute_tool=, max_iterations=)` | `LLMResponse` | Generate → call tools → respond loop. |
| `.search_tools()` | `(media_store)` | `list[dict]` | Get tool declarations for a MediaStore. |

### MediaStore with AI

```python
MediaStore(
    s3_endpoint="localhost:9002",
    ai=ai,              # pass AI instance — auto-embeds on upload
)
```

When `ai=` is provided, `upload()` automatically chunks text and generates embeddings. `semantic_search()` and `hybrid_search()` become available.

---

## Search Modes

| Mode | Method | How it works | Best for |
|------|--------|-------------|----------|
| **Full-text** | `ms.search(q)` | PG tsvector weighted ranking | Exact keyword matches |
| **Semantic** | `ms.semantic_search(q)` | pgvector cosine similarity on chunks | Meaning-based queries |
| **Hybrid** | `ms.hybrid_search(q)` | RRF fusion of text + semantic | General queries (default for RAG) |

### Search Weights (full-text)

| Weight | Source |
|--------|--------|
| **A** (highest) | Title |
| **B** | Filename + tags |
| **C** | Extracted text |

---

## Chunking & Embeddings

Documents are automatically chunked and embedded on upload when `ai=` is set.

| Parameter | Value |
|-----------|-------|
| Chunk size | ~512 tokens |
| Overlap | ~50 tokens |
| Splitting | Sentence-aware boundaries |
| Embedding model | `gemini-embedding-001` |
| Dimension | 768 |
| Task types | `RETRIEVAL_DOCUMENT` (index), `RETRIEVAL_QUERY` (search) |
| Index | HNSW (cosine distance) |

---

## Demo

```bash
export GEMINI_API_KEY="your-key"
pip install -e ".[ai,media]"
python3 demo_rag.py
```

Exercises all features: upload → 3 search modes → RAG Q&A → extraction → streaming → tool calling.

---

## Agents

### Basic Agent

```python
from ai import Agent, tool

@tool
def get_price(symbol: str) -> str:
    """Get the current price of a stock.

    Args:
        symbol: Ticker symbol (e.g. AAPL).
    """
    return '{"price": 150.25}'

agent = Agent(
    tools=[get_price],
    system_prompt="You are a trading assistant.",
)

result = agent.run("What is AAPL trading at?")
print(result.content)       # "AAPL is currently at $150.25"
print(result.steps)         # [AgentStep(action=ToolCall(...), observation="...")]
print(result.iterations)    # 2 (1 tool call + 1 final response)
```

The `@tool` decorator auto-generates JSON Schema from type hints and docstrings. No manual schema writing.

### Multi-turn Conversations

```python
agent = Agent(system_prompt="You are a math tutor.")
agent.run("What is 5 * 7?")       # "35"
agent.run("Divide that by 5?")    # "7" — remembers context
agent.reset()                      # clear history
```

### Persistent Memory

```python
from ai.memory import AgentMemory, bootstrap_conversations_table

# Bootstrap (admin, once)
bootstrap_conversations_table(admin_conn, grant_to="app_user")

# Use
memory = AgentMemory(store_conn=conn)
agent = Agent(tools=[...], memory=memory, name="trading_agent")

result = agent.run("Analyze AAPL risk")
# Conversation auto-saved to PG

# Later — resume
agent.load_conversation(agent.conversation_id)
agent.run("What about MSFT?")

# Browse history
convos = agent.list_conversations(limit=10)
```

Conversations are stored in PostgreSQL with auto-summarization for long threads (>20 messages).

### Platform Tools (auto-registration)

```python
from ai._tools import ToolRegistry

# Auto-register all built-in tools from platform services
registry = ToolRegistry.from_platform(
    media_store=ms,        # → search_documents, semantic_search, hybrid_search, list_documents
    lakehouse=lh,          # → query_lakehouse, list_tables
)
```

### Multi-Agent Teams

```python
from ai import Agent
from ai.team import AgentTeam

researcher = Agent(tools=[search_docs], system_prompt="You research documents.")
analyst = Agent(tools=[query_lh], system_prompt="You analyze data.")

team = AgentTeam(agents={"researcher": researcher, "analyst": analyst})
result = team.run("Research Basel III docs and analyze capital impact")

print(result.content)           # Synthesized answer
print(result.delegation_log)    # Which agent handled what
```

The router LLM decomposes tasks and delegates to specialists sequentially.

---

## Evaluation

### Eval Cases

```python
from ai.eval import EvalRunner, EvalCase

cases = [
    EvalCase(
        input="What is AAPL trading at?",
        expected_tools=["get_price"],
        expected_output_contains=["AAPL", "price"],
        tags=["tool_use", "price"],
    ),
    EvalCase(
        input="Search for Basel III documents",
        expected_tools=["hybrid_search"],
        tags=["search"],
    ),
]

runner = EvalRunner(agent=agent)
results = runner.run(cases)
runner.summary()
# ==================================================
#   Eval Results: 2/2 passed (100%)
# ==================================================
#   Avg latency:    1200ms
#   Total tokens:   450
#   Tool accuracy:  2/2 (100%)
# ==================================================
```

### A/B Model Comparison

```python
agent_a = Agent(model="gemini-3-flash-preview", tools=[...])
agent_b = Agent(model="gemini-2.0-flash", tools=[...])

runner_a = EvalRunner(agent=agent_a)
runner_b = EvalRunner(agent=agent_b)

results_a = runner_a.run(cases)
results_b = runner_b.run(cases)

EvalRunner.compare(results_a, results_b, "Flash 3", "Flash 2")
```

### Built-in Eval Datasets

```python
from ai.eval_datasets import tool_use_cases, qa_cases, sql_cases

cases = (
    tool_use_cases() +              # pre-built tool calling tests
    qa_cases(media_store) +         # auto-generated from uploaded docs
    sql_cases(lakehouse)            # auto-generated from lakehouse tables
)
```

### LLM-as-Judge

```python
runner = EvalRunner(agent=agent, judge=AI())

cases = [
    EvalCase(
        input="Explain credit default swaps",
        expected_output="A CDS is a financial derivative that transfers credit risk...",
    ),
]
results = runner.run(cases)
# Judge LLM evaluates if actual output matches expected semantically
```

---

## Public API

15 symbols exported from `ai/`:

| Symbol | Kind | Description |
|--------|------|-------------|
| `AI` | class | Single entry point for all AI capabilities |
| `Agent` | class | Tool-calling agent with conversation memory |
| `AgentResult` | dataclass | Agent run result — content + steps + usage |
| `AgentStep` | dataclass | One tool-call round: action + observation |
| `AgentTeam` | class | Multi-agent orchestration with LLM router |
| `Message` | dataclass | Conversation message (role, content) |
| `LLMResponse` | dataclass | Response from generate/run_tool_loop |
| `ToolCall` | dataclass | Tool call in LLMResponse.tool_calls |
| `RAGResult` | dataclass | Response from ask() — answer + sources |
| `ExtractionResult` | dataclass | Response from extract() — structured data |
| `Tool` | dataclass | Custom tool definition (name, schema, function) |
| `EvalRunner` | class | Run eval cases, summary, A/B compare |
| `EvalCase` | dataclass | Eval test case — input, expected output/tools |
| `EvalResult` | dataclass | Eval result — passed, latency, tools match |
| `tool` | decorator | Convert typed function to Tool |

---

## Test Coverage

| Test suite | Count | What |
|------------|-------|------|
| `test_ai_client.py` | 12 | Clean API: generate, stream, ask, extract, tools, import hygiene |
| `test_embeddings.py` | 8 | Gemini embeddings: batch, query, similarity |
| `test_llm.py` | 12 | LLM: generate, stream, tool calling, run_tool_loop |
| `test_extraction.py` | 5 | Structured extraction: basic, financial, dataclass, list, nulls |
| `test_rag.py` | 5 | RAG pipeline: ask, sources, semantic mode |
| `test_tools.py` | 7 | Tool registry, search tools, LLM+tool integration |
| `test_embed_upload.py` | 12 | Upload+embed, semantic search, hybrid search |
| `test_agents.py` | 44 | @tool decorator, Agent, memory, team, eval, import hygiene |
| **Total** | **105** | All real API calls, real PG, real S3 |
