from memgraph_agent import MemgraphQueryToolSpec
from llama_index.llms.openai import OpenAI
from llama_index.agent.openai import OpenAIAgent
import os
from llama_index.core.indices.property_graph import SchemaLLMPathExtractor
from llama_index.embeddings.openai import OpenAIEmbedding
from llama_index.core import PropertyGraphIndex

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
llm = OpenAI(model="gpt-4.1-mini", api_key=OPENAI_API_KEY, temperature=0)

from llama_index.graph_stores.memgraph import MemgraphPropertyGraphStore
from collections import namedtuple

from llama_index.core.settings import Settings
Settings.llm = OpenAI(model="gpt-4.1-mini", api_key=OPENAI_API_KEY, temperature=0)

Schema = namedtuple("Schema", ["left_node", "relation", "right_node"])

def extract_lineage_schema(graph_store: MemgraphPropertyGraphStore):
    rels = graph_store.structured_schema.get("relationships", [])
    return [Schema(r["start"], r["type"], r["end"]) for r in rels]

# Attach `.schema` dynamically (if you're not subclassing)
MemgraphPropertyGraphStore.schema = property(lambda self: extract_lineage_schema(self))



gds_db = MemgraphQueryToolSpec(
    url="bolt://localhost:7687",
    user="",
    password="",
    llm=llm,


)



tools = gds_db.to_tool_list()
#agent = OpenAIAgent.from_tools(tools, llm=llm, verbose=True)

question = "Describe me full column level lineage for wh_db.Prospect.marketingnameplate"
#answer = agent.chat(question)

#print(gds_db.graph_store.structured_schema)

#print(gds_db.get_system_message())

answer = "asd"
prompt = f"""You are a helpful BI analyst assistant working with a Cypher query system.

The user asked:
\"\"\"{question}\"\"\"

The system returned this structured result:
```json
{answer}

Explain the answer in concise way, providing only factual information without long explenation
"""

#final_response = llm.complete(prompt)
#print(final_response)

index = PropertyGraphIndex.from_existing(
    property_graph_store=gds_db.graph_store,
    embed_model=OpenAIEmbedding(model_name="text-embedding-ada-002", api_key=OPENAI_API_KEY),
    kg_extractors=[
        SchemaLLMPathExtractor(
            llm=OpenAI(model="gpt-4.1-mini", api_key=OPENAI_API_KEY, temperature=0)
        )
    ],
    show_progress=True,
)

query_engine = index.as_query_engine(verbose=True)

q = "Marketingnameplate"

# smoke test
response = query_engine.query(
    q
)
print(response)

graph_store = index.property_graph_store
nodes = graph_store.get_llama_nodes()
#relationships = graph_store.get_all_relationships()

print("Nodes:")
for node in nodes:
    print(node)


