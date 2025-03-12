docker cp C:\lopu-kg-test\project\src\answer_from_LLM.json cb9422a4aefd:/usr/lib/memgraph/query_modules/answer_from_LLM.json

docker ps -aqf "name=memgraph-mage"

CALL import_util.json("/usr/lib/memgraph/query_modules/answer_from_LLM.json");


CALL json_util.load_from_path("/usr/lib/memgraph/query_modules/answer_from_LLM.json")
YIELD objects
UNWIND objects AS o

CREATE (:NodeType {name: o.ColumnName}),
       (:NodeType {name: o.TableName}),
       (:NodeType {name: o.DataType}),
       (:NodeType {name: o.SchemaName}),
       (:NodeType {name: o.Comment}),
       (:NodeType {name: o.Summary});

CREATE (:Relationship {from: o.ColumnName, type: o.PART_OF, to: o.TableName}),
       (:Relationship {from: o.ColumnName, type: o.TYPE_OF, to: o.DataType}),
       (:Relationship {from: o.TableName, type: o.PART_OF, to: o.SchemaName}),
       (:Relationship {from: o.Comment, type: o.DESCRIBES, to: o.ColumnName}),
       (:Relationship {from: o.Summary, type: o.DESCRIBES, to: o.TableName});


CALL json_util.load_from_path("path/to/data.json")
YIELD objects
UNWIND objects AS o
CREATE (:Person {first_name: o.first_name, last_name: o.last_name, pets: o.pets});


# clean up

MATCH (n)
DETACH DELETE n;

# find nodes
MATCH (node) RETURN node;

# find connections 
MATCH (node1)-[connection]-(node2) RETURN node1, connection, node2;

# FIX DATA TYPE

MATCH (n:`Data Type`)
SET n:`DataType`
REMOVE n:`Data Type`;

# FIX properties and node name


MATCH (n:`DataType`)
                OPTIONAL MATCH (n)-[r]-(connected)
                RETURN n, r, connected;

# find columns
MATCH (n:`ColumnName`)
                OPTIONAL MATCH (n)-[r]-(connected)
                RETURN n, r, connected

https://memgraph.com/docs/querying/read-and-modify-data

# TODO
1. Data pipelineid lõpuni 
2. Metadata steppide kohta salvesta kuskile
    Võta ka sample datasetid tabelite / protsesside kohta
    Data Lineage?
3. DQ ruleid
4. Mõtle küsimused / vastused
5. Vaata Data Governance enforcingut - quality by design - codereview with LLM
6. RAG osa ja vector embeddings - kuidas saada kontext kätte? 



Kolmapäev 13:15

Kas selle andmestiku jaoks on tehtud tüüp küsimusi?
    Tsenaariumi järgi klassifitseerida, lihtsamad ja raskemad.

Tasakaal erialakirjanduse ja teaduskirjandus.

Nõudmised:
    Kui tundlik on muutustele?
    Kui kiire peaks olema vastus

Funktsionaalsed ja mittefunktsionaalsed nõuded?

Design Science - metodoloogia mõttes - **sellest peab sügavamalt aru saama**
    funktsionaalsete - mittefunktsionaalsete nõuete valideerimine. - Valideeritavana kirja pandud.

Struktuur paika:
    Uurimusküsimused - eesmärgid.
        2-3 aga võivad olla ka alamküsimused.

ANTS:
    Üldine meetod: Design science
    Meetod: funktsionaalsete ja mittefunktsionaalsete nõuete valideerimine
    Uurimisküsimused: 2-3, aga võivad olla alamküsimused
    Research gaps

git remote set-url origin https://flipenstain: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@github.com/flipenstain/gov-rag.git




// Querying all relationships in Memgraph

// 1. Get all relationship types
MATCH ()-[r]->()
RETURN DISTINCT TYPE(r) AS RelationshipType;

// 2. Get all relationships with details
MATCH (n1)-[r]->(n2)
RETURN 
    ID(n1) AS StartNodeId, 
    labels(n1) AS StartNodeLabels,
    TYPE(r) AS RelationshipType,
    ID(r) AS RelationshipId,
    properties(r) AS RelationshipProperties,
    ID(n2) AS EndNodeId,
    labels(n2) AS EndNodeLabels;

// 3. Get relationships with specific node labels
MATCH (n1:Person)-[r:ACTED_IN]->(n2:Movie)
RETURN 
    ID(n1) AS StartNodeId, 
    labels(n1) AS StartNodeLabels,
    TYPE(r) AS RelationshipType,
    ID(r) AS RelationshipId,
    properties(r) AS RelationshipProperties,
    ID(n2) AS EndNodeId,
    labels(n2) AS EndNodeLabels;

// 4. Get relationships with specific relationship type
MATCH ()-[r:ACTED_IN]->()
RETURN 
    ID(startNode(r)) AS StartNodeId,
    labels(startNode(r)) AS StartNodeLabels,
    ID(endNode(r)) AS EndNodeId,
    labels(endNode(r)) AS EndNodeLabels,
    properties(r) as RelationshipProperties;

// 5. Get relationships and node properties
MATCH (n1)-[r]->(n2)
RETURN 
    n1, // Start node with all properties
    r, // Relationship with all properties
    n2; // End node with all properties

// 6. Get relationships and specific node properties
MATCH (n1)-[r]->(n2)
RETURN 
    ID(n1), n1.name, // Start node ID and name property
    TYPE(r), properties(r), // Relationship type and properties
    ID(n2), n2.title; // End node ID and title property