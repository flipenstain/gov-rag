MATCH (t:Table {name: 'DimCustomer'})-[:IN_SCHEMA]->(s:Schema {name: 'wh_db'})
MATCH (c:Column{name: 'marketingnameplate'})-[:IN_TABLE]->(t)
OPTIONAL MATCH path = (c)-[d:DERIVED_FROM*]->(source)
RETURN DISTINCT path

