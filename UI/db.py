from neo4j import GraphDatabase

class Neo4jConnection:
    def __init__(self, uri, user, password):
        self._driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self._driver.close()

    def query(self, query, parameters=None):
        with self._driver.session() as session:
            return session.run(query, parameters)

    def verify_connectivity(self):
        try:
            self._driver.verify_connectivity()
            return "Connected to Neo4j"
        except Exception as e:
            return f"Connection failed: {e}"
