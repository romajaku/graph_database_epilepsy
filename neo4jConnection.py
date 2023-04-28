from neo4j import GraphDatabase


class Neo4jConnection:

    def __init__(self, uri, user, pwd):
        self.__uri = uri
        self.__user = user
        self.__pwd = pwd
        self.__driver = None
        self.connect_db()

    def connect_db(self):
        try:
            self.__driver = GraphDatabase.driver(self.__uri, auth=(self.__user, self.__pwd))
        except Exception as e:
            print("Unable to create driver:", e)

    def close(self):
        if self.__driver is not None:
            self.__driver.close()

    def run_query(self, query):
        result = None
        if self.__driver is not None:
            with self.__driver.session() as session:
                result = session.run(query)
        else:
            print("Driver not initialized")
        return result

    def run_query_list(self, queries):
        for query in queries:
            if query:
                self.run_query(query)
