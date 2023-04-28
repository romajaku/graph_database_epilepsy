import neo4jConnection as con
import main as org
import random
import timeit
import numpy as np
import matplotlib.pyplot as plt
import database_api


chain_query = "MATCH (p:Patient)-[:next*]->(e:Event) WHERE p.id = 1235 AND e.name=\"Target\" RETURN e"
star_query = "MATCH (p:Patient)-[:performed]->(e:Event) WHERE p.id = 1234 AND e.name=\"Target\" RETURN e"
skiplist_query = "MATCH (p:Patient)-[:nextTarget*]->(e:Event) WHERE p.id = 1235 RETURN e"


def initialize(db, event_num, event_perc):
    create_patients(db)
    events = create_events(event_num, event_perc)
    stars = create_star_queries(1234, events)
    chains = create_chain_skiplist_queries(1235, events)
    db.run_query_list(stars)
    db.run_query_list(chains)


def create_events(quantity, target_percentage):
    target_number = int(quantity * target_percentage)
    random_number = quantity - target_number
    events = ["3T", "7T", "Implant", "Preimplant MRI", "Surgical intervention", "Preoperative neuropsych"]
    random_events = []
    for i in range(random_number):
        random_events.append(random.choice(events))
    event_list = ["Target"] * target_number + random_events
    random.shuffle(event_list)
    return event_list


def create_star_queries(patient_id, event_list):
    queries = []
    for event in event_list:
        query = f"MATCH (p:Patient) WHERE p.id = {patient_id} "
        query += "CREATE (e:Event { name:\"" + event + "\"}), (p)-[:performed]->(e) "
        queries.append(query)
    return queries


def create_chain_skiplist_queries(patient_id, event_list):
    queries = []
    query = f"MATCH (p:Patient) WHERE p.id = {patient_id} "
    i = 1
    target_counter = 0
    for event in event_list:
        if i == 1:
            query += "CREATE (e" + str(i) + ":Event { name:\"" + event + "\"} ), "
            query += f"(p) -[:next]-> (e{i}), "
        else:
            query += "(e" + str(i) + ":Event { name:\"" + event + "\"} ), "
            query += f"(e{i - 1}) -[:next]-> (e{i}), "
        if event == "Target":
            if target_counter == 0:
                query += f"(p) -[:nextTarget]-> (e{i}), "
            else:
                query += f"(e{target_counter}) -[:nextTarget]-> (e{i}), "
            target_counter = i
        i += 1
    query += f"(p)-[:last]->(e{i - 1})"
    queries.append(query)
    return queries


def create_chain_queries(patient_id, event_list):
    queries = []
    query = f"MATCH (p:Patient) WHERE p.id = {patient_id} "
    i = 1
    for event in event_list:
        if i == 1:
            query += "CREATE (e" + str(i) + ":Event { name:\"" + event + "\"} ), "
            query += f"(p) -[:next]-> (e{i}), "
        else:
            query += "(e" + str(i) + ":Event { name:\"" + event + "\"} ), "
            query += f"(e{i - 1}) -[:next]-> (e{i}), "
        i += 1
    query += f"(p)-[:last]->(e{i - 1})"
    queries.append(query)
    return queries


def create_patients(db):
    db.run_query("MERGE (p:Patient {id: 1234})")
    db.run_query("MERGE (p:Patient {id: 1235})")


def measure_time(db):
    warm_cache(db)
    start = timeit.default_timer()
    db.run_query(star_query)
    end = timeit.default_timer()
    star_time = end - start
    start = timeit.default_timer()
    db.run_query(chain_query)
    end = timeit.default_timer()
    chain_time = end - start
    start = timeit.default_timer()
    db.run_query(skiplist_query)
    end = timeit.default_timer()
    skiplist_time = end - start
    return star_time, chain_time, skiplist_time


def measure_index_time(db):
    delete_indexes(db)
    warm_cache(db)
    start = timeit.default_timer()
    db.run_query(skiplist_query)
    end = timeit.default_timer()
    noindex_time = end - start
    org.create_indexes(db)
    start = timeit.default_timer()
    db.run_query(skiplist_query)
    end = timeit.default_timer()
    index_time = end - start
    return noindex_time, index_time


def clear_db(db):
    db.run_query("MATCH (n) DETACH DELETE n")


def warm_cache(db):
    db.run_query("MATCH (n) OPTIONAL MATCH (n) -[r]->() RETURN count(n.prop) + count(r.prop)")


def delete_indexes(connection):
    connection.run_query('DROP INDEX patient_name_index IF EXISTS ')
    connection.run_query('DROP INDEX event_name_index IF EXISTS ')
    connection.run_query('DROP INDEX study_name_index IF EXISTS ')
    connection.run_query('DROP INDEX seizure_name_index IF EXISTS ')
    connection.run_query('DROP INDEX later_name_index IF EXISTS ')
    connection.run_query('DROP INDEX medication_name_index IF EXISTS ')


def event_variable_test(db, event_num_array, event_perc):
    star_times_matrix = []
    chain_times_matrix = []
    skiplist_times_matrix = []
    for iteration in range(5):
        print(f"Run: {iteration+1} out of 5.")
        star_times = []
        chain_times = []
        skiplist_times = []
        for i in range(len(event_num_array)):
            print(f"Currently on search of {event_num_array[i]} events")
            initialize(db, event_num_array[i], event_perc)
            star_time, chain_time, skiplist_time = measure_time(db)
            star_times.append(star_time)
            chain_times.append(chain_time)
            skiplist_times.append(skiplist_time)
            clear_db(db)
        star_times_matrix.append(star_times)
        chain_times_matrix.append(chain_times)
        skiplist_times_matrix.append(skiplist_times)
    return np.median(star_times_matrix, axis=0), np.median(chain_times_matrix, axis=0), np.median(skiplist_times_matrix, axis=0)


def percentage_variable_test(db, event_num, event_perc_array):
    star_times_matrix = []
    chain_times_matrix = []
    skiplist_times_matrix = []
    for iteration in range(5):
        print(f"Run: {iteration + 1} out of 5.")
        star_times = []
        chain_times = []
        skiplist_times = []
        for i in range(len(event_perc_array)):
            initialize(db, event_num, event_perc_array[i])
            star_time, chain_time, skiplist_time = measure_time(db)
            star_times.append(star_time)
            chain_times.append(chain_time)
            skiplist_times.append(skiplist_time)
            clear_db(db)
        star_times_matrix.append(star_times)
        chain_times_matrix.append(chain_times)
        skiplist_times_matrix.append(skiplist_times)
    return np.median(star_times_matrix, axis=0), np.median(chain_times_matrix, axis=0), np.median(skiplist_times_matrix, axis=0)


def index_variable_test(db, event_num_array, event_perc):
    noindex_matrix = []
    index_matrix = []
    for iteration in range(5):
        print(f"Run: {iteration+1} out of 5.")
        noindex_times = []
        index_times = []
        for i in range(len(event_num_array)):
            print(f"Currently on search of {event_num_array[i]} events")
            initialize(db, event_num_array[i], event_perc)
            warm_cache(db)
            noindex_time, index_time = measure_index_time(db)
            noindex_times.append(noindex_time)
            index_times.append(index_time)
            clear_db(db)
        noindex_matrix.append(noindex_times)
        index_matrix.append(index_times)
    return np.median(noindex_matrix, axis=0), np.median(index_matrix, axis=0)


def print_graph(x, y_star, y_chain, y_skip, xlabel, title):
    plt.plot(x, y_star, label="Star model")
    plt.plot(x, y_chain, label="Chain model")
    plt.plot(x, y_skip, label="Skip list model")
    plt.xlabel(xlabel)
    plt.ylabel('Execution time [s]')
    plt.title(title)
    plt.legend()
    plt.show()


def make_example_patients(db):
    event_num = 15
    event_perc = 0.3
    db.run_query("MERGE (p:Patient {id: 1234, name:\"star\"})")
    db.run_query("MERGE (p:Patient {id: 1235, name: \"chain\"})")
    db.run_query("MERGE (p:Patient {id: 1236, name:\"skiplist\"})")
    events = create_events(event_num, event_perc)
    stars = create_star_queries(1234, events)
    chains = create_chain_queries(1235, events)
    skiplist = create_chain_skiplist_queries(1236, events)
    db.run_query_list(stars)
    db.run_query_list(chains)
    db.run_query_list(skiplist)


def main():
    uri, username, password = org.read_config('config.ini')
    db = con.Neo4jConnection(uri, username, password)
    org.create_constrains(db)
    org.create_indexes(db)
    make_example_patients(db)

    percentages = [0.001, 0.01, 0.05, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9]
    events_small = list(range(5, 201, 5))
    events_large = list(range(500, 10001, 500))
    initialize(db, 20, 0.2)

    noindex, index = index_variable_test(db, events_large, 0.2)
    plt.plot(events_large, noindex, label="Skiplist without indexes")
    plt.plot(events_large, index, label="Skiplist with indexes")
    plt.xlabel("Number of events")
    plt.ylabel('Execution time [s]')
    plt.title("Comparison of query execution speed with and without indexes on event name")
    plt.legend()
    plt.show()

    star, chain, skiplist = event_variable_test(db, events_small, 0.2)
    # print("Star: ", star)
    # print("Chain: ", chain)
    print_graph(events_small, star, chain, skiplist, "Number of events", "Variable number of events, constant target percentage")

    star, chain, skiplist = event_variable_test(db, events_large, 0.2)
    # print("Star: ", star)
    # print("Chain: ", chain)
    print_graph(events_large, star, chain, skiplist, "Number of events", "Variable number of events, constant target percentage")

    star, chain, skiplist = percentage_variable_test(db, 1000, percentages)
    # print("Star: ", star)
    # print("Chain: ", chain)
    print_graph(percentages, star, chain, skiplist, "Ratio of target events", "Constant number of events, variable target percentage")


if __name__ == '__main__':
    main()
