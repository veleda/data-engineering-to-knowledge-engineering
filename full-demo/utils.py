def print_count(msg: str, m):

    count = """
        SELECT (COUNT(?s) AS ?count)
        WHERE { ?s ?p ?o . }
    """

    result = m.query(count)
    count_value = result["count"][0] if len(result) > 0 else 0
    print("Graph size after", msg, ": ", count_value)