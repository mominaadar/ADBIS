from collections import defaultdict

# Step 1: Read RDF triples from the input dataset
# triples = [
#     ("wsdbm:City0", "gn:parentCountry", "wsdbm:Country20"),
#     ("wsdbm:City1", "gn:parentCountry", "wsdbm:Country0"),
#     ("wsdbm:City2", "gn:parentCountry", "wsdbm:Country1"),
#     # more triples...
# ]
def prep_data():
    triples = []
    with open("100k.txt",'r') as file:
        for line in file.readlines():
            line = line.strip()
            line = line.strip('.')
            line = line.strip(' ')
            sub, pred, obj = line.split(maxsplit=2)
            pred = pred.strip('.')
            triples.append((sub,pred,obj))
    # Step 2: Create a dictionary to store distinct properties
    properties = set()
    for triple in triples:
        properties.add(triple[1])
    
    #print(properties)
    
    # Step 3-5: Vertically partition triples into tables
    tables = defaultdict(list)
    for triple in triples:
        subject, predicate, obj = triple
        tables[predicate].append((subject, obj))
    
    # Step 6: Build a dictionary of string-to-integer mappings
    string_to_int = {}
    next_int_id = 0
    for table in tables.values():
        for subject, obj in table:
            if subject not in string_to_int:
                string_to_int[subject] = next_int_id
                next_int_id += 1
            if obj not in string_to_int:
                string_to_int[obj] = next_int_id
                next_int_id += 1
    
    
    # Replace string values in the tables with integer IDs
    for table in tables.values():
        for i in range(len(table)):
            subject, obj = table[i]
            table[i] = (string_to_int[subject], string_to_int[obj])
    #print("-------------",string_to_int)
    return tables, string_to_int
    #print(string_to_int)