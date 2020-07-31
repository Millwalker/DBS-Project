from typing import List, Union

test_database = "test_db"
sql_teststring: str = "SELECT firstname\n" \
                      "FROM Test"


########## <relational algebra operations>
def project(input_dict: dict, requested_columns: List[str]) -> dict:
    for column in requested_columns:
        try:
            assert column in input_dict
        except AssertionError:
            raise KeyError(f"column '{column}' doesn't exist in table: \n {input_dict}")
    return {key: input_dict[key] for key in requested_columns}


def select(input_dict: dict, boolean_expression: str) -> dict:
    # boolean expression as string? -> safe eval?
    raise NotImplementedError


def load(db_path: str, table_name: str) -> dict:
    # find column names in index file:
    index_file = open(f'{db_path}/index.txt', 'r')
    lines = index_file.readlines()
    index_file.close()
    split = []
    for l in lines:
        split = l.split(' ')
        if len(split) >= 1 and split[0] == table_name:
            break
    if len(split) == 0:
        raise KeyError(f'table name "{table_name}" not found in index file"{db_path}/index.txt"')

    # columns: words in index row without table name
    columns = split[1:]

    # initialize dictionary:
    output = {col: [] for col in columns}

    file = open(f"{db_path}/{table_name}", 'r')
    table_entries = file.readlines()
    for entry in table_entries:
        split_entry = entry.split(" ")
        for i in range(len(split_entry)):
            output[columns[i]].append(split_entry[i].replace('\n', ''))
    file.close()
    return output


def group_by(input_dict: dict, group: str) -> dict:
    raise NotImplementedError


########## </relational algebra operations>




class Query:
    class Node:
        def __init__(self, operation, parameters):
            self.children: List[Query.Node] = []
            self.operation = operation
            self.parameters = parameters
            raise NotImplementedError


class Database:
    def __init__(self, root: str):
        self.root: str = input
        raise NotImplementedError

    def query(self, query: Query) -> str:
        # traverse querytree from bottom to top and execute operations
        # return table created by final operation
        raise NotImplementedError


class Parser:
    KEYWORDS = ["SELECT", "FROM", "WHERE"]  # "GROUP BY"
    @staticmethod
    def parse(input: str) -> Query:
        # create Query-tree from sql-string
        raise NotImplementedError


if __name__ == "__main__":

    # database = Database(test_database)
    # query = Parser.parse(sql_teststring)
    # database.query(query)
    test = project(load('test_db', 'TEST'), ['fav_food'])

    print(test)
