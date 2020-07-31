import re
import functools
from typing import List, Union


test_database = "test_db"
testquery =\
"SELECT fav_food\n" \
"FROM TEST T, BLUB B\n" \
"WHERE T.firstname == 'bob'\n" \
"and T.lastname == 'burnquist'\n"




# QueryTree, Query Object passed to DB should usually be root node (individual objects are individual nodes)
class Query:

    ########## <relational algebra operations>
    ### (self, input_dicts, parameters) -> output_dict
    def project(self, input_dict: List[dict], requested_columns: List[str]) -> dict:  # TODO: receives str, not List[str]
        input_dict = input_dict[0]  # hacky bandaid
        for column in requested_columns:
            try:
                assert column in input_dict
            except AssertionError:
                raise KeyError(f"column '{column}' doesn't exist in table: \n {input_dict}")
        return {key: input_dict[key] for key in requested_columns}

    def select(self, input_dict_list: List[dict], boolean_expression: str) -> dict:

        # one exponential runtime function, coming right up! (gnarf) (maybe noone notices if we only ever combine 2 tables and noone reads this comment)
        indices = [0]*len(input_dict_list)
        sizes = []  # count of rows in each input_dict
        for d in input_dict_list:
            keys = list(d.keys())
            keys.remove('TABLE_NAME')
            sizes.append(len(d[keys[0]]))

        while True:



        assert 1 == 1
        # boolean expression as string? -> safe eval?
        raise NotImplementedError

    def load(self, input_dict: List[dict], table_name: str) -> dict:
        # input_dict stays null
        db_path = self.root
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
        output = {f'{table_name}.{col}': [] for col in columns}
        try:
            file = open(f"{db_path}/{table_name}", 'r')
            table_entries = file.readlines()
            for entry in table_entries:
                split_entry = entry.split(" ")
                for i in range(len(split_entry)):
                    output[f"{table_name}.{columns[i]}"].append(split_entry[i].replace('\n', ''))
            file.close()
        except FileNotFoundError:
            print(f'Table "{table_name}" not found')
            raise
        output['TABLE_NAME'] = table_name  # TODO: hacky bandaid fix so the dictionary knows what table it's from gnarf
        return output

    #def group_by(input_dict: dict, group: str) -> dict:
    #    raise NotImplementedError

    relational_algebra_operations = {'SELECT': project, 'FROM': lambda s, d, p: d, 'LOAD': load, 'WHERE': select}

    ########## </relational algebra operations>

    def __init__(self, operation, parameters):
        self.children: List[Query] = []
        self.operation = operation
        self.parameters = parameters
        self.root = "" # rootDIRECTORY for the tables, not root of the tree (misnomer?)

    @property
    def root(self):
        return self._root

    # recursively set all children to know about the root directory
    @root.setter
    def root(self, new):
        self._root = new
        for child in self.children:
            child.root = new

    def execute(self) -> dict:
        op = Query.relational_algebra_operations[self.operation]
        return op(self, [child.execute() for child in self.children], self.parameters)




class Database:
    def __init__(self, root: str):
        self.root: str = root


    def query(self, input: str) -> dict:
        '''

        :param query: valid "µSQL" in string format
        :return: dictionary representing the return table
        '''

        # traverse querytree from bottom to top and execute operations
        # return table created by final operation
        parsed = Parser.parse(input)
        parsed.root = self.root  # (so the load function knows where to look for tables
        return parsed.execute()



class Parser:
    KEYWORDS = ["SELECT", "FROM", "WHERE"]  # "GROUP BY",  "as"
    @staticmethod
    def parse(input: str) -> Query:
        # create Query-tree from sql-string

        # STEP 0: preliminary string cleanup
        # replace linebreaks with ' '
        clean = input.replace('\n', ' ')
        # remove repeating + trailing spaces:
        while "  " in clean:
            clean.replace('  ', ' ')
        while clean[-1] == ' ':
            clean = clean[:-1]

        # STEP 1: extract nested queries, treat recursively (implement later, nice-to-have for now)
        outer = clean  # (implement later if needed)

        # STEP 2: identify keywords, separate remaining string into (keyword, parameter) dictionaries
        keyword_parameter_dicts = []
        words = outer.split(' ')
        for word in words:
            if word in Parser.KEYWORDS:
                keyword_parameter_dicts.append({'keyword': word, 'parameter': []})
            else:
                keyword_parameter_dicts[-1]['parameter'].append(word)

        for d in keyword_parameter_dicts:
            d['parameter'] = functools.reduce(lambda x, y: x + y + ' ', d['parameter'], '')[:-1]

        # STEP 3: identify & substitute(homogenize) aliases
        aliases = []
        from_parameter = [d for d in keyword_parameter_dicts if d['keyword'] == 'FROM'][0]['parameter']
        # TODO : only works with exactly 1 FROM statement (outside nested statements) - probably not an issue
        # (just picks the first FROM it finds)#
        from_parameters = re.split(',[ ]*', from_parameter)

        for parameter in from_parameters:
            if " " in parameter:
                aliases.append(re.split('[ ]+', parameter))

        # to-be-used (full) name in position

        for aliaslist in aliases:
            for alias in aliaslist[1:]:
                for d in keyword_parameter_dicts:
                    pattern = f'(?<=\W){alias}(?=\W)'
                    replacement = aliaslist[0]
                    d['parameter'] = re.sub(pattern, replacement, f" {d['parameter']} ")[1:-1]
                    # a bit hacky. adding ' ' to the front and back so we can just search for \W instead of \W|$|^
                    # (which seems to work differently than expected)

        # remove (now) duplicated schema name in FROM statements (since we replaced the alias):
        for d in keyword_parameter_dicts:
            if d['keyword'] == "FROM":
                for aliaslist in aliases:
                    while d['parameter'].count(aliaslist[0]) > 1:
                        start_index = d['parameter'].index(aliaslist[0])
                        last_index = start_index + len(aliaslist[0])+1  # +1 error yay (was causing excessive spaces)
                        d['parameter'] = d['parameter'][0:start_index]+d['parameter'][last_index:]


        #print(keyword_parameter_dicts)
        # STEP 4: create Query-Node for each (keyword, parameter) tuple and connect the nodes

        #FROM:
        from_dict = {}
        for d in keyword_parameter_dicts:
            if d['keyword'] == 'FROM':
                from_dict = d
        from_node = Query('FROM', from_dict['parameter'])
        # here, we could include nested queries instead of loading tables from stored files (#later)
        load_nodes = from_dict['parameter'].split(',')
        load_nodes = [node.replace(' ', '') for node in load_nodes]
        for node in load_nodes:
            q = Query('LOAD', node)
            from_node.children.append(q)

        #WHERE:
        where_dict = {}
        for d in keyword_parameter_dicts:
            if d['keyword'] == 'WHERE':
                where_dict = d
        where_node = Query('WHERE', where_dict['parameter']) # might have to do some substitution here or later so eval works *cough*

        #SELECT:
        select_dict = {}
        for d in keyword_parameter_dicts:
            if d['keyword'] == 'SELECT':
                select_dict = d
        select_node = Query('SELECT', select_dict['parameter'])


        # connect nodes:
        select_node.children = [where_node]
        where_node.children = [from_node]
        return select_node



if __name__ == "__main__":

    database = Database(test_database)
    test_query = database.query(testquery)

    #test = project(load('test_db', 'TEST'), ['fav_food'])
    #print (testquery)

    #print(test)
