import re

class Node:
    def __init__(self, id, operator, input, result, params):
        self.id = id
        self.operator = operator
        self.input = input
        self.result = result
        self.params = params
        self.children = []
        self.parent = None

    def add_child(self, child_node):
        self.children.append(child_node)
        child_node.parent = self

    def __repr__(self):
        return f"Node(id = {self.id}, operator={self.operator}, input={self.input}, result={self.result}, params={self.params}, parent={self.parent.id if self.parent else None}, children={[child.id for child in self.children]})"
    
class Tree:
    def __init__(self, root, node_list, tables_data):
        self.root = root
        self.node_list = node_list
        self.valid_push_down_ops = ['join', 'sem_join', 'map', 'sem_map', 'groupby', 'sem_cluster_by', 'column_calculate', 'filter', 'sem_filter']
        self.tables_data = tables_data
        self.push_down_set = []
        self.swap_cnt = 0

    def insert(self, node, child_node):
        node.add_child(child_node)

    def find_leaf_nodes(self, node):
        if not node.children:
            return [node]
        leaf_nodes = []
        for child in node.children:
            leaf_nodes.extend(self.find_leaf_nodes(child))
        return leaf_nodes

    def swap_nodes(self, node, child_node, grandchild):
        self.swap_cnt += 1
        parent_node = node.parent
        if parent_node:
            parent_node.children.remove(node)
            parent_node.children.append(child_node)
        node.children.remove(child_node)
        node.children.append(grandchild)
        node.parent = child_node
        child_node.children.remove(grandchild)
        child_node.children.append(node)
        child_node.parent = parent_node
        grandchild.parent = node
        if node == self.root:
            self.root = child_node

    def find_sem_join(self, filter_node, node):
        if node.operator not in self.valid_push_down_ops:
            return False
        if node.operator == 'sem_join':
            target_node = self.find_child_node_with_column(filter_node, node)
            return target_node is not None
        else:
            return self.check_child_node(filter_node, node)
                
    # check if the filter_node can be pushed down to child_node
    def check_child_node(self, node, child_node):
        if child_node.operator == 'init':
            return False
        # if child_node is groupby or sem_cluster_by, then the filter can be pushed down only if the filter column is in the groupby columns
        if child_node.operator == 'groupby' or child_node.operator == 'sem_cluster_by':
            groupby_cols = child_node.params.split(';;')[1]
            if groupby_cols[0] == '[':
                groupby_cols = [i.strip() for i in groupby_cols[1:-1].split(',')]
            else:
                groupby_cols = [groupby_cols]
            if node.operator == 'filter':
                col = node.params.split(';;')[1]
                return col in groupby_cols 
            else:
                cols = parse_cols(node.params.split(';;')[1])
                for col in cols:
                    if col not in groupby_cols:
                        return False
                return True
        # if it is Join, it needs to determine based on the type of the filter operator
        elif child_node.operator == 'join':
            if node.operator == 'filter':
                return self.find_child_node_with_column(node, child_node) is not None
            # for sem_filter, check if there is an exchangeable sem_join in the subtree. If not, it is not necessary to push down.
            else:
                target_node = self.find_child_node_with_column(node, child_node)
                if not target_node:
                    return False
                return self.find_sem_join(node, target_node)
        elif child_node.operator == 'agg' or child_node.operator == 'value_calculate' and 'filter' in node.operator:
            return False
        else:
            return self.find_child_node_with_column(node, child_node) is not None
        
    def find_child_node_with_column(self, node, child_node):
        for grandchild in child_node.children:
            leaf_nodes = self.find_leaf_nodes(grandchild)
            tables = [leaf_node.result for leaf_node in leaf_nodes]
            found = False
            valid = True
            target_node = None
            for table in tables:
                df = self.tables_data[table]
                # there is only one column in filter
                if node.operator == 'filter':
                    col = node.params.split(';;')[1]
                    if col in df.columns:
                        target_node = grandchild
                        found = True
                        break
                # there are multiple columns in sem_filter
                else:
                    cols = parse_cols(node.params.split(';;')[1])
                    count = 0
                    for col in cols:
                        if col in df.columns:
                            count += 1
                    if count != 0 and count != len(cols):
                        valid = False
                        break
                    elif count == len(cols):
                        target_node = grandchild
                        found = True
                        break

            if found or not valid:
                break

        return target_node

    def push_down_filter(self, node, child_node):
        if not self.check_child_node(node, child_node):
            return
        # if child_node is not join, sem_join or filter, then it must have only one child
        if child_node.operator != 'join' and child_node.operator != 'sem_join' and child_node.operator != 'filter':
            self.swap_nodes(node, child_node, child_node.children[0])
        # If it is join, it is necessary to traverse all child nodes to find the node that contains the table with the corresponding column in the subtree.
        else:
            target_node = self.find_child_node_with_column(node, child_node)
            if target_node:
                self.swap_nodes(node, child_node, target_node)

    def push_down(self):
        flag = True
        while flag:
            flag = False
            queue = [self.root]
            uncompleted_nodes = None
            while queue:
                node = queue.pop(0)
                if node not in self.push_down_set and (node.operator == 'filter' or node.operator == 'sem_filter'):
                    uncompleted_nodes = node
                    flag = True
                    break
                queue.extend(node.children)

            if uncompleted_nodes:
                uncompleted = True
                while uncompleted:
                    uncompleted = False
                    for child in uncompleted_nodes.children:
                        if self.check_child_node(uncompleted_nodes, child):
                            self.push_down_filter(uncompleted_nodes, child)
                            uncompleted = True
                            break
                    if not uncompleted:
                        self.push_down_set.append(uncompleted_nodes)

    def estimate(self, node):
        if not node.operator.startswith('sem_'):
            return 0
        if node.operator == 'sem_filter':
            leaf_nodes = self.find_leaf_nodes(node)
            tables = [leaf_node.result for leaf_node in leaf_nodes]
            cols = parse_cols(node.params.split(';;')[1])
            count = 1
            for col in cols:
                for table in tables:
                    df = self.tables_data[table]
                    if col in df.columns:
                        count *= len(df[col].unique())
            return count
        else:
            leaf_nodes = self.find_leaf_nodes(node)
            tables = [leaf_node.result for leaf_node in leaf_nodes]
            cols = parse_cols(node.params.split(';;')[2])
            left_on = None
            right_on = None
            for col in cols:
                if ":left" in col:
                    left_on = col.split(":left")[0]
                elif ":right" in col:
                    right_on = col.split(":right")[0]
            for table in tables:
                df = self.tables_data[table]
                if left_on in df.columns:
                    left_count = len(df[left_on].unique())
                if right_on in df.columns:
                    right_count = len(df[right_on].unique())

            return left_count * right_count

    def swap_same_type_operator(self):
        chains = self.find_same_type_operator()
        for chain in chains:
            # if it is Filter type operator
            if chain[0].operator == 'filter' or chain[0].operator == 'sem_filter':
                for i in range(len(chain) - 1):
                    for j in range(i, len(chain) - 1):
                        if self.estimate(chain[j]) < self.estimate(chain[j + 1]):
                            if len(chain[j + 1].children) == 1:
                                self.swap_nodes(chain[j], chain[j + 1], chain[j + 1].children[0])
                                chain[j], chain[j + 1] = chain[j + 1], chain[j]
                            else:
                                for child in chain[j + 1].children:
                                    if child.operator in self.valid_push_down_ops:
                                        self.swap_nodes(chain[j], chain[j + 1], child)
                                        chain[j], chain[j + 1] = chain[j + 1], chain[j]
                                        break
            # if it is Join type operator
            else:
                for i in range(len(chain) - 1):
                    for j in range(i, len(chain) - 1):
                        if self.estimate(chain[j]) < self.estimate(chain[j + 1]):
                            leaf_nodes = self.find_leaf_nodes(chain[j + 1])
                            tables = [leaf_node.result for leaf_node in leaf_nodes]
                            if chain[j].operator == 'join':
                                cols = [chain[j].params.split(';;')[2], chain[j].params.split(';;')[3]]
                            else:
                                cols = parse_cols(chain[j].params.split(';;')[2])
                                for i in range(len(cols)):
                                    cols[i] = cols[i].split(':')[0]

                            col_to_join = None
                            for col in cols:
                                for table in tables:
                                    df = self.tables_data[table]
                                    if col in df.columns:
                                        col_to_join = col
                                        table_to_join = table
                                        break
                                if col_to_join:
                                    break
                            
                            if col_to_join is None:
                                return 
                            
                            for child in chain[j + 1].children:
                                if table_to_join in [leaf_node.result for leaf_node in self.find_leaf_nodes(child)]:
                                    child_to_join = child
                                    break
                            chain[j + 1].children.remove(child_to_join)
                            chain[j + 1].children.append(chain[j])
                            chain[j + 1].parent = chain[j].parent
                            chain[j].children.remove(chain[j + 1])
                            chain[j].children.append(child_to_join)
                            chain[j].parent = chain[j + 1]
                            child_to_join.parent = chain[j]
                            if chain[j] == self.root:
                                self.root = chain[j + 1]
                            chain[j], chain[j + 1] = chain[j + 1], chain[j]

    def find_same_type_operator(self):
        Join = ['join', 'sem_join']
        Filter = ['filter', 'sem_filter']
        same_type_operator_pairs = []
        queue = [self.root]
        while queue:
            current_node = queue.pop(0)
            for child in current_node.children:
                if child.operator in Join and current_node.operator in Join:
                    same_type_operator_pairs.append((current_node, child))
                elif child.operator in Filter and current_node.operator in Filter:
                    same_type_operator_pairs.append((current_node, child))
                queue.append(child)

        # Link the pair into a chain
        chains = []
        for pair in same_type_operator_pairs:
            found = False
            for chain in chains:
                if pair[0] == chain[-1]:
                    chain.append(pair[1])
                    found = True
                    break
            if not found:
                chains.append([pair[0], pair[1]])

        return chains
    
    def topological_sort(self):
        visited = []
        while len(visited) < len(self.node_list):
            for node in self.node_list:
                if node in visited:
                    continue
                state = True
                for child in node.children:
                    if child not in visited:
                        state = False
                        break
                if state:
                    visited.append(node)
                    break

        return visited
    
    def params_rewrite(self):
        topo_sort = self.topological_sort()
        for node in topo_sort:
            if node.operator == "init" or node.operator == "value_calculate":
                continue
            elif node.operator in ['agg', 'head', 'map', 'sem_map', 'groupby', 'sem_cluster_by', 'sem_agg', 'sem_filter', 'sem_topk', 'column_calculate', 'orderby', 'filter']:
                if len(node.children) == 1:
                    node.input = [node.children[0].result]
                    node.params = f"{node.children[0].result};;{';;'.join(node.params.split(';;')[1:])}"
                    if node.operator != 'agg':
                        node.result = node.children[0].result + "_" + node.operator
                else:
                    for child in node.children:
                        print(f"Processing child: {child}")
                        if child.operator != 'agg' and child.operator != 'value_calculate':
                            node.input[0] = child.result
                            node.params = f"{child.result};;{';;'.join(node.params.split(';;')[1:])}"
                            print(f"Updated params: {node.params}")
                            node.result = child.result + "_" + node.operator
                        else:
                            node.params = node.params.replace(child.result, '<placeholder>')
            else:
                if node.operator == 'join':
                    left_on = node.params.split(';;')[2]
                    right_on = node.params.split(';;')[3]
                    params_left = f"{left_on};;{right_on}"
                elif node.operator == 'sem_join':
                    cols = parse_cols(node.params.split(';;')[2])
                    for col in cols:
                        if ":left" in col:
                            left_on = col.split(":left")[0]
                        elif ":right" in col:
                            right_on = col.split(":right")[0]
                    params_left = f"{node.params.split(';;')[2]}"
                if left_on == right_on:
                    if node.children[0].operator == 'init':
                        left_node = node.children[1]
                        right_node = node.children[0]
                    else:
                        left_node = node.children[0]
                        right_node = node.children[1]
                else:
                    left_node = None
                    right_node = None
                    first_leaf_nodes = self.find_leaf_nodes(node.children[0])
                    second_leaf_nodes = self.find_leaf_nodes(node.children[1])
                    first_tables = [leaf_node.result for leaf_node in first_leaf_nodes]
                    second_tables = [leaf_node.result for leaf_node in second_leaf_nodes]
                    for table in first_tables:
                        df = self.tables_data[table]
                        if left_on in df.columns:
                            left_node = node.children[0]
                            break
                    for table in second_tables:
                        df = self.tables_data[table]
                        if right_on in df.columns:
                            right_node = node.children[1]
                            break
                    if left_node is None or right_node is None:
                        left_node, right_node = node.children[1], node.children[0]
                node.input[0] = left_node.result
                node.input[1] = right_node.result
                node.params = f"{left_node.result};;{right_node.result};;{params_left}"
                node.result = left_node.result + "_" + node.operator

    def optimize(self):
        self.push_down()
        self.swap_same_type_operator()
        self.params_rewrite()

    def tree2json(self):
        topo_sort = self.topological_sort()
        res = [
            {
                "id": node.id,
                "operator": node.operator,
                "input": node.input,
                "result": node.result,
                "params": node.params,
                "children": [child.id for child in node.children],
                "parent": node.parent.id if node.parent else None
            }
            for node in topo_sort
        ]
        return res

    def __repr__(self):
        res = ""
        queue = [self.root]
        while queue:
            node = queue.pop(0)
            res += f"{node}\n"
            for child in node.children:
                queue.append(child)
        return res

def parse_cols(text: str) -> list[str]:
    # Regular expression pattern to match variables in brackets not escaped by double brackets
    pattern = r"(?<!\{)\{(?!\{)(.*?)(?<!\})\}(?!\})"
    # Find all matches in the text
    matches = re.findall(pattern, text)
    return matches

def parse_param(process):
    inputs = []
    params = []
    for i in range(len(process[0]['initial_table'])):
        inputs.append([])
        params.append(None)
    for i in range(1, len(process)):
        operator = process[i]['operator']
        param = process[i]['params']
        op_with_one_param = ['agg', 'head', 'map', 'sem_map', 'groupby', 'sem_cluster_by', 'sem_agg', 'sem_filter', 'sem_topk', 'column_calculate', 'orderby']

        param_list = []
        if operator in op_with_one_param:
            param_list.append(param.split(';;')[0])
        else:
            if operator == 'filter':
                param_list.append(param.split(';;')[0])
                op_list = param.split(';;')[2].replace('[', '').replace(']', '').replace("(", "").replace(')', '')
                op_list = [i.strip() for i in op_list.split(',')]
                for i in range(0, len(op_list), 2):
                    param_list.append(op_list[i + 1])
            elif 'join' in operator:
                param_list.append(param.split(';;')[0])
                param_list.append(param.split(';;')[1])
            else:
                param_list = param.split(';;')[1:]

        inputs.append(param_list)
        params.append(param)
    return inputs, params

def parse_result(process):
    initial_table = process[0]['initial_table']
    results = [list(initial_table[i].keys())[0] for i in range(len(initial_table))]
    for i in range(1, len(process)):
        result = process[i]['result']
        current_table = process[i]['current_table']
        
        if 'table_length' in result:
            result = current_table[-1]
        else:
            result = result.split('result: ')[1]

        results.append(result)
    
    return results

def build_tree(process, tables_data):
    initial_table_count = len(process[0]['initial_table'])
    operators = ['init' for _ in range(initial_table_count)]
    for i in range(1, len(process)):
        operators.append(process[i]['operator'])
    inputs, params = parse_param(process)
    results = parse_result(process)
    
    node_list = []
    for i in range(len(inputs)):
        if i < initial_table_count:
            node = Node(i, "init", inputs[i], results[i], params[i])
        else:
            node = Node(i, operators[i], inputs[i], results[i], params[i])
        node_list.append(node)

    op_tree = Tree(node_list[-1], node_list, tables_data)
    for i in range(len(inputs) - 1, 0, -1):
        for j in range(i):
            for input in inputs[i]:
                if input == results[j]:
                    op_tree.insert(node_list[i], node_list[j])

    for i in range(len(node_list) - 1):
        if node_list[i].parent is None:
            print(node_list[i])
            print("Invalid tree.")
            exit(0)
            break

    op_tree.optimize()

    return op_tree
