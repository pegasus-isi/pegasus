#  Copyright 2007-2014 University Of Southern California
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#  http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing,
#  software distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

__author__ = "Rajiv Mayani"

import heapq
import logging

log = logging.getLogger(__name__)


class Node:
    pass


class TableNode(Node):
    def __init__(self, prefix):
        self._prefix = prefix
        self._table = None

        self._parents = set()
        self._children = set()

    @property
    def prefix(self):
        return self._prefix

    @property
    def parents(self):
        yield from self._parents

    @property
    def children(self):
        yield from self._children

    def add_parent(self, parent):
        if parent:
            self._parents.add(parent)

    def add_child(self, child):
        if child:
            self._children.add(child)

    def __eq__(self, other):
        return self.prefix == other.prefix

    def __str__(self):
        return self.prefix

    def __repr__(self):
        return "<TableNode: %s>" % self.prefix


class Graph:
    def __init__(self):
        self._nodes = set()
        self._edge_map = {}
        self._root = []

    def get_root_nodes(self):
        yield from self._nodes

    def add_node(self, node):
        if not isinstance(node, Node):
            return Exception("Expecting Node object")

        self._nodes.add(node)

    def add_edge(self, parent, child):
        if parent.prefix not in self._nodes:
            self.add_node(parent)

        if child.prefix not in self._nodes:
            self.add_node(child)

        parent.add_child(child)
        child.add_parent(parent)

        self._edge_map[(parent, child)] = True

    def get_joins(self, parent, child):
        joins = []

        #
        # Breadth First Search
        #
        back_ref = {}
        queue = [parent]
        visited = set()

        def traverse_back_ref(dest):
            while dest in back_ref:
                src = back_ref[dest]
                log.debug(f"Back Ref: src -> dest = {src} -> {dest}")
                joins.insert(0, (src, dest))
                dest = src

        while queue:
            node = queue.pop(0)

            visited.add(node)

            if node == child:
                traverse_back_ref(node)
                break

            for c in node.children:
                if c not in visited:
                    queue.append(c)
                    back_ref[c] = node

        return joins

    def _is_edge_present(self, parent, child):
        return (parent, child) in self._edge_map

    def get_priority_map(self, start):
        if not start or start not in self._nodes:
            return None

        stack = [(0, start)]

        #
        # Depth First Search
        #
        max_depth = {}

        while stack:
            depth, node = stack.pop()

            max_depth[node] = (
                max(depth, max_depth[node]) if node in max_depth else depth
            )

            for child in node.children:
                stack.append((depth + 1, child))

        heap = []
        [heapq.heappush(heap, (depth, node)) for node, depth in max_depth.iteritems()]

        return heap


class E:
    def __init__(self):
        self.a = "A"
        self.b = "B"
        self.c = "C"


class D:
    def __init__(self):
        self.a = "A"
        self.b = "B"
        self.c = "C"


class A:
    def __init__(self):
        self.a = "A"
        self.b = "B"
        self.c = "C"
        self.d = D()


import json
from collections import OrderedDict


class ResultsWrapper:
    def __init__(self, records, total_records=None, total_filtered=None):
        self.records = records
        self.total_records = total_records
        self.total_filtered = total_filtered


class ComplexEncoder(json.JSONEncoder):
    INSTANCES = {}

    def default(self, obj):
        if isinstance(obj, ResultsWrapper):
            ComplexEncoder.INSTANCES["ResultsWrapper"] = True
            return OrderedDict(
                [
                    ("records", obj.records),
                    (
                        "_meta",
                        OrderedDict(
                            [
                                ("total_records", obj.total_records),
                                ("total_filtered", obj.total_filtered),
                            ]
                        ),
                    ),
                ]
            )
        elif isinstance(obj, A):
            ComplexEncoder.INSTANCES["A"] = True
            return {"Aa": obj.a, "Ab": obj.b, "Ac": obj.c, "Ad": obj.d}
        elif isinstance(obj, D):
            ComplexEncoder.INSTANCES["D"] = True
            return {"Ba": obj.a, "Bb": obj.b, "Bc": obj.c}
        elif isinstance(obj, E):
            ComplexEncoder.INSTANCES["E"] = True
            return {"Ca": obj.a, "Cb": obj.b, "Cc": obj.c}
        print(ComplexEncoder.INSTANCES)
        # Let the base class default method raise the TypeError
        return json.JSONEncoder.default(self, obj)


def main():
    w = TableNode("w")
    ws = TableNode("ws")
    j = TableNode("j")
    ji = TableNode("ji")
    t = TableNode("t")
    js = TableNode("js")
    i = TableNode("i")
    h = TableNode("h")

    graph = Graph()

    graph.add_edge(w, ws)
    graph.add_edge(w, j)
    graph.add_edge(w, t)
    graph.add_edge(w, h)
    graph.add_edge(w, i)

    graph.add_edge(j, t)
    graph.add_edge(j, ji)

    graph.add_edge(ji, h)
    graph.add_edge(ji, i)
    graph.add_edge(ji, js)

    graph.add_edge(t, i)

    heap = graph.get_priority_map(w)

    while heap:
        n = heapq.heappop(heap)
        print("Node:", n)

    print(graph.get_joins(w, ws))
    print(graph.get_joins(w, js))


if __name__ == "__main__":
    main()
