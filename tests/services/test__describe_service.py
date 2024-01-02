import networkx as nx

from prep2dbt.describe_services import (calculate_average_degree,
                                        calculate_density,
                                        calculate_depth_and_width,
                                        calculate_node_and_edge_count,
                                        calculate_source_and_sink_count)
from prep2dbt.models.graph import DAG


class TestDescribeService:
    def test__calculate_node_and_edge_count(self):
        # 1 -┐
        #    2 - 4
        # 3 -┘
        g = nx.DiGraph([(1, 2), (3, 2), (2, 4)])
        in_dag = DAG(g)
        actual = calculate_node_and_edge_count(in_dag)
        expected = (4, 3)
        assert actual == expected

    def test__calculate_source_and_sink_count(self):
        # 1 -┐
        #    2 - 4
        # 3 -┘
        g = nx.DiGraph([(1, 2), (3, 2), (2, 4)])
        in_dag = DAG(g)
        actual = calculate_source_and_sink_count(in_dag)
        expected = (2, 1)
        assert actual == expected

    def test__calculate_depth_and_width(self):
        # 1 -┐
        #    2 - 4
        # 3 -┘
        g = nx.DiGraph([(1, 2), (3, 2), (2, 4)])
        in_dag = DAG(g)
        actual = calculate_depth_and_width(in_dag)
        expected = (3, 2)
        assert actual == expected

    def test__calculate_density(self):
        # 1 -┐
        #    2 - 4
        # 3 -┘
        g = nx.DiGraph([(1, 2), (3, 2), (2, 4)])
        in_dag = DAG(g)
        actual = calculate_density(in_dag)
        expected = 0.5
        assert actual == expected

    def test__calculate_density__zero(self):
        g = nx.DiGraph()
        in_dag = DAG(g)
        actual = calculate_density(in_dag)
        expected = 0
        assert actual == expected

    def test__calculate_average_degree(self):
        # 1 -┐
        #    2 - 4
        # 3 -┘
        g = nx.DiGraph([(1, 2), (3, 2), (2, 4)])
        in_dag = DAG(g)
        actual = calculate_average_degree(in_dag)
        expected = 1.5  # degreeは、1, 3, 1, 1
        assert actual == expected
