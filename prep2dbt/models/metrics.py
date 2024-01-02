from dataclasses import dataclass


@dataclass(frozen=True)
class NodeMetrics:
    in_degree: int
    out_degree: int
    id: str
    name: str
    node_type: str

    def to_csv(self) -> str:
        return (
            self.id
            + ","
            + self.name
            + ","
            + self.node_type
            + ","
            + str(self.in_degree)
            + ","
            + str(self.out_degree)
        )


@dataclass
class Metrics:
    node_count: int
    edge_count: int
    width: int
    depth: int
    density: float
    source_node_count: int
    sink_node_count: int
    average_degree: float
    entropy: float
    nodes: list[NodeMetrics]

    def nodes_to_csv(self, header=True) -> str:
        import os

        result = ""

        if header:
            result = "id,name,node_type,in_degree,out_degree"

        for node in self.nodes:
            result = result + os.linesep + node.to_csv()

        return result
