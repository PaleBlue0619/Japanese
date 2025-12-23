import pandas as pd
import networkx as nx
import dolphindb as ddb
from pyecharts import options as opts
from pyecharts.charts import Graph
from collections import defaultdict

if __name__=="__main__":
    G=nx.read_gexf(r"japanese_kanji_network.gexf")  # MultiGraph
    G_simple=nx.Graph(G)
    print(len(G.edges()))
    connected_components = list(nx.connected_components(G_simple))

    # Calculate MST for each component
    mst_node_sets = []
    mst_edge_sets = []
    counter = 0
    index = 0
    for component in connected_components:
        subgraph = G_simple.subgraph(component)
        mst = nx.minimum_spanning_tree(subgraph)
        mst_node_sets.append(set(mst.nodes()))
        index = index +1    # 子图标号
        for u, v, data in mst.edges(data=True):
            # 获取原始边属性（如果有的话）
            kanji = data.get('kanji', '')
            kana = data.get('kana', '')
            if kanji.index(u) < kanji.index(v):
                mst_edge_sets.append({"component":index,"start":u,"end":v,"kanji":kanji,"kana":kana})   # 根据原有汉字对应顺序排序
            else:
                mst_edge_sets.append({"component":index,"start":v,"end":u,"kanji":kanji,"kana":kana})
            counter+=1

    result_df = pd.DataFrame(mst_edge_sets)
    result_df.to_excel(r"../汉字.xlsx",index=None)

    # MST for each component
    for i, node_set in enumerate(mst_node_sets, 1):
        print(f"Component {i}: {node_set}")
