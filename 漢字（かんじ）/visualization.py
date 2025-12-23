import itertools
import pandas as pd
import networkx as nx
import dolphindb as ddb
from pyecharts import options as opts
from pyecharts.charts import Graph
from collections import defaultdict

if __name__=="__main__":
    # Step1. 数字準備
    session=ddb.session()
    session.connect("8.152.192.31",8848,"admin","123456")
    pool=ddb.DBConnectionPool("8.152.192.31",8848,10,"admin","123456")
    df: pd.DataFrame() = session.run("""
    select kana_str,kana_pair,kanji,rensou_kanji,rensou_kana from loadTable("dfs://JP","word") where regexCount(rensou_kana,"）")==0 and regexCount(rensou_kana,"（")==0 and strlenu(rensou_kanji)<=3
    """)
    print(df)

    # Step2. 节点&连边準備
    # 创建节点和边的数据结构
    nodes = []
    edges = []
    node_set = set()
    edge_tracker = set()
    G = nx.Graph()

    for _, row in df.iterrows():
        kanji_chars = list(row['rensou_kanji'])
        rensou_kanji = row['rensou_kanji']
        rensou_kana = row['rensou_kana']

        # 如果kanji只有一个字，跳过（无法建立边）
        if len(kanji_chars) < 2:
            continue

        # 为kanji中的每个字与其他字建立边
        # char_pairs = itertools.combinations(kanji_chars, 2)
        char_pairs = [(kanji_chars[0],kanji_chars[-1])]
        for char1, char2 in char_pairs:
            edge_key = tuple(sorted((char1, char2)))

            if edge_key in edge_tracker:
                continue

            edge_tracker.add(edge_key)

            # 如果节点不存在，添加到节点集合
            if char1 not in node_set:
                node_set.add(char1)
                nodes.append({
                "name": char1,
                "symbolSize": 10,
                "category": char1
            })
                G.add_node(char1)
            if char2 not in node_set:
                node_set.add(char2)
                nodes.append({
                        "name": char2,
                        "symbolSize": 10,
                        "category": char2
                })
                G.add_node(char2)

            # 添加边
            edges.append({
                "source": char1,
                "target": char2,
                "label": {
                    "show": True,
                    "formatter": rensou_kanji,
                    "position": "middle"
                },
                "lineStyle": {
                    "width": 1,
                    "curveness": 0.2
                },
                "emphasis": {
                    "label": {
                        "show": True,
                        "formatter": f"{rensou_kanji}\n{rensou_kana}"
                    }
                },
                "tooltip": {
                    "formatter": f"{rensou_kanji}<br>{rensou_kana}"
                }
            })
            G.add_edge(char1,char2,kanji=rensou_kanji,kana=rensou_kana)

    # 创建图
    graph = (
        Graph(init_opts=opts.InitOpts(width="1600px",
                                      height="800px",
                                      renderer="canvas",
                                      bg_color="rgba(0,0,0,0)",
                                      js_host="")).add(
            "",
            nodes,
            edges,
            is_roam=True,   # 允许缩放和平移
            repulsion=1000,
            layout="force",
            is_draggable=True,
            categories=[{"name": char} for char in node_set],
            edge_label=opts.LabelOpts(
                is_show=True,
                position="middle",
                formatter="{c}",
                font_size=10,
            ),
            linestyle_opts=opts.LineStyleOpts(width=0.5, curve=0.3),
        ).set_global_opts(
            title_opts=opts.TitleOpts(title="日语汉字关联网络图"),
            legend_opts=opts.LegendOpts(is_show=False,pos_right=None,inactive_color=None),
            tooltip_opts=opts.TooltipOpts(
                trigger="item",
                formatter="{b}"
            ),
            # 确保没有多余的padding
            graphic_opts=[
                opts.GraphicGroup(
                    graphic_item=opts.GraphicItem(
                        bounding="raw",
                        right=0,
                        bottom=0,
                        z=100
                    )
                )]
        )
    )

    # 渲染图表
    graph.render("japanese_kanji_network.html")
    nx.write_gexf(G, "japanese_kanji_network.gexf")
