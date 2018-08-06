from flask import Flask, request, jsonify
from py2neo import Graph
import logging
import csv

app = Flask(__name__)
logging.basicConfig(level=logging.INFO)

uri = "bolt://192.168.99.100:7687"


def get_graph():
    global uri
    uri += "/db/data/"
    graph = Graph(uri)
    return graph


@app.route('/')
def hello_world():
    return "<h1 style='color:blue'>阿帕比neo4j</h1>"


@app.route('/update')
def update_db():
    # 清空数据库
    graph = get_graph()
    graph.delete_all()

    # 从csv文件中读取数据
    with open('resources/mysql_data_test.csv', encoding='utf-8-sig') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            metaid = row['metaid']
            labels = row['label']
            year = row['year']
            # 插入数据
            insert_into_db(metaid, labels, year, graph)
    return "Database updated", 200


def insert_into_db(metaid, labels, year, graph):
    graph.run("merge (b:Book {metaid: '" + metaid + "'}) "
              "merge (y:Year {value: toInt(" + year + ")}) "
              "merge (b)-[:PUBLISHED_IN]->(y)")
    lst = labels.split('&')
    for label in lst:
        graph.run("merge (b:Book {metaid: '" + metaid + "'}) "
                  "merge (l:Label {name: '" + label + "'}) "
                  "merge (b)-[:HAS]->(l)")


@app.route('/query', methods=['POST'])
def query():
    request_json = request.json
    labels = request_json['labels'].split('&')
    graph = get_graph()
    results = graph.run("match (a:Book)-[:HAS]->(l:Label), "
                        "(a)-[:PUBLISHED_IN]->(y:Year)  "
                        "where l.name in " + str(labels) +
                        " and y.value > 2015 "
                        " return a.metaid")
    records = {}
    for record in results:
        metaid = record["a.metaid"]
        if metaid not in records:
            records[metaid] = 1
        else:
            records[metaid] += 1

    if not records:
        return jsonify(message="没有找到您想要的书籍")

    return jsonify(records)


if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=80)
