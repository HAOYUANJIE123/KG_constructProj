import copy

from GraphConstruction.function2pyneo import Neo4jDao
import time
import re
#使用Cypher语句获取两点间的全部路径
def findpaths_between2Node_cypher(headNam='',tailNam='',maxLength=20,findBest=True):
    startT=time.time()
    dao = Neo4jDao(username='neo4j', password='123456')
    graph=dao.my_graph

    #查询节点及其标签
    headNode=dao.findNode_with_name((headNam))
    headLabel=list(headNode._labels)[0]
    tailNode = dao.findNode_with_name((tailNam))
    tailLabel = list(tailNode._labels)[0]


    1.##构建Cpher语句并查询

    #搜索  所有最短路径
    # cypherStr='MATCH (p1:'+headLabel+'{name:"'+headNam+'"}), (p2:'+tailLabel+'{name:"'+tailNam+'"}),p=allshortestpaths((p1)-[*..'+str(maxLength)+']-(p2)) RETURN p'


    # 搜索  所有路径  使用weight计算总路长  进行降序排序， 输出所有最短路径
    # cypherStr = 'MATCH p=(:' + headLabel + '{name:"' + headNam + '"})-[*..'+str(maxLength)+']-(:' + tailLabel + '{name:"' + tailNam + '"}) WITH p, REDUCE(x=0,a IN relationships(p) | x+a.weight) AS sum_weight ORDER by sum_weight RETURN p'

    # 搜索  所有路径  使用weight计算总路长  输出一条最短路径
    # cypherStr = 'MATCH p=(:'+headLabel+'{name:"'+headNam+'"})-[*..'+str(maxLength)+']-(:'+tailLabel+'{name:"'+tailNam+'"}) WITH p, REDUCE(x=0,a IN relationships(p) | x+a.weight) AS sum_weight ORDER by sum_weight DESC LIMIT 1 RETURN p'

    # 搜索  所有路径，有环路出现
    # cypherStr = 'MATCH p=(:' + headLabel + '{name:"' + headNam + '"})-[*..'+str(maxLength)+']->(:' + tailLabel + '{name:"' + tailNam + '"})  RETURN p'

    # 搜索所有路径，避免环路，现在主要用这个
    cypherStr = 'MATCH path = (:' + headLabel + '{name:"' + headNam + '"})-[*..' + str(maxLength) + ']-(:' + tailLabel + '{name:"' + tailNam + '"}) UNWIND NODES(path) AS n WITH path, SIZE(COLLECT(DISTINCT n)) AS testLength WHERE testLength = LENGTH(path) + 1 RETURN path'


    print(cypherStr,'\n')
    results=graph.run(cypherStr)
    # print(results)
    results=results.to_series()

    paths=[]
    ##2.对搜索路径结果进行筛选，取包含升级关系的路径
    for num, path in enumerate(results):
        # 获取路径中的节点和关系
        nodes = path.nodes
        relations=path.relationships
        relationNams=[type(r).__name__ for r in relations]
        weights=[]
        nodeNams=[node['name'] for node in nodes]
        pathStr='_'.join(nodeNams) #对路径可视化为字符串形式
        if '升级' in relationNams:
            #路径权重记录为float数组
            idx = 0
            while idx<len(nodes)-1:
                node1=nodes[idx]
                node2=nodes[idx+1]
                weightVal = dao.get_relation_property(node1, node2, 'weight')
                weights.append(weightVal)
                idx+=1

            #路径排序算法，此时以weightSum代替
            weightsSum=round(sum(weights),4)


            pathInfo=[pathStr,weights,weightsSum]
            paths.append(pathInfo)
            # print('第 {} 条 '.format(num+1),pathInfo,'\n')

    ##3.筛选结果路径集降序排序
    paths=sorted(paths, key=lambda x: x[::-1], reverse=True)
    for idx, path in enumerate(paths):
        print('第 {} 条 '.format(idx+1),path)


    ##4.提取最优路径中路径终点对应的技能
    if len(paths)!=0:
        bestPath=paths[0]

    def match_course_with_singlePath(bestPath):
        #4.1取出最优路径中标签为技能的节点
        bestPath_Nodes=bestPath[0].split('_')
        skillNodes=[]
        for nodeNam in bestPath_Nodes:
            bestPath_node = dao.findNode_with_name((nodeNam))
            bestPath_node_label = list(bestPath_node._labels)[0]
            if 'Skill' in bestPath_node_label:
                skillNodes.append([nodeNam,bestPath_node_label])

        #4.2筛出路径终点的技能标签节点
        tail_skillNodes=[]
        for idx in range(len(skillNodes)):
            nodeNam=skillNodes[idx][0]
            nodeLabel=skillNodes[idx][1]
            cypherStr = 'MATCH p=(:' + tailLabel + '{name:"' + tailNam + '"})-[*..' + str(maxLength) + ']->(:' + nodeLabel + '{name:"' + nodeNam + '"})  RETURN p'
            results = graph.run(cypherStr).to_series()
            if len(results)!=0:
                tail_skillNodes.append([nodeNam,nodeLabel])

        ##5.找出路基路径终点的技能节点对应课程节点
        recommendCourse=[]
        for idx in range(len(tail_skillNodes)):
            nodeNam=tail_skillNodes[idx][0]
            nodeLabel=tail_skillNodes[idx][1]
            cypherStr='MATCH p=(:' + nodeLabel + '{name:"' + nodeNam + '"})-[*]->(n)  RETURN n'
            results = graph.run(cypherStr).to_series()
            for num, path in enumerate(results):
                # 获取路径中的节点和关系
                nodes = path.nodes
                nodeNams = [node['name'] for node in nodes]
                recommendCourse+=nodeNams

        return tail_skillNodes,recommendCourse

    all_skills=[]
    all_recommendCourses=[]
    final_paths=copy.deepcopy(paths)

    for i in final_paths:
        bestPath=i
        tail_skillNodes, recommendCourse=match_course_with_singlePath(bestPath)
        all_skills+=tail_skillNodes
        all_recommendCourses+=recommendCourse
        if findBest:
            break

    all_recommendCourses=list(set(all_recommendCourses))
    print('\n从 {} 到 {} ，\n你需要掌握这些技能 {} ,\n学习这些课程 {}'.format(headNam,tailNam,[node[0] for node in all_skills],all_recommendCourses))
    endT = time.time()
    print('搜索时长：{}S'.format(endT-startT))

    pass





if __name__ == '__main__':

    # findpaths_between2Node_cypher(headNam='预算员', tailNam='造价员', maxLength=10,findBest=True)
    findpaths_between2Node_cypher(headNam='预算员', tailNam='造价员', maxLength=10, findBest=False)
    # findpaths_between2Node_cypher(headNam='预算员', tailNam='《施工组织管理与预算》', maxLength=20)
