from GraphConstruction.function2pyneo import Neo4jDao
import time
import pandas as pd
from tqdm import tqdm
'''
本页为知识图谱节点，边更新代码，包括直接对某名字节点属性更新、某边属性更新；
后面对整个关系的xlsx文件的weight进行计算

'''


def update_NodeProperty_with_name(importanceFile=r'E:\PythonProject\zhilianDataProcess\importance_File.csv'):
    importanceTable = pd.read_csv(importanceFile)
    dao= Neo4jDao(username='neo4j', password='123456')
    colNames = importanceTable.columns.to_list()
    for idx in tqdm(range(len(importanceTable))) :
        nodeNam=importanceTable[colNames[0]][idx]
        importance = importanceTable[colNames[2]][idx]

        # 查找出对应属性的节点
        goal_node =dao.findNode_with_name(nodeNam)

        #更新节点属性
        goal_node.update({'weight':importance})
        dao.my_graph.push(goal_node)
        print(' update node weight: {}'.format(importance))

def update_RelationProperty_with_name(importanceFile=r'E:\PythonProject\zhilianDataProcess\importance_File.csv'):
    importanceTable = pd.read_csv(importanceFile)
    dao= Neo4jDao(username='neo4j', password='123456')
    colNames = importanceTable.columns.to_list()
    for idx in tqdm(range(len(importanceTable))) :

        head_node_name='基础技能'
        tail_node_name=importanceTable[colNames[0]][idx]
        importance = importanceTable[colNames[2]][idx]

        # 查找出关系的头尾节点
        head_node=dao.findNode_with_name(head_node_name)
        tail_node =dao.findNode_with_name(tail_node_name)

        #根据头尾节点寻找关系
        relationship =dao.findRelation(head_node,tail_node)

        #更新关系属性
        relationship.update({'weight':importance})
        dao.my_graph.push(relationship)

        print(' update relationship weight: {}'.format(importance))


def update_relationXlsxFile(entityXlsxFile=r'',relationXlsxFile = r'',importanceFile = r''):






    ###############读取实体表并构建实体标签字典########################

    entityTable=pd.read_excel(entityXlsxFile)
    labels=entityTable.columns.to_list()
    ##1. 构建实体标签词典
    entityLabelDict={}
    for i in tqdm(labels) :
        entitys=entityTable[i]
        entitys=list(set(entitys))
        if '《》' in entitys:
            entitys.remove('《》')

        for j in entitys :
            if j==j:
                entityLabelDict[j]={'label':i}



    ##2. 读取权重文件存入字典
    importanceDic={}
    importanceTable = pd.read_csv(importanceFile)
    colNames = importanceTable.columns.to_list()
    rowLength=len(importanceTable[colNames[0]])
    for idx in range(rowLength):
        nodeNam=importanceTable[colNames[0]][idx]
        importanceVal=importanceTable[colNames[2]][idx]
        if nodeNam==nodeNam and importanceVal==importanceVal:
            importanceDic[nodeNam]={'Weight':float(importanceVal)}



    ##3.读取关系excel文件，并进行Weight更新
    relationTable = pd.read_excel(relationXlsxFile)
    colNames= relationTable.columns.to_list()
    rowLength = len(relationTable[colNames[0]])
    for idx in tqdm(range(rowLength)):
        headVal,tailVal, WeightVal = relationTable[colNames[0]][idx], relationTable[colNames[2]][idx], relationTable[colNames[3]][idx]
        tailVal=tailVal.strip()
        if '虚节点' in entityLabelDict[tailVal]['label']:
            relationTable[colNames[3]][idx]=0.0
        else:
            relationTable[colNames[3]][idx]=importanceDic[tailVal]['Weight']

    ##4. 将更新后的关系dataFrame存入覆盖原关系Xlsx文件
    relationTable.to_excel(relationXlsxFile,index=False)
if __name__ == '__main__':
    # update_NodeProperty_with_name() #根据节点名称更新节点属性
    # update_RelationProperty_with_name() #根据头尾节点名称更新关系属性
    update_relationXlsxFile(entityXlsxFile=r'C:\Users\glodon\Desktop\知识图谱搭建\简易版图谱\预算员简易图谱\预算员相关实体表1008.xlsx',
                            relationXlsxFile = r'C:\Users\glodon\Desktop\知识图谱搭建\简易版图谱\预算员简易图谱\预算员联系表1008.xlsx',
                            importanceFile = r'C:\Users\glodon\Desktop\知识图谱搭建\简易版图谱\预算员简易图谱\importance_File.csv') #根据权重文件更新关系表weight属性
