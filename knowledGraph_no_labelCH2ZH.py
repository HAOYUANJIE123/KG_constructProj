from GraphConstruction.function2pyneo import Neo4jDao
import time
import pandas as pd
from tqdm import tqdm


#构造neo4j中节点标签（node label），中英文对应字典

def KnowledgeGraph_Construction(entityXlsxFile=r'',relationXlsxFile=r''):
    start_time=time.time()
    dao = Neo4jDao(username='neo4j', password='123456')
    dao.deleteall()

    ###############读取实体表并构建实体节点########################

    entityTable=pd.read_excel(entityXlsxFile)
    labels=entityTable.columns.to_list()
    #构建实体标签词典
    entityLabelDict={}

    nodecount=0
    for i in tqdm(labels) :
        entitys=entityTable[i]
        entitys=list(set(entitys))
        if '《》' in entitys:
            entitys.remove('《》')

        for j in entitys :
            if j==j:
                entityLabelDict[j]={'label':str(i)}

    for key in tqdm(entityLabelDict):
        dao.createNode(label=entityLabelDict[key]['label'], properties={'name': key.strip()})
        nodecount+=1

    print('知识图谱节点创造完成,实体节点数 {}个'.format(nodecount))

    #### 读取关系表并构建关系##############
    relationTable = pd.read_excel(relationXlsxFile)
    rellabels = relationTable.columns.to_list()
    rel_num=0
    for k in tqdm(range(len(relationTable[rellabels[0]]))) :
        rel_num+=1
        headVal,relVal, tailVal=relationTable[rellabels[0]][k],relationTable[rellabels[1]][k],relationTable[rellabels[2]][k]
        if headVal==headVal and relVal==relVal and tailVal==tailVal:
            headVal,relVal, tailVal=headVal.strip(),relVal.strip(), tailVal.strip()

            dao.createRelation_with_properties(startNodeLabel=entityLabelDict[headVal]['label'], startNodeProperties={'name': headVal},endNodeLabel=entityLabelDict[tailVal]['label'], endNodeProperties={'name': tailVal},relation_type=relVal)
    print('知识图谱关系创造完成, 边数量 {} 个'.format(rel_num))


    end_time=time.time()
    print('构造时长为 {}s'.format(end_time-start_time))




if __name__ == '__main__':
    KnowledgeGraph_Construction(entityXlsxFile=r'C:\Users\glodon\Desktop\培训整理\知识整理\知识实体.xlsx',relationXlsxFile=r'C:\Users\glodon\Desktop\培训整理\知识整理\知识联系.xlsx')
    # KnowledgeGraph_Construction(entityXlsxFile=r'C:\Users\glodon\Desktop\培训整理\分工图谱\分工实体0926.xlsx', relationXlsxFile=r'C:\Users\glodon\Desktop\培训整理\分工图谱\分工联系0926.xlsx')


