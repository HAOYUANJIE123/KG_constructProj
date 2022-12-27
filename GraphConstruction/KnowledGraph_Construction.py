from GraphConstruction.function2pyneo import Neo4jDao
import time
import pandas as pd
from tqdm import tqdm
import os
import numpy as np
'''
知识图谱构建部分，分为单个职业节点关系文件构建、merge节点及关系文件构建

'''

#构造neo4j中节点标签（node label），中英文对应字典
label_CH2ZH={
    '职业':'Profession',
    '专业要求':'MajorRequire',
    '学历要求':'Education',
    '岗位职责': 'Responsibility',
    '基础理论知识需求':'BaseTheoryKnowledge',
    '基础理论课程': 'BaseTheoryCourse',
    '基础技能需求': 'BaseSkill',
    '基础技能课程': 'BaseSkillCourse',
    '职业素养': 'ProfessionQuality',
    '证书要求': 'ZhengShu',
    '暖通方向理论': 'NTongTheory',
    '暖通方向理论课程': 'NTongTheoryCourse',
    '暖通方向技能': 'NTongSkill',
    '暖通方向技能课程': 'NTongSkillCourse',
    '安装方向理论': 'AnZhuangTheory',
    '安装方向理论课程': 'AnZhuangTheoryCourse',
    '安装方向技能': 'AnZhuangSkill',
    '安装方向技能课程': 'AnZhuangSkillCourse',
    '土建方向理论': 'TuJianTheory',
    '土建方向理论课程': 'TuJianTheoryCourse',
    '土建方向技能': 'TuJianSkill',
    '土建方向技能课程': 'TuJianSkillCourse',
    '成本方向理论': 'ChengBenTheory',
    '成本方向理论课程': 'ChengBenTheoryCourse',
    '成本方向技能': 'ChengBenSkill',
    '成本方向技能课程': 'ChengBenSkillCourse',
    '装饰方向理论': 'DecorationTheory',
    '装饰方向理论课程': 'DecorationTheoryCourse',
    '装饰方向技能': 'DecorationSkill',
    '装饰方向技能课程': 'DecorationSkillCourse',
    'first虚节点':'firstVirtualNode',
    'second虚节点':'secondVirtualNode',
    '小方向':'SmallDirection'
}
def KnowledgeGraph_Construction(entityXlsxFile='',relationXlsxFile=''):
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
                entityLabelDict[str(j).strip()]={'label':label_CH2ZH[i]}

    for key in entityLabelDict:
        dao.createNode(label=entityLabelDict[key]['label'], properties={'name': str(key).strip()})
        nodecount+=1

    print('知识图谱节点创造完成,实体节点数 {}个，实体标签数 {}个'.format(nodecount,len(label_CH2ZH)))

    #### 读取关系表并构建关系##############
    relationTable = pd.read_excel(relationXlsxFile)
    rellabels = relationTable.columns.to_list()
    rel_num=0
    for k in tqdm(range(len(relationTable[rellabels[0]]))) :
        rel_num+=1
        headVal,relVal, tailVal,weightVal=relationTable[rellabels[0]][k],relationTable[rellabels[1]][k],relationTable[rellabels[2]][k],relationTable[rellabels[3]][k]
        if headVal == headVal and relVal == relVal and tailVal == tailVal:
            headVal, relVal, tailVal = headVal.strip(), relVal.strip(), tailVal.strip()
            relationPro={'weight':weightVal}
            dao.createRelation_with_properties(startNodeLabel=entityLabelDict[headVal]['label'], startNodeProperties={'name': headVal},endNodeLabel=entityLabelDict[tailVal]['label'], endNodeProperties={'name': tailVal},
                                               relation_type=relVal,relation_properties=relationPro)
    print('知识图谱关系创造完成, 边数量 {} 个'.format(rel_num))


    end_time=time.time()
    print('构造时长为 {}s'.format(end_time-start_time))

def nodeANDrelation_fileMerge(nodeDir=r'C:\Users\glodon\Desktop\知识图谱搭建\nodeDir',relationDir=r'C:\Users\glodon\Desktop\知识图谱搭建\relationDir'):


    nodeFiles=os.listdir(nodeDir)
    relationFiles=os.listdir(relationDir)

    ##1. 整合节点实体xlsx文件
    entityLabelDict = {}
    for idx, nodeFile in enumerate(nodeFiles):
        print('第 {} 个文件开始处理， {}'.format(idx+1,nodeFile))
        nodeFilePath=os.path.join(nodeDir,nodeFile)
        entityTable = pd.read_excel(nodeFilePath)
        labels = entityTable.columns.to_list()
        # 构建实体标签词典
        for i in tqdm(labels):
            entitys = entityTable[i]
            entitys = list(set(entitys))
            if '《》' in entitys:
                entitys.remove('《》')

            for j in entitys:
                if j == j:
                    entityLabelDict[str(j).strip()] = {'label': label_CH2ZH[i]}
        print('该文件节点数量为 {} \n'.format(len(entityLabelDict.keys())))
    np.save('./constructionData/nodeMerge.npy',entityLabelDict)

    ##2. 整合关系xlsx文件
    dfList=[]
    for idx, relationFile in enumerate(relationFiles):
        print('第 {} 个文件开始处理， {}'.format(idx + 1, relationFile))
        relationFilePath = os.path.join(relationDir, relationFile)
        relationTable=pd.read_excel(relationFilePath)
        dfList.append(relationTable)
        print('该文件关系数量为 {} \n'.format(len(relationTable['Weight'])))

    merge_df=pd.concat(dfList)
    merge_df.to_excel('./constructionData/relationMerge.xlsx',index=False)
    print('节点关系合并完成！！！')
    pass

def KnowledgeGraph_Construction_with_MergeFile(nodeMergeFile=r'',relationMergeFile=r''):
    start_time=time.time()
    dao = Neo4jDao(username='neo4j', password='123456')
    dao.deleteall()

    relationXlsxFile=relationMergeFile


    ###############读取实体表并构建实体节点########################
    #构建实体标签词典
    entityLabelDict=np.load(nodeMergeFile,allow_pickle=True).item()

    nodecount=0
    for key in tqdm(entityLabelDict):
        dao.createNode(label=entityLabelDict[key]['label'], properties={'name': str(key).strip()})
        nodecount+=1

    print('知识图谱节点创造完成,实体节点数 {}个，实体标签数 {}个'.format(nodecount,len(label_CH2ZH)))

    #### 读取关系表并构建关系##############
    relationTable = pd.read_excel(relationXlsxFile)
    rellabels = relationTable.columns.to_list()
    rel_num=0
    for k in tqdm(range(len(relationTable[rellabels[0]]))) :
        rel_num+=1
        headVal,relVal, tailVal,weightVal=relationTable[rellabels[0]][k],relationTable[rellabels[1]][k],relationTable[rellabels[2]][k],relationTable[rellabels[3]][k]
        if headVal == headVal and relVal == relVal and tailVal == tailVal:
            headVal, relVal, tailVal = headVal.strip(), relVal.strip(), tailVal.strip()
            relationPro={'weight':weightVal}
            dao.createRelation_with_properties(startNodeLabel=entityLabelDict[headVal]['label'], startNodeProperties={'name': headVal},endNodeLabel=entityLabelDict[tailVal]['label'], endNodeProperties={'name': tailVal},
                                               relation_type=relVal,relation_properties=relationPro)
    print('知识图谱关系创造完成, 边数量 {} 个'.format(rel_num))


    end_time=time.time()
    print('构造时长为 {}s'.format(end_time-start_time))



if __name__ == '__main__':

    ###根据单一节点，关系表构造知识图谱过程
    # KnowledgeGraph_Construction(entityXlsxFile=r'C:\Users\glodon\Desktop\知识图谱搭建\简易版图谱\预算员简易图谱\预算员相关实体表1008.xlsx',
    #                         relationXlsxFile = r'C:\Users\glodon\Desktop\知识图谱搭建\简易版图谱\预算员简易图谱\预算员联系表1008.xlsx')

    ###根据merge节点，关系表构造知识图谱过程
    nodeANDrelation_fileMerge(nodeDir=r'C:\Users\glodon\Desktop\知识图谱搭建\nodeDir',relationDir=r'C:\Users\glodon\Desktop\知识图谱搭建\relationDir')
    KnowledgeGraph_Construction_with_MergeFile(nodeMergeFile=r'./constructionData/nodeMerge.npy', relationMergeFile=r'./constructionData/relationMerge.xlsx')


