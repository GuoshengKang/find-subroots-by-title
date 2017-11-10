#!/usr/bin/python
# coding=utf-8

import sys
import os
import pprint
import json
from jieba_cut import *
from collections import OrderedDict
reload(sys)
sys.setdefaultencoding('utf8')

try:
  import cPickle as pickle
except:
  import pickle

# 将subroot_keyword_tfidf.txt文件存储为pickle文件,提高文件的读取速度
# tfidf_path=os.path.join(os.path.split(os.path.realpath(__file__))[0], 'subroot_keyword_tfidf')
# fin=open(tfidf_path) #打开文件
# subroot2keyword=dict() #{subroot1:{keyword1:tfidf1,keyword2:tfidf2,...},}
# for line in fin:
#     line=unicode(line.strip(),'utf-8')
#     subroot,keyword,tfidf=line.split(unicode(',','utf-8'))
#     if subroot2keyword.has_key(subroot):
#       subroot2keyword[subroot][keyword]=float(tfidf)
#     else:
#       subroot2keyword[subroot]={}
#       subroot2keyword[subroot][keyword]=float(tfidf)
# fin.close()
# pkl_data_path=os.path.join(os.path.split(os.path.realpath(__file__))[0], "subroot2keyword.pkl")
# output = open(pkl_data_path, 'wb')
# pickle.dump(subroot2keyword, output)
# output.close()

def find_top_k_subroots(k,title):
  '''
  input:
  --k:返回的subroot数目
  --title:商品或优惠券的title,待分词的字符串可以是unicode或UTF-8字符串
  output:
  --json格式字符串,unicode编码
  e.g.,{"无线高清": 7.17356076646, "服饰箱包定制": 5.284682918428801, "U盘": 4.363466010542201}
  '''
  pkl_data_path=os.path.join(os.path.split(os.path.realpath(__file__))[0], "subroot2keyword.pkl")
  pkl_data_file= file(pkl_data_path, 'rb') 
  subroot2keyword=pickle.load(pkl_data_file)
  pkl_data_file.close()

  subroot_num=len(subroot2keyword)
  if k>subroot_num:
    k=subroot_num

  split_result=seg_sentence(title) #待分词的字符串可以是unicode或UTF-8字符串,输出unicode编码字符串
  split_list=split_result.split(unicode('|','utf-8'))
  subroot2score=dict() #{subroot1:score1,subroot2:score2,···}
  for subroot in subroot2keyword:
    score=0.0
    for keyword in split_list:
      score=score+subroot2keyword[subroot].get(keyword,0.0)
    subroot2score[subroot]=score

  ranked_subroot2score=sorted(subroot2score.iteritems(), key=lambda d:d[1], reverse = True ) #d[0]为key,d[1]为value,返回一个元组列表
  top_k_subroots=OrderedDict() #定义有序字典,按键的插入顺序排序
  for (subroot,score) in ranked_subroot2score[:k]:
    top_k_subroots[subroot]=score
  top_k_result = json.dumps(top_k_subroots, ensure_ascii=False)
  return top_k_result


# test
title='【国际狂欢价】【新品】medicura壳聚糖胶囊排油丸吸油丸60粒减肥瘦身'
top_k_subroots=find_top_k_subroots(3,title)
subroots_dict=OrderedDict()
print top_k_subroots