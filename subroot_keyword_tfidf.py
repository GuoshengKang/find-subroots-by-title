#!/usr/bin/python
# -+- coding: utf-8 -+-
import re,os
import sys,math
import string
import datetime
import time
reload(sys)
sys.setdefaultencoding('utf-8')
starttime = datetime.datetime.now()
'''
input:sample_300000_subroot_token
subroot_name,keyword1;keyword2;keyword3;···
output:
keywords文件--所有的关键词
subroots文件--subroot及其关键词的数量(不同的title中的关键词,不去重)
subroot_keyword_tfidf--subroot中每个keyword的tf*idf值
'''
print 'the program start now:',time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()) 
fin_path = os.path.join(os.path.split(os.path.realpath(__file__))[0], "sample_300000_subroot_token.txt")
fin=open(fin_path) #打开文件
keywords=[] #所有关键词列表
subroot2keyword=dict() #{subroot:[keyword1,keyword2,...]}
sentence_num=0
lines=fin.readlines()
subroot_count=dict() #{subroot:num} 每个subroot包含的title数目
print "there are %d lines in the file!!!"%len(lines)
for line in lines:
  sentence_num+=1
  if sentence_num%10000==0:
    print "output:","line %d is processing ···"%sentence_num
  line=unicode(line.strip(),'utf-8')
  # print line
  subroot,token=line.split(unicode(',','utf-8'),1)
  if subroot_count.has_key(subroot):
    subroot_count[subroot]+=1
  else:
    subroot_count[subroot]=1
  keyword_list=token.split(unicode(';','utf-8'))
  keyword_list=list(set(keyword_list)) #jieba分词去重
  keywords.extend(keyword_list)
  keywords=list(set(keywords)) #去重
  if subroot2keyword.has_key(subroot):
    subroot2keyword[subroot].extend(keyword_list)
  else:
    subroot2keyword[subroot]=keyword_list
fin.close()
keyword_num=len(keywords) #所有的关键词数量
subroot_num=len(subroot2keyword) #所有的subroot数量
print 'there are %d senteces!!!' % sentence_num
print 'there are %d keywords in all sentences!!!' % keyword_num
print 'there are %d subroots for all sentences!!!' % subroot_num 

print "now saving keywords to file:",time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()) 
fout_path = os.path.join(os.path.split(os.path.realpath(__file__))[0], "keywords")
fout=open(fout_path,'w') #打开文件
for keyword in keywords:
  fout.write(keyword.encode('utf-8')+'\n')
fout.close()

print "now saving (subroot,keyword_num) to file:",time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()) 
fout_path = os.path.join(os.path.split(os.path.realpath(__file__))[0], "subroots")
fout=open(fout_path,'w') #打开文件
tmp_dict={subroot:len(subroot2keyword[subroot]) for subroot in subroot2keyword}
sorted_tmp_dict=sorted(tmp_dict.iteritems(), key=lambda d:d[1], reverse = True ) #d[0]为key,d[1]为value,返回一个元组列表
for subroot,keyword_num in sorted_tmp_dict:
  line=subroot+','+str(keyword_num)
  fout.write(line.encode('utf-8')+'\n')
fout.close() 
print time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()) 

print "now calculating idf for all keywords:",time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()) 
keyword2idf=dict()
for keyword in keywords:
  accur_num=0
  for subroot in subroot2keyword:
    if keyword in subroot2keyword[subroot]:
      accur_num+=1
  keyword2idf[keyword]=math.log(float(subroot_num)/(accur_num+1))

print "now calculating tf_idf for subroots:",time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()) 
fout_path = os.path.join(os.path.split(os.path.realpath(__file__))[0], "subroot_keyword_tfidf")
fout=open(fout_path,'w') #打开文件
subroot_no=0;
for subroot in subroot2keyword:
  subroot_no+=1
  keyword2tfidf=dict()
  keywords=set(subroot2keyword[subroot]) #subroot关联的keywords,不重复
  length=len(subroot2keyword[subroot]) #subroot关联的keyword数量,包括重复的
  print "subroot_%d: %s(keywords:%d) is processing... elapsed time %s ..."%(subroot_no,subroot.encode('utf-8'),len(keywords),(datetime.datetime.now() - starttime))
  for keyword in keywords:
    # tf=float(subroot2keyword[subroot].count(keyword))/length
    tf=float(subroot2keyword[subroot].count(keyword))/subroot_count[subroot]
    idf=keyword2idf[keyword]
    tf_idf=tf*idf
    keyword2tfidf[keyword]=tf_idf
  sorted_dict=sorted(keyword2tfidf.iteritems(), key=lambda d:d[1], reverse = True ) #d[0]为key,d[1]为value,返回一个元组列表
  # need_num=int(len(sorted_dict)*0.5) #50% 取一半重要的keyword
  # if len(keywords)<200: #至多取200个keyword
  #   need_num=length
  # else:
  #   need_num=200
  # for keyword,tfidf in sorted_dict[:200]:
  for keyword,tfidf in sorted_dict:
    new_line=','.join([subroot,keyword,str(tfidf)])
    fout.write(new_line.encode('utf-8')+'\n')
fout.close()

print time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()) 