#encoding:utf-8
f_read = open("abc.txt", "r") # 一二三四五六七八
lines = f_read.readlines()
for line in lines:
    print line[0:4]       #一二，因为中文每个字符占两位
f_read.close()
