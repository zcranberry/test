#encoding:utf-8
def singleline_comment_filter(line):
    return line[:line.find('--')]

def multiline_comment_filter(lines):
    begin_pos = lines.find('/*')
    #print begin_pos
    #print type(begin_pos)
    end_pos = lines.find('*/')
    #print end_pos
    #print type(end_pos)
    return lines[begin_pos: end_pos + 2]


if __name__ == '__main__':
    #long_str = '''abcdefg -- 123456
    #hijkmnl --7890'''
    #print long_str
    #print long_str[5:]
    #print singleline_comment_filter(long_str)
    import codecs
    f = open("ACRM_ACRMBB_HXKHQXFXYBB_JGBNC.sql",'r')
    #for line in f.read():
    #    #print line.decode('gbk').encode('gbk')
    #    #print singleline_comment_filter(line)
    #    #print multiline_comment_filter(line)
    #    print line[0:200]
    lines = f.read()
    print multiline_comment_filter(lines)


