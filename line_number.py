#def parse_lines(lines):
#
#
#
#test_str = '''
#select * from a; select * from 
#bb;
#select * from c;
#'''
#if __name__ == '__main__':
#    print test_str

f = open('test.sql')

lines = f.readlines()
sql = ''
begin_pos = 0
for i in xrange(0, len(lines)):
    line = lines[i]
    semi_pos = line.find(';', begin_pos)
    while (semi_pos != -1 ):
        sql += line[begin_pos:semi_pos + 1]
        print i, sql
        sql = ''
        begin_pos = semi_pos + 1
        semi_pos = line.find(';', begin_pos)
    sql += line[begin_pos:]
    begin_pos = 0



