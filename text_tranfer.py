f = open('ACRM_ACRMBB_HXKHQXFXYBB_JGBNC.sql')
content = f.read()
trans = content.replace('\'',r'\'')
print 'insert into xxx values E\'' + trans + '\''
