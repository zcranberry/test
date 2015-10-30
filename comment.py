def multipleline_comment_filter(lines):
    cursor_begin = 0
    comment_begin = 0
    comment_end = 0
    out_str = ''
    comment_begin = lines.find('/*', cursor_begin)
    while(comment_begin != -1):
        out_str += lines[cursor_begin:comment_begin]
        comment_end = lines.find('*/',comment_begin) + 2
        cursor_begin = comment_end
        comment_begin = lines.find('/*', cursor_begin)
    out_str += lines[cursor_begin:]
    return out_str


def singleline_comment_filter(line):
    #print line.find('--')
    if line.find('--') != -1 :
        return line[:line.find('--')]
    else:
        return line

if __name__ == '__main__':
    f = open('ACRM_ACRMBB_HXKHQXFXYBB_JGBNC.sql', 'r')
    content = f.read()
    multi_comment_striped = multipleline_comment_filter(content)
    #print multi_comment_striped
    lines = multi_comment_striped.split('\n')
    for line in lines:
       # print '1', line
        single_comment_stripped = singleline_comment_filter(line)
        if single_comment_stripped.strip() != '':
            print  single_comment_stripped

