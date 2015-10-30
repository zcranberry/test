import os
from os.path import join
for root, dirs, files in os.walk("E:\study\Coding\Python\learn_python_hard_way"):
    #for each_file in files:
    #    print join(root, each_file)
    print dirs
    #for each_subdir in dirs:
    #    print join(root, dirs)
