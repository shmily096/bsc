import os
import mpmath
import json

# num=math.pow(100,100)
# pi_str=int(math.pi*num)

# mpmath.mp.dps=1000000
# pi_str=str(mpmath.mp.pi)
#print(pi_str)

current_pwd=os.getcwd()
print(current_pwd)
# pi=r'C:\Python313\testdata\pi1.txt'
str_value=r'C:\Python313\testdata\古文观止简.txt'
str_comment=r'C:\Python313\testdata\古文观止简目录.txt'

# with open(pi,'w') as file_object:
#     file_object.write(pi_str)


# with open(pi) as file_object:
#     contents=file_object.read()
#     print(contents.rstrip())

def comment_name(str_value):
    listnum=['一','二','三','四','五','六','七','八','九','十','十一','十二']
    file_object=open(str_value,encoding='utf-8')
    birthday='卷'
    pi_string=''
    lines=file_object.readlines()
    for line in lines:
        if line[0]==birthday and line[1] in listnum:
            print(line)
    #         pi_string+=line
    # with open(str_comment,'w',encoding='utf-8') as writeobject:
    #     writeobject.write(pi_string)

#comment_name(str_value)

def count_words(filename):
    fname=filename.split("\\")[-1].replace('.txt','')
    try:
        with open(filename,encoding='utf-8') as f_obj:
            contents=f_obj.read()
    except FileNotFoundError:
        # msg="Sorry,the file "+fname+" does not exist."
        # print(msg)
        pass
    else:
        words=contents.split()
        num_words=len(words)
        print("The file "+fname+" has about " +str(num_words)+" words")
filename=r'C:\Python313\testdata\Christmas in Storyland.txt'
# print(filename.split("\\")[-1].replace('.txt',''))
# count_words(filename)

filenames=['ss.txt',r'C:\Python313\testdata\Christmas in Storyland.txt','tt.txt']

# for f in filenames:
#     count_words(f)

filejson=r'C:\Python313\testdata\numbers.json'
numbers=[2,3,4,5,6,7,12,15]
# with open(filejson,'w') as j_obj:
#     json.dump(numbers,j_obj)
with open(filejson) as j_obj:
    num=json.load(j_obj)
print(num)