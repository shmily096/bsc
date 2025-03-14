import opencc

tang3=r'C:\Python313\testdata\千字文.txt'
tang31=r'C:\Python313\testdata\千字文简.txt'

cc=opencc.OpenCC('t2s')
t2stang=cc.convert('孤鴻海上來，池潢不敢顧；')
print(t2stang)

with open(tang3,encoding='utf-8') as file_object:
    contents=file_object.read()
    #print(contents.rstrip())     
t2stang=cc.convert(contents)

with open(tang31,'w',encoding='utf-8') as file_object:

    file_object.write(t2stang)