import pandas as pd

file_name=r'C:\Python313\testdata\team.xlsx'
df=pd.read_excel(file_name)

# print(df.head()) #查看前5条，括号里可以写明你想看的条数
# print(df.tail()) #查看尾部5条
# print(df.sample(5)) #随机查看5条

# df.shape # (100, 6) 查看行数和列数
# df.info() # 查看索引、数据类型和内存信息
# df.describe() # 查看数值型列的汇总统计
# df.dtypes # 查看各字段类型
# df.axes # 显示数据行和列名
# df.columns # 列名

# print(df.describe()) #总数（count）​、平均数（mean）​、标准差（std）​、最小值（min）​、四分位数和最大值（max）​：

df.set_index('name',inplace=True)
# print(df.head())

# print(df['Q1'][:5])
# print(df[['team','Q1']][:5])
# print(df.loc[:,['team','Q1']][:5]) #df.loc[x, y]是一个非常强大的数据选择函数，其中x代表行，y代表列

# 用指定索引选取
df[df.index == 'Liver'] # 指定姓名

# 用自然索引选择，类似列表的切片
# df[0:3] # 取前三行
# df[0:10:2] # 在前10个中每两个取一个
# df.iloc[:10,:] # 前10个

# df['Q1'].plot() # Q1成绩的折线分布
# 列表
a = [1, 2, 3]
len(a) # 3（元素个数）
max(a) # 3（最大值）
min(a) # 1（最小值）
sum(a) # 6（求和）
a.index(2) # 1（指定元素位置）
a.count(1) # 1（求元素的个数）
for i in a: print(i) # 迭代元素
sorted(a) # 返回一个排序的列表，但不改变原列表
any(a) # True（是否至少有一个元素为真）
all(a) # True（是否所有元素为真）
a.append(4) # a: [1, 2, 3, 4]（增加一个元素）
a.pop() # 每执行一次，删除最后一个元素
a.extend([9,8]) # a: [1, 2, 3, 9, 8]（与其他列表合并）
a.insert(1, 'a') # a: [1, 'a', 2, 3]（在指定索引位插入元素，索引从0开始）
a.remove('a') # 删除第一个指定元素
a.clear() # []（清空）

#元组
x = (1,2,3,4,5)
a, *b = x # a占第一个，剩余的组成列表全给b
# a -> 1
# b -> [2, 3, 4, 5]
# a, b -> (1, [2, 3, 4, 5])

a, *b, c = x # a占第一个，c占最后一个，剩余的组成列表全给b
# a -> 1
# b -> [2, 3, 4]
# c -> 5
# a, b, c -> (1, [2, 3, 4], 5)

#字典
d = {} # 定义空字典
d = dict() # 定义空字典
d = {'a': 1, 'b': 2, 'c': 3}
d = {'a': 1, 'a': 1, 'a': 1} # {'a': 1} key不能重复，重复时取最后一个
d = {'a': 1, 'b': {'x': 3}} # 嵌套字典
d = {'a': [1,2,3], 'b': [4,5,6]} # 嵌套列表

# 以下均可定义如下结果
# {'name': 'Tom', 'age': 18, 'height': 180}
d = dict(name='Tom', age=18, height=180)
d1 = dict([('name', 'Tom'), ('age', 18), ('height', 180)])
d = dict(zip(['name', 'age', 'height'], ['Tom', 18, 180]))

d['name']  # 'Tom'（获取键的值）
d['age'] = 20  # 将age的值更新为20
d['Female'] = 'man'  # 增加属性
d.get('height', 180)  # 180

# 嵌套取值
d = {'a': {'name': 'Tom', 'age':18}, 'b': [4,5,6]}
d['b'][1] # 5
d['a']['age'] # 18

d.pop('name') # 'Tom'（删除指定key）
d.popitem() # 随机删除某一项
del d['name']  # 删除键值对
d.clear()  # 清空字典

# 按类型访问，可迭代
d.keys() # 列出所有键
d.values() # 列出所有值
d.items() # 列出所有键值对元组（k, v）

# 操作
d.setdefault('a', 3) # 插入一个键并给定默认值3，如不指定，则为None
d1.update(dict2) # 将字典dict2的键值对添加到字典dict
# 如果键存在，则返回其对应值；如果键不在字典中，则返回默认值
d.get('math', 100) # 100
d2 = d.copy() # 深拷贝，d变化不影响d2

d = {'a': 1, 'b': 2, 'c': 3}
max(d) # 'c'（最大的键）
min(d) # 'a'（最小的键）
len(d) # 3（字典的长度）
str(d) # "{'a': 1, 'b': 2, 'c': 3}"（字符串形式）
any(d) # True（只要一个键为True）
all(d) # True（所有键都为True）
sorted(d) # ['a', 'b', 'c']（所有键的列表排序）

#集合set
s = {'a', 'b', 'c'}

# 判断是否有某个元素
'a' in s # True

# 添加元素
s.add(2) # {2, 'a', 'b', 'c'}
s.update([1,3,4]) # {1, 2, 3, 4, 'a', 'b', 'c'}

# 删除和清空元素
s.remove('a') # {'b', 'c'}（删除不存在的会报错）
s.discard('3') # 删除一个元素，无则忽略，不报错
s.clear() # set()（清空）

s1 = {1,2,3}
s2 = {2,3,4}

s1 & s2 # {2, 3}（交集）
s1.intersection(s2) # {2, 3}（交集）
s1.intersection_update(s2) # {2, 3}（交集，会覆盖s1）

s1 | s2  # {1, 2, 3, 4}（并集）
s1.union(s2) # {1, 2, 3, 4}（并集）

s1.difference(s2) # {1}（差集）
s1.difference_update(s2) # {1}（差集，会覆盖s1）

s1.symmetric_difference(s2) # {1, 4}（交集之外）

s1.isdisjoint(s2) # False（是否没有交集）
s1.issubset(s2) # False （s1是否是s2的子集）
s1.issuperset(s2) # False（s1是否是s2的超集，即s1是否包含s2中的所有元素）