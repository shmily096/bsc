class Person:
    def set_name(self,name):
        self.name = name

    def get_name(self):
        return self.name
    
    def greet(self):
        print("hello,world! I'm {}.".format(self.name))

class Bird:
    song ='Squaawk'
    def sing(self):
        print(self.song)

class Secretive:
    def __inaccessible(self):
        print("Bet you can't see me...")

    def accessible(self):
        print("The secret message is:")
        self.__inaccessible()

class MemberCounter:
    members=0
    def init(self):
        MemberCounter.members +=1
        print(self.members)

class Filter:
    def init(self):
        self.blocked =[]
    def filter(self,sequence):
        return [x for x in sequence if x not in self.blocked]
    
class SPAMFilter(Filter):
    def init(self):
        self.blocked= ['SPAM']

s = SPAMFilter()
s.init()
value =['SPAM','JKL','SPAM','YUU','SM']
print(s.filter(value))
s.__class__