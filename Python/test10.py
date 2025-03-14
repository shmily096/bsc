
class MuffledCalculator:
    muffled = False
    def calc(self,expr):
        try:
            return eval(expr)
        # except ZeroDivisionError:
        #     if self.muffled:
        #         print("Division by Zero is illegal")
        #     else:
        #         raise ValueError from None
        # except TypeError:
        #     print("That wasn't a number, was if?")
        except(ZeroDivisionError,TypeError,NameError) as e:
            print("Your numbers were bogus...")
            print(e)
        else:
            print("That went well")

class FooBar:
    def __init__(self,value=42):
        self.somevar = value
        print(self.somevar)

# f =FooBar("This is a constructor argument")

class Bird:
    def __init__(self):
        self.hungry = True
    def eat(self):
        if self.hungry:
            print("Aaaah...")
            self.hungry =False
        else:
            print("no,thanks!")

class SongBird(Bird):
    def __init__(self):
        # Bird.__init__(self)  # 调用未关联的超类构造函数
        super().__init__()     # 使用函数super
        self.sound ="Squawk!"
    def sing(self):
        print(self.sound)

# sb = SongBird()
# sb.sing()
# sb.eat()
# sb.eat()

class CounterList(list):
    def __init__(self,*args):
        super().__init__(*args)
        self.counter =0
    def __getitem__(self,index):
        self.counter +=1
        return super(CounterList,self).__getitem__(index)
    
c1=CounterList(range(8))
print(c1)
print(c1[4])
print(c1[0])
print(c1.counter)