import matplotlib.pyplot as plt
x=[1,4,9,16,25]
y=[1,2,3,4,5]
class DataGraph():
    def __init__(self,x,y):
        self.x=x
        self.y=y
        plt.title("Square Numbers",fontsize=24)
        plt.xlabel("Value",fontsize=14)
        plt.ylabel("Square of Value",fontsize=14)
        plt.tick_params(axis='both',labelsize=14)

    def line_chart(self):
        plt.plot(self.x,self.y,linewidth=5)
        plt.show()
    def scatter_chart(self):
        plt.scatter(self.x,self.y,s=40)
        plt.axis([0,1000,0,1000000])
        plt.show()

# th=DataGraph(x,y)
# th.line_chart()

x_values=list(range(1,1001))
y_values=[x**2 for x in x_values]
scatter=DataGraph(x_values,y_values)
scatter.scatter_chart()




