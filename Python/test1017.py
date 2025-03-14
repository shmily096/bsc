from scipy import stats

volumns =[4988,5006,5021,4923,4947,4896,5104,4992,5070,5009,4892,4997]

TtestResult= stats.ttest_1samp(a=volumns,popmean=5000)
#TtestResult(statistic=np.float64(-0.6941390148304715), pvalue=np.float64(0.5019915686890506), df=np.int64(11))
print(TtestResult[0])