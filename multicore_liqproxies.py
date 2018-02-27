from sqlplus import *
import statistics as st
import numpy as np


def addmonth(date, n):
    return dmath(date, f'{n} months', '%Y-%m')


setdir('data')


def stoll_cov(prices):
    dprices = []
    for p0, p1 in zip(prices, prices[1:]):
        if isnum(p0, p1):
            dprices.append(p1 - p0)
    # np.cov 는 covariance matrix 를 계산
    return np.cov([dprices[1:], dprices[:-1]])[0, 1] if len(dprices) > 2 else ''


def fn(dbfile, nmonths):
    with connect(dbfile) as c:
        def gen():
            # group 을 하면 id 랑 yyyymm 이 같은 애들을 덩어리로 만들고 overlap 이 nmonths 니까 그런 덩어리들을
            # 12 개씩 묶어서 flatten(이건 자동으로 됨) 한 다음 끄집어 내는거임.
            for nmonth in nmonths:
                for rs in c.fetch("daily", group="id, yyyymm", overlap=(nmonth, 1)):
                    if rs[0].id == rs[-1].id and addmonth(rs[0].yyyymm, nmonth - 1) == rs[-1].yyyymm:
                        r = Row()
                        r.yyyymm = rs[-1].yyyymm
                        r.id = rs[-1].id
                        
                        xs = []
                        for r1 in rs.isnum('ret, tvol'):
                            if r1.tvol > 0:
                                xs.append(abs(r1.ret) * 10_000_000 / r1.tvol)
                        r.amihud = st.mean(xs) if xs else ''

                        r.zerofreq = len(rs.where(lambda r: isnum(r.ret) and r.ret == 0))
                        r.stoll_cov = stoll_cov(rs['prc'])
                        r.nmonth = nmonth
                        # missing 이 아닌 것들의 개수
                        r.n = len(rs.isnum('ret'))
                        print(r)
                        yield r
        c.insert(gen(), 'liqproxies')



if __name__ == "__main__":
    def yyyymm(r):
        "1981-01-21 => 1981-01 column 하나 추가"
        r.yyyymm = dconv(r.date, '%Y-%m-%d', '%Y-%m')
        return r

    with connect('db.db') as c:
        # c.load('daily.csv', fn=yyyymm)
        # c.pwork(fn, 'daily', [[1], [3], [6], [12]])
        # c.create('select * from daily where id="A005930"', 'daily1')
        c.pwork(fn, 'daily', [[1], [3], [6], [12]])

   

