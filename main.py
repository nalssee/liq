# ==============================================================================
# Liquidity vs Financial Constraint
# ==============================================================================

#%%
from itertools import product
from sqlplus import *
import statistics as st
import numpy as np
import math
from scipy.stats import ttest_1samp
from contextlib import redirect_stdout


def addmonth(date, n):
    return dmath(date, f'{n} months', '%Y-%m')


def stars(pval):
    if pval <= 0.01:
        return "***"
    elif pval <= 0.05:
        return "**"
    elif pval <= 0.10:
        return "*"
    return ""


def diff(high, low):
    return [a - b for a, b in zip(high, low)]


def ttest(seq, n=3):
    tval, pval = ttest_1samp(seq, 0.0)
    return f'{round(st.mean(seq), n)}{stars(pval)}', round(tval, n)


setdir('data')
print('ready...')


# ==============================================================================
# File Loading
# ==============================================================================

#%%
with connect('db.db') as c:
    # 파일을 로딩할때 살짝 바꿔서 로딩할수 있음
    def yyyymm(r):
        "1981-01-21 => 1981-01 column 하나 추가"
        r.yyyymm = dconv(r.date, '%Y-%m-%d', '%Y-%m')
        return r

    def build_yyyymm(r):
        "year 하고 month 붙이기 예를들면 1999, 3 => 1999-03"
        r.yyyymm = str(r.year) + '-' + str(r.month).zfill(2)
        return r

    c.load('daily.csv', fn=yyyymm)
    c.load('comp_kor.csv', fn=build_yyyymm)
    c.load('prc.csv', fn=yyyymm)
    c.load('shrout.csv', fn=yyyymm)
    c.load('inflation.csv')
    c.load('ret.csv', fn=yyyymm)
    c.load('mkt500.csv', fn=yyyymm)
    c.load('exchcd.csv', fn=yyyymm)


# ==============================================================================
# Liquidity measures: Amihud, Zerofreq, Stoll(1989)
# ==============================================================================
# 이 작업은 좀 오래걸리니까 컴퓨터가 4개 이상의 코어를 갖고 있다면
# multi process 를 활용하는 것을 고려해 볼만함 (single core 로 3시간 걸린거 같은데)
#%%
def stoll_cov(prices):
    dprices = []
    for p0, p1 in zip(prices, prices[1:]):
        if isnum(p0, p1):
            dprices.append(p1 - p0)
    # np.cov 는 covariance matrix 를 계산
    return np.cov([dprices[1:], dprices[:-1]])[0, 1] if len(dprices) > 2 else ''


with connect('db.db') as c:
    nmonths = [1, 3, 6, 12]
    # nmonths = [12]
    # ins 를 쓰면 코드는 이쁜데 너무 느려서 insert 를 써야함
    def gen():
        for rs in c.fetch("daily", group="id"):
            print(rs[0].id)
            for nmonth in nmonths:
                for rs1 in rs.overlap(nmonth, 1, 'yyyymm'):
                    if addmonth(rs1[0].yyyymm, nmonth - 1) == rs1[-1].yyyymm:
                        r = Row()
                        r.yyyymm = rs1[-1].yyyymm
                        r.id = rs1[-1].id
                        xs = []
                        for r0 in rs1.isnum('ret, tvol'):
                            if r0.tvol > 0:
                                xs.append(abs(r0.ret) * 10_000_000 / r0.tvol)
                        r.amihud = st.mean(xs) if xs else ''
                        r.zerofreq = len(rs1.where(lambda r: isnum(r.ret) and r.ret == 0))
                        r.stoll_cov = stoll_cov(rs1['prc'])
                        r.nmonth = nmonth
                        # missing 이 아닌 것들의 개수
                        r.n = len(rs1.isnum('ret'))
                        yield r
    c.insert(gen(), 'liqproxies')


# ==============================================================================
# Financial Contraint measures
# ==============================================================================

#%%
# 모든 fiscal month 가 12 월이라고 간주하고 진행하면, (12월이 아닌 경우는 9% 정도)
# (나중에 바꿔볼지도 모르지만, kz index 계산과정에서 12 월 size 랑 매칭하는 걸 고려할때, 생각해 볼만함)


# 12 월의 size 가 필요하니까
# TODO: 이상하게 prc 파일이 shrout 보다 훨씬 크다. 둘다 코스닥 포함이던데...체크
with connect('db.db') as c:
    c.join(
        ['prc', '*', 'yyyymm, id'],
        ['shrout', 'shrout', 'yyyymm, id'],
        name='temp'
    )

    c.drop('size')
    for r in c.fetch('temp', where='isnum(prc, shrout)'):
        if r.yyyymm[5:] == '12':
            # 금액인 경우 단위가 다 천원이더라구
            r.size = (r.prc * r.shrout) / 1000
            r.year = r.yyyymm[0:4]
            c.ins(r, 'size')

#%%

# 전기 total asset 이랑 size 가 필요하니까
with connect('db.db') as c:
    c.create("select ta, ppe, id, year + 1 as year1 from comp_kor", "temp")

    c.join(
        ["comp_kor", "*", "id, year"],
        # 혹시 몰라서 ppe 도
        ["temp", "ta as ta1, ppe as ppe1", "id, year1"],
        ['size', 'size', "id, year"],
        name='comp'
    )
    # inflation 을 붙이고
    c.join(
        ['comp', '*', 'year'],
        ['inflation', 'inflation', 'year']
    )


#%%

with connect('db.db') as c:
    c.drop('constraints')
    for r in c.fetch('comp', where="""
        isnum(oi, noi, dep, noc, div, ta1, ta, te, prefstock, size, tl, cash, csec, estdate)
        and ta1 > 0 and te > 0
        """):
        # cash flow
        cf = (r.oi + r.noi + r.dep - r.noc - r.div) / r.ta1
        # cash holdings
        ch  = (r.cash + r.csec) / r.ta1
        tobinq = (r.ta - (r.te - r.prefstock) + r.size) / r.te
        r.kzindex = -1.002 * cf + 0.283 * tobinq + 3.139 * (r.tl / r.ta1) \
                    - 39.368 * (r.div / r.ta1) - 1.315 * ch
        r.age = r.year - int(str(r.estdate)[0:4])
        # TODO: Whited and Wu index 는 왜 안섰는지
        # TODO: sa index 계산을 원화로 하면 뭔가 많이 이상해지는데
        # ta 는 단위 1000원 이므로 dollar 랑 비슷하긴 한데
        # inflation 은 2015 년을 100 으로 한 통계청 소비자 물가 지수
        # 논문에서는 age 랑 size winsorizing  도 있음!!
        logta = math.log(r.ta / (r.inflation / 100))

        r.saindex = -0.737 * logta + 0.043 * logta ** 2 - 0.040 * r.age
        r.ch = ch
        r.cf = cf
        r.tobinq = tobinq
        r.logta = logta
        c.ins(r, 'constraints')


#%%
# Almeida (alpha1)
with connect('db.db') as c:
    c.create('select id, year + 1 as year, ch as ch1 from constraints', 'temp')
    c.join(
        ['constraints', '*', 'id, year'],
        ['temp', 'ch1', 'id, year'],
        name='temp',
        pkeys='id, year'
    )

    c.drop('almeida')
    # TODO: 6 년은 임의의 값
    for rs in c.fetch('temp', order='year', group='id', where="""
        isnum(ch, ch1, cf, tobinq, logta)
        """):
        for rs1 in rs.overlap(6):
            if rs1[0].year + 5 == rs1[-1].year:
                for r in rs1:
                    r.dch = r.ch - r.ch1
                result = rs1.ols('dch ~ cf + tobinq + logta')
                r0 = Row()
                r0.year = rs1[-1].year
                r0.id = rs1[-1].id
                r0.almeida = result.params.cf
                c.ins(r0, 'almeida')

    c.join(
        ['constraints', '*', 'id, year'],
        ['almeida', 'almeida', 'id, year'],
        name='constraints1'
    )




# ==============================================================================
# DATASET
# ==============================================================================
#%%
# liquidity measure 랑 constraint 랑 합해보자
# 근데 lagged(1 month) size 가 필요해 나중에 weighted return 구해야 하거든
with connect('db.db') as c:
    # 아 그냥 'db' 라고 할걸 괜히 'db.db' 라고 해서 귀찮네
    c.join(
        ['prc', '*', 'id, yyyymm'],
        ['shrout', 'shrout', 'id, yyyymm'],
        name='temp'
    )
    c.register(addmonth)
    c.create('select *, (prc * shrout) / 1000 as size1, addmonth(yyyymm, 1) as yyyymm1 from temp', 'size1')


#%%

with connect('db.db') as c:
    def gen(tbl):
        for r in c.fetch(tbl):
            if r.nmonth == 12 and r.yyyymm[5:] == '12':
                r.year = r.yyyymm[:4]
                r.month = r.yyyymm[5:]
                yield r
    c.insert(gen('liqproxies'), 'temp1')

    def gen1(tbl):
        for r in c.fetch(tbl):
            if r.yyyymm[5:] == '12':
                r.year = r.yyyymm[:4]
                r.month = r.yyyymm[5:]
                yield r
    c.insert(gen1('mkt500'), 'temp2')
    c.insert(gen1('exchcd'), 'temp3')
    c.join(
        ['temp1', '*', 'id, year'],
        ['temp2', 'mkt500', 'id, year'],
        ['temp3', 'exchcd', 'id, year']
    )

    c.join(
        ['temp1', '*', 'id, year'],
        ['constraints1', 'kzindex, logta, age, payout, saindex, almeida', 'id, year']
    )

    # TODO: 맞는지 확인, "2012-04" - "2013-03" 까지를 2011 년의 constraint 랑 맞춰줘야 하니까,
    def matching_year(yyyymm):
        year = int(yyyymm[0:4])
        if yyyymm[5:] in ['01', '02', '03']:
            return year - 2
        return year - 1

    # 위의 함수를 등록해주면, sql 에서 쓸수 있게 됨
    c.register(matching_year)
    c.join(
        ['ret', '*', 'id, yyyymm'],
        ['size1', 'prc, shrout, size1', 'id, yyyymm1'],
        name='temp2'
    )
    c.create('select *, matching_year(yyyymm) as myear from temp2')

    c.join(
        ['temp2', '*', 'id, myear'],
        ['temp1', """amihud, zerofreq, stoll_cov, nmonth, exchcd, mkt500,
        kzindex, logta, age, payout, saindex, almeida""", 'id, year'],
        name='dataset'
    )
    c.create("""select * from dataset where isnum(ret, prc) and prc >= 1000
    and (exchcd="유가증권시장" or exchcd="코스닥")
    """)




# ==============================================================================
# 2 WAY sorting
# ==============================================================================
#%%
# liquidity 계산 기간을 1, 3, 6, 12 로 했는데 생각해 보니까 FC 랑 맞추려면 일단 12 개월이
# 첫번째 후보가 되어야 할것 같음
with connect('db.db') as c:
    fcs = ['kzindex', 'logta', 'age', 'payout', 'saindex', 'almeida']
    liqs = ['amihud', 'zerofreq', 'stoll_cov']

    def gen():
        for fc, liq in product(fcs, liqs):
            print(fc, liq)
            for rs in c.fetch('dataset', order='yyyymm', group='myear', where=f"""
                isnum({fc}, {liq}, ret) and (exchcd="유가증권시장")
                """):
                # fc, liq column 추가
                rs.set('fc', fc)
                rs.set('liq', liq)

                # 1 년치씩 뽑아낸 후에, 첫번째 달의 값을 기준으로 포트폴리오를 짜고
                rs0 = rs.group('yyyymm')[0]

                # dependent sort
                rs.set('pn_fc', '')
                rs.set('pn_liq', '')
                # fc 로 먼저 sort
                for i, c1 in enumerate(rs0.order(fc).chunk(5), 1):
                    c1.set('pn_fc', i)
                    for j, c2 in enumerate(c1.order(liq).chunk(5), 1):
                        c2.set('pn_liq', j)
                # 이제 firm 별로 그루핑을 하고 첫번째 달의 값을 나머지 달에도 할당해 주면
                for rs1 in rs.group('id'):
                    rs1.set('pn_fc', rs1[0].pn_fc)
                    rs1.set('pn_liq', rs1[0].pn_liq)
                yield from rs
    c.insert(gen(), 'twoway')


#%%
# 각 포트폴리오별 ew, vw 평균을 구해봅시다
# (도대체 이게 왜 오래걸리는지 모르겠다만, 꽤 오래걸림)

with connect('db.db') as c:
    def gen():
        for rs in c.fetch('twoway', group="fc, liq, yyyymm, pn_fc, pn_liq",
                          where="isnum(pn_fc, pn_liq, size1)"):
            r = rs[0]
            r.ewret = rs.avg('ret')
            r.vwret = rs.avg('ret', 'size1')
            r.n = len(rs)
            yield r

        for rs in c.fetch('twoway', group="fc, liq, yyyymm, pn_fc ",
                          where="isnum(pn_fc, pn_liq, size1)"):
            r = rs[0]
            r.pn_liq = 0
            r.ewret = rs.avg('ret')
            r.vwret = rs.avg('ret', 'size1')
            r.n = len(rs)
            yield r

        for rs in c.fetch('twoway', group="fc, liq, yyyymm, pn_liq ",
                          where="isnum(pn_fc, pn_liq, size1)"):
            r = rs[0]
            r.pn_fc = 0
            r.ewret = rs.avg('ret')
            r.vwret = rs.avg('ret', 'size1')
            r.n = len(rs)
            yield r

        for rs in c.fetch('twoway', group="fc, liq, yyyymm",
                          where="isnum(pn_fc, pn_liq, size1)"):
            r = rs[0]
            r.pn_fc = 0
            r.pn_liq = 0
            r.ewret = rs.avg('ret')
            r.vwret = rs.avg('ret', 'size1')
            r.n = len(rs)
            yield r

    c.insert(gen(), 'result01')


# ==============================================================================
# Table01.csv
# ==============================================================================
#%%
def getfn(rs, i, j):
    if i == 0 and j == 0:
        return rs
    elif i == 0:
        return rs.where(lambda r: r.pn_liq == j)
    elif j == 0:
        return rs.where(lambda r: r.pn_fc == i)
    else:
        return rs.where(lambda r: r.pn_fc == i and r.pn_liq == j)


with connect('db.db') as c:
    def avgfn(rs, col):
        print()
        print(rs[0].fc, rs[0].liq, col)
        print(',0,1,2,3,4,5,diff')
        for i in range(6):
            print(i, end=',')
            for j in range(6):
                print(getfn(rs, i, j).avg(col, ndigits=2), end=',')
            high = getfn(rs, i, 5)[col]
            low = getfn(rs, i, 1)[col]
            m, tval = ttest(diff(high, low))
            print(f'{m}[{tval}]')
        print(end=',')
        for j in range(6):
            high = getfn(rs, 5, j)[col]
            low = getfn(rs, 1, j)[col]
            m, tval = ttest(diff(high, low))
            print(f'{m}[{tval}]', end=',')
        h1 = getfn(rs, 1, 5)[col]
        l1 = getfn(rs, 1, 1)[col]
        h2 = getfn(rs, 5, 5)[col]
        l2 = getfn(rs, 5, 1)[col]
        # diff of diff
        d = diff(diff(h2, l2), diff(h1, l1))
        m, tval = ttest(d)
        print(f'{m}[{tval}]')
        print()

    with open('table01.csv', 'w') as f, redirect_stdout(f):
        for rs in c.fetch('result01', group='fc, liq', where="""
            yyyymm >= '2001-04' and yyyymm <= '2017-03'
            """):
            avgfn(rs, 'ewret')
            avgfn(rs, 'vwret')




#%%

# ==============================================================================
# ==============================================================================

with connect('db.db') as c:
    # print(c.df('dataset', cols='ret, size1, amihud, stoll_cov, zerofreq, kzindex, logta, age, payout, saindex, almeida').describe())
    print(c.df('dataset', where='isnum(ret)', cols='ret').describe([0.01, 0.05, 0.1, 0.25, 0.5, 0.75, 0.9, 0.95, 0.99]))
    # print(c.df('dataset', where='isnum(ret) and ret > 1000', cols='ret').describe([0.01, 0.05, 0.1, 0.25, 0.5, 0.75, 0.9, 0.95, 0.99]))
