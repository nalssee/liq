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


def credit_rating(x):
    ratings1 = ["AAA", "AA+", "AA", "AA-", "A+"]
    ratings2 = ["A", "A-", "BBB+", "BBB"]
    ratings3 = ["BBB-", "BB+", "BB", "BB-", "B+", "B", "B-", "C", "CC", "CCC-", "CCC", "CCC+", "D"]

    if x in ratings1:
        return 1
    elif x in ratings2:
        return 2
    elif x in ratings3:
        return 3
    else:
        return ''


def addmonth(date, n):
    return dmath(date, f'{n} months', '%Y-%m')


def pos(*xs):
    return all(isnum(x) and x > 0 for x in xs)


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



def fnguide1(fname, sname):
    rss = readxl(fname, sname)
    ids = rss[8][1:]
    for rs in rss[14:]:
        date = rs[0]
        for id, val in zip(ids, rs[1:]):
            r = Row()
            r.yyyymm = date.strftime('%Y-%m')
            r.id = id
            r.icode = val
            yield r


def yyyymm(r):
    "1981-01-21 => 1981-01 column 하나 추가"
    r.yyyymm = dconv(r.date, '%Y-%m-%d', '%Y-%m')
    return r


def build_yyyymm(r):
    "year 하고 month 붙이기 예를들면 1999, 3 => 1999-03"
    r.yyyymm = str(r.year) + '-' + str(r.month).zfill(2)
    return r




# # ==============================================================================
# # DATASET
# # ==============================================================================



# # ==============================================================================
# # 2 WAY sorting
# # ==============================================================================
# # liquidity 계산 기간을 1, 3, 6, 12 로 했는데 생각해 보니까 FC 랑 맞추려면 일단 12 개월이
# # 첫번째 후보가 되어야 할것 같음
# with connect('db.db') as c:
#     fcs = ['kzindex', 'logta', 'age', 'payout', 'saindex', 'almeida', 'sgr', 'rgr', 'cr']
#     liqs = ['amihud', 'zerofreq', 'stoll_cov']

#     def gen():
#         for fc, liq in product(fcs, liqs):
#             print(fc, liq)
#             for rs in c.fetch('dataset', order='yyyymm', group='myear', where=f"""
#                 isnum({fc}, {liq}, ret)
#                 """):

#                 # fc, liq column 추가
#                 rs.set('fc', fc)
#                 rs.set('liq', liq)

#                 # 1 년치씩 뽑아낸 후에, 첫번째 달의 값을 기준으로 포트폴리오를 짜고
#                 rs0 = rs.group('yyyymm')[0]

#                 # dependent sort
#                 rs.set('pn_fc', '')
#                 rs.set('pn_liq', '')
#                 # fc 로 먼저 sort

#                 if fc == 'cr':
#                     for i, c1 in enumerate(rs0.group(fc), 1):
#                         c1.set('pn_fc', i)
#                         for j, c2 in enumerate(c1.order(liq).chunk(5), 1):
#                             c2.set('pn_liq', j)

#                 else:
#                     for i, c1 in enumerate(rs0.order(fc).chunk(5), 1):
#                         c1.set('pn_fc', i)
#                         for j, c2 in enumerate(c1.order(liq).chunk(5), 1):
#                             c2.set('pn_liq', j)

#                 # 이제 firm 별로 그루핑을 하고 첫번째 달의 값을 나머지 달에도 할당해 주면
#                 for rs1 in rs.group('id'):
#                     rs1.set('pn_fc', rs1[0].pn_fc)
#                     rs1.set('pn_liq', rs1[0].pn_liq)
#                 yield from rs
#     c.insert(gen(), 'twoway')



# # 각 포트폴리오별 ew, vw 평균을 구해봅시다
# # (도대체 이게 왜 오래걸리는지 모르겠다만, 꽤 오래걸림)

# with connect('db.db') as c:
#     def gen():
#         def fn(rs, pn_fc, pn_liq):
#             r0 = rs[0]
#             r = Row()
#             r.fc = r0.fc
#             r.liq = r0.liq
#             r.yyyymm = r0.yyyymm

#             r.pn_fc = pn_fc
#             r.pn_liq = pn_liq
#             r.ewret = rs.avg('ret')
#             r.vwret = rs.avg('ret', 'size1')
#             r.n = len(rs)
#             return r

#         for rs in c.fetch('twoway', group="fc, liq, yyyymm",
#                           where="isnum(pn_fc, pn_liq, ret, size1)"):
#             yield fn(rs, 0, 0)
#             for rs1 in rs.group('pn_fc'):
#                 yield fn(rs1, rs1[0].pn_fc, 0)
#             for rs1 in rs.group('pn_liq'):
#                 yield fn(rs1, 0, rs1[0].pn_liq)
#             for rs1 in rs.group('pn_fc, pn_liq'):
#                 yield fn(rs1, rs1[0].pn_fc, rs1[0].pn_liq)

#     c.insert(gen(), 'result01')



# # ==============================================================================
# # Table01.csv
# # ==============================================================================
# def getfn(rs, i, j):
#     if i == 0 and j == 0:
#         return rs
#     elif i == 0:
#         return rs.where(lambda r: r.pn_liq == j)
#     elif j == 0:
#         return rs.where(lambda r: r.pn_fc == i)
#     else:
#         return rs.where(lambda r: r.pn_fc == i and r.pn_liq == j)


# with connect('db.db') as c:
#     def avgfn(rs, col):

#         psize1 = 3 if rs[0].fc  == 'cr' else 5
#         psize2 = 5

#         print(rs[0].fc, rs[0].liq, col)
#         print(',0,1,2,3,4,5,diff')

#         for i in range(psize1 + 1):
#             print(i, end=',')
#             for j in range(psize2 + 1):
#                 print(getfn(rs, i, j).avg(col, ndigits=2), end=',')
#             high = getfn(rs, i, psize2)[col]
#             low = getfn(rs, i, 1)[col]
#             m, tval = ttest(diff(high, low))
#             print(f'{m}[{tval}]')
#         print(end=',')
#         for j in range(psize2 + 1):
#             high = getfn(rs, psize1, j)[col]
#             low = getfn(rs, 1, j)[col]
#             m, tval = ttest(diff(high, low))
#             print(f'{m}[{tval}]', end=',')
#         h1 = getfn(rs, 1, psize2)[col]
#         l1 = getfn(rs, 1, 1)[col]
#         h2 = getfn(rs, psize1, psize2)[col]
#         l2 = getfn(rs, psize1, 1)[col]
#         # diff of diff
#         d = diff(diff(h2, l2), diff(h1, l1))
#         m, tval = ttest(d)
#         print(f'{m}[{tval}]')
#         print()

#     with open('table01.csv', 'w') as f, redirect_stdout(f):
#         for rs in c.fetch('result01', group='fc, liq', where="""
#             (yyyymm >= '2001-04' and yyyymm <= '2007-06') or
#             (yyyymm >= '2011-04' and yyyymm <= '2017-12')
#             """):
#             avgfn(rs, 'ewret')
#             avgfn(rs, 'vwret')



# # ==============================================================================
# # ==============================================================================

# # 이 작업은 좀 오래걸리니까 컴퓨터가 4개 이상의 코어를 갖고 있다면
# # multi process 를 활용하는 것을 고려해 볼만함 (single core 로 3시간 걸린거 같은데)

def stoll_cov(prices):
    dprices = []
    for p0, p1 in zip(prices, prices[1:]):
        if isnum(p0, p1):
            dprices.append(p1 - p0)
    # np.cov 는 covariance matrix 를 계산
    return np.cov([dprices[1:], dprices[:-1]])[0, 1] if len(dprices) > 2 else ''


def compute_liquidities(rs, nmonths):
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


def compute_size(r):
    # 일단은 12 월치만 필요하니까
    if r.yyyymm[5:] == '12':
        # 금액인 경우 단위가 다 천원이더라구
        r.size = (r.prc * r.shrout) / 1000
        r.year = r.yyyymm[0:4]
        yield r


def year1(r):
    r.year1 = r.year + 1
    yield r


def add(**kwargs):
    def fn(r):
        for k, v in kwargs.items():
            try:
                r[k] = v(r)
            except Exception:
                r[k] = ''
        yield r
    return fn



def compute_constraints(r):
    # cash flow
    cf = (r.oi + r.noi + r.dep - r.noc - r.div) / r.ta1
    # cash holdings
    ch  = (r.cash + r.csec) / r.ta1
    tobinq = (r.ta - (r.te - r.prefstock) + r.size) / r.te
    # kzindex 가 크면 contraint 크다
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
    r.sgr = (r.sales - r.sales1) / r.sales1 if pos(r.sales, r.sales1) else ''
    r.rgr = (r.rnd - r.rnd1) / r.rnd1 if pos(r.rnd, r.rnd1) else ''
    r.cr = credit_rating(r.rating)
    yield r


def compute_almeida(rs):
    for rs1 in rs.overlap(6):
        try:
            if rs1[0].year + 5 == rs1[-1].year:
                for r in rs1:
                    r.dch = r.ch - r.ch1
                result = rs1.ols('dch ~ cf + tobinq + logta')
                r0 = Row()
                r0.year = rs1[-1].year
                r0.id = rs1[-1].id
                r0.almeida = result.params.cf
                yield r0
        except:
            pass


# TODO: 맞는지 확인, "2012-04" - "2013-03" 까지를 2011 년의 constraint 랑 맞춰줘야 하니까,
def matching_year(r):
    year = int(r.yyyymm[0:4])
    if r.yyyymm[5:] in ['01', '02', '03']:
        r.myear = year - 2
    else:
        r.myear = year - 1
    yield r





if __name__ == "__main__":
    fcs = 'kzindex, logta, age, payout, saindex, almeida, sgr, rgr, cr'
    liqs = 'amihud, zerofreq, stoll_cov'

    process(
        Load('daily.csv', fn=yyyymm),
        Load('comp_kor.csv', fn=build_yyyymm),
        Load('prc.csv', fn=yyyymm),
        Load('shrout.csv', fn=yyyymm),
        Load('inflation.csv', fn=yyyymm),
        Load('ret.csv', fn=yyyymm),
        Load('mkt500.csv', fn=yyyymm),
        Load('exchcd.csv', fn=yyyymm),
        Load(fnguide1('code.xlsx', 'code'), 'indcode'),

        # input table 이름이랑 output table 이름이 같으면 서로 다른 cpu core를 사용한다
        # 주의할 점은 multicore 를 스면 에러가 나도 에러메시지를 뱉지 않는다.
        # single core 에서 제대로 작동하는 경우에만 사용
        Apply('daily', 'liqproxies', compute_liquidities, group='id', arg=[1, 12]),
        Apply('daily', 'liqproxies', compute_liquidities, group='id', arg=[3, 6]),

        # make size table
        Join(
            ['prc', '*', 'yyyymm, id'],
            ['shrout', 'shrout', 'yyyymm, id'],
            name='temp_size'
        ),
        Apply('temp_size', 'size', compute_size),

        Join(
            ['comp_kor', '*', 'id, year'],
            ['comp_kor', 'ta as ta1, ppe as ppe1, sales as sales1, rnd as rnd1',
             lambda r: (r.id, r.year + 1)],
            ['size', 'size', 'id, year'],
            ['inflation', 'inflation', ',year'],
            name='comp'
        ),

        # constraints 계산
        Apply('comp', 'constraints', compute_constraints, where=lambda r: r.ta1 > 0 and r.te > 0),

        # almeida(alpha1) 계산
        Join(
            ['constraints', '*', 'id, year'],
            ['constraints', 'ch as ch1', lambda r: (r.id, r.year + 1)],
            name='temp_constraints',
            pkeys='id, year'
        ),
        Apply('temp_constraints', 'almeida', compute_almeida, order='year', group='id'),

        Join(
            ['temp_constraints', '*', 'id, year'],
            ['almeida', 'almeida', 'id, year'],
            name='constraints1'
        ),

        # build dataset
        # append matching year
        Apply('ret', 'ret1', matching_year),
        Apply('temp_size', 'size1', add(size=lambda r: r.shrout * r.prc / 1000)),

        Join(
            ['liqproxies', '*', lambda r: (r.id, addmonth(r.yyyymm, 1))],
            ['ret1', 'ret, myear', 'id, yyyymm'],
            ['size1', 'size', lambda r: (r.id, addmonth(r.yyyymm, 1))],
            name='liqproxies1',
        ),
        Join(
            ['liqproxies1', '*', 'id, myear'],
            ['constraints1', fcs, 'id, year'],
            name='dset01'
        )

    )


# with connect('db.db') as c:
#     fcs = 'kzindex, logta, age, payout, saindex, almeida, sgr, rgr, cr'
#     liqs = 'amihud, zerofreq, stoll_cov'

#     def gen(tbl):
#         for r in c.fetch(tbl):
#             if r.nmonth == 12 and r.yyyymm[5:] == '12':
#                 r.year = r.yyyymm[:4]
#                 r.month = r.yyyymm[5:]
#                 yield r
#     c.insert(gen('liqproxies'), 'temp1')

#     def gen1(tbl):
#         for r in c.fetch(tbl):
#             if r.yyyymm[5:] == '12':
#                 r.year = r.yyyymm[:4]
#                 r.month = r.yyyymm[5:]
#                 yield r
#     c.insert(gen1('mkt500'), 'temp2')
#     c.insert(gen1('exchcd'), 'temp3')
#     c.insert(gen1('indcode'), 'temp4')
#     c.join(
#         ['temp1', '*', 'id, year'],
#         ['temp2', 'mkt500', 'id, year'],
#         ['temp3', 'exchcd', 'id, year'],
#         ['temp4', 'icode', 'id, year']
#     )

#     c.join(
#         ['temp1', '*', 'id, year'],
#         ['constraints1', fcs, 'id, year']
#     )

#     # TODO: 맞는지 확인, "2012-04" - "2013-03" 까지를 2011 년의 constraint 랑 맞춰줘야 하니까,
#     def matching_year(yyyymm):
#         year = int(yyyymm[0:4])
#         if yyyymm[5:] in ['01', '02', '03']:
#             return year - 2
#         return year - 1

#     # 위의 함수를 등록해주면, sql 에서 쓸수 있게 됨
#     c.register(matching_year)
#     c.join(
#         ['ret', '*', 'id, yyyymm'],
#         ['size1', 'prc, shrout, size1', 'id, yyyymm1'],
#         name='temp2'
#     )
#     c.create('select *, matching_year(yyyymm) as myear from temp2')

#     c.join(
#         ['temp2', '*', 'id, myear'],
#         ['temp1', ','.join([liqs, fcs, 'nmonth, exchcd, mkt500, icode']), 'id, year'],
#         name='dataset'
#     )
#     c.create("""select * from dataset where isnum(ret, prc) and prc >= 1000
#     and (exchcd="유가증권시장" or exchcd="코스닥") and icode != "K"
#     """)


