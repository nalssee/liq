import csv

fname = 'crsp_kor_MKT500.csv'
col = 'mkt500'

with open(fname, newline='', encoding='utf-8') as f:
    with open('foo.csv', 'w', newline='', encoding='utf-8') as fout:

        rd = csv.reader(f)
        w = csv.writer(fout)

        ids = next(rd)

        w.writerow(['date', 'id', col])

        for r in rd:
            date = r[0][:10]
            for id, x in zip(ids[1:], r[1:]):
                w.writerow([date, id, x])
