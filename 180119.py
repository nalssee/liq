#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Jan 19 18:32:22 2018

@author: bjlee
"""

import pandas as pd
import numpy as np
#==============================================================================
# raw data : comp_kor(=accounting data) , crsp_kor(=stock data)
#TA : total asset
#CA : current asset
#CHE : cash and cash equivalent
#TL : total liability
#TE : total equity
#RND : RND
#PAYOUTRATIO : cash dividend
#SETUP : 기업의 설립일자
#CREDIT : 신용등급
#SALE : 매출액
#OPINCOME : 영업이익
#NOPINCOME : 비영업이익
#NOPCOST : 비영업비용
#DEP : 유형자산상각비
#DIV : 현금배당액
#MKTABLASSET : 단기유가증권
#BE : 보통주자본금
#지금까지는 모든 단위가 모두 천원
#==============================================================================

comp = pd.read_csv('comp_kor.csv')

comp.head(10)

comp["TA"] = comp["TA"].replace('-99', np.NaN)

print(comp["TA"])

comp["cashholding"]=(comp["CHE"]+comp["MKTABLASSET"])/(comp["TA"])

comp.info()


