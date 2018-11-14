import pandas as pd
import talib as ta
import numpy as np

def initialize(ctx):
    # 設定
    ctx.logger.debug("initialize() called")
    ctx.configure(
      target="jp.stock.daily",
      channels={          # 利用チャンネル
        "jp.stock": {
          "symbols": [
            'jp.stock.1605',	#	国際石油開発帝石(株)
            'jp.stock.1802',	#	(株)大林組
            'jp.stock.1803',	#	清水建設(株)
            'jp.stock.9502',	#	中部電力(株)
            'jp.stock.9503',	#	関西電力(株)
            'jp.stock.9506',	#	東北電力(株)
            'jp.stock.9531',	#	東京ガス(株)
            'jp.stock.9532',	#	大阪ガス(株)
            'jp.stock.9613',	#	(株)ＮＴＴデータ
          ],
          "columns": [
            #"open_price_adj",    # 始値(株式分割調整後)
            #"high_price_adj",    # 高値(株式分割調整後)
            #"low_price_adj",     # 安値(株式分割調整後)
            "close_price",        # 終値
            "close_price_adj",    # 終値(株式分割調整後) 
            "volume_adj",         # 出来高
            "txn_volume",         # 売買代金
            "ns_sentiment",
          ]
        }
      }
    )

    def _mavg_signal(data):
        ns_sentiment = data["ns_sentiment"].fillna(method="ffill")
        # ctx.logger.debug(ns_sentiment)
      
        ns_sentiment_m50 = ns_sentiment.rolling(window=50, center=False).mean()
        # ctx.logger.debug(ns_sentiment_m25)

        ns_sentiment_m50_pct_change = ns_sentiment_m50.pct_change()
        # ctx.logger.debug(ns_sentiment_m25_diff)
        m25 = data["close_price_adj"].fillna(method='ffill').ewm(span=25).mean()
        m75 = data["close_price_adj"].fillna(method='ffill').ewm(span=75).mean()
        dfma25 = (data["close_price_adj"] -  m25) / m25 * 100
        ratio = m25 / m75

        buy_sig = dfma25[(dfma25 < -2) & (ns_sentiment_m50 > 0.16)]
        sell_sig = dfma25[(dfma25 > 2) & (ns_sentiment_m50_pct_change > 0.1)]
        return {
            "mavg_25:price": m25,
            "mavg_75:price": m75,
            #"mavg_ratio:ratio": ratio,
            "buy:sig": buy_sig,
            "sell:sig": sell_sig,
            "ns_sentiment_m50": ns_sentiment_m50,
            "ns_sentiment_m50_pct_change": ns_sentiment_m50_pct_change,
        }

    # シグナル登録
    ctx.regist_signal("mavg_signal", _mavg_signal)

def handle_signals(ctx, date, current):
    '''
    current: pd.DataFrame
    '''

    done_syms = set([])
    for (sym,val) in ctx.portfolio.positions.items():
        returns = val["returns"]
        #ctx.logger.debug("%s %f" % (sym, returns))
        if returns < -0.02:
          sec = ctx.getSecurity(sym)
          sec.order(-val["amount"], comment="損切り(%f)" % returns)
          done_syms.add(sym)
        # elif returns > 0.08:
        #   sec = ctx.getSecurity(sym)
        #   sec.order(-val["amount"], comment="利益確定売(%f)" % returns)
        #   done_syms.add(sym)


    buy = current["buy:sig"].dropna()
    for (sym,val) in buy.items():
        if sym in done_syms:
          continue
        
        sec = ctx.getSecurity(sym)
        sec.order(sec.unit() * 1, comment="SIGNAL BUY")
        #ctx.logger.debug("BUY: %s,  %f" % (sec.code(), val))
        pass

    sell = current["sell:sig"].dropna()
    for (sym,val) in sell.items():
        if sym in done_syms:
          continue

        sec = ctx.getSecurity(sym)
        sec.order(sec.unit() * -1, comment="SIGNAL SELL")
        #ctx.logger.debug("SELL: %s,  %f" % (sec.code(), val))
        pass

