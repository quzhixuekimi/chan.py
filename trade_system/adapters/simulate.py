from futu import *

# 1. 建立连接 (假设 OpenD 运行在本地 11111 端口)
# filter_trdmarket 指定市场：HK(港股), US(美股), SH/SZ(沪深)
trd_ctx = OpenSecTradeContext(
  filter_trdmarket=TrdMarket.US, host="127.0.0.1", port=22222
)

# 2. 获取账户列表，筛选模拟环境
ret, data = trd_ctx.get_acc_list()
if ret == RET_OK:
  # 过滤出模拟账户
  paper_accounts = data[data["trd_env"] == "SIMULATE"]
  print(paper_accounts)

# 3. 查询虚拟账户余额 (accinfo_query)
# 注意：trd_env 必须指定为 SIMULATE
ret, data = trd_ctx.accinfo_query(trd_env=TrdEnv.SIMULATE)
if ret == RET_OK:
  print(data[["total_assets", "cash", "power"]])  # 总资产、现金、购买力
else:
  print("查询失败: ", data)


ret, data = trd_ctx.place_order(
  price=28.5,  # 设定的限价
  qty=1,
  code="US.HIMS",  # 注意前缀是 US.
  trd_side=TrdSide.BUY,
  trd_env=TrdEnv.SIMULATE,
)

if ret == RET_OK:
  print("下单成功：", data)
else:
  print("下单失败：", data)

trd_ctx.close()
