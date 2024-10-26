import pandas as pd

# 读取合并后的表格数据
data = pd.read_csv('merged_table.txt', names=['user_id', 'active_days','sex', 'city', 'constellation_number'])

# 计算活跃天数与性别的相关性
correlation_sex = data['active_days'].corr(data['sex'])

# 计算活跃天数与城市的相关性（这里城市为数值类型，如果实际城市是类别数据，可能需要进一步处理，比如使用独热编码）
correlation_city = data['active_days'].corr(data['city'])

# 计算活跃天数与星座对应数字的相关性
correlation_constellation = data['active_days'].corr(data['constellation_number'])

print("活跃天数与性别的相关性:", correlation_sex)
print("活跃天数与城市的相关性:", correlation_city)
print("活跃天数与星座对应数字的相关性:", correlation_constellation)