
# 存储part-r-00000.txt的数据
user_active_days = {}
with open("D:\\学习啊\\金融大数据\\实验二\\part-r-00000.txt", 'r') as file:
    for line in file:
        parts = line.strip().split()
        if len(parts) == 2:
            user_id = int(parts[0])
            active_days = int(parts[1])
            user_active_days[user_id] = active_days

# 星座到数字的映射字典
constellation_mapping = {
    "白羊座": 1, "金牛座": 2, "双子座": 3, "巨蟹座": 4, "狮子座": 5, "处女座": 6,
    "天秤座": 7, "天蝎座": 8, "射手座": 9, "摩羯座": 10, "水瓶座": 11, "双鱼座": 12
}
import csv
# 处理user_profile_table.csv并合并
with open("D:\\学习啊\\金融大数据\\实验二\\user_profile_table.txt", 'r', newline='', encoding='utf-8') as csvfile, \
        open('D:\\学习啊\\金融大数据\\实验二\\merged_table.txt', 'w', newline='', encoding='utf-8') as output_file:
    reader = csv.reader(csvfile)
    writer = csv.writer(output_file)
    # 跳过标题行
    next(reader)
    for row in reader:
        if len(row) == 4:
            user_id = int(row[0])
            sex = int(row[1])
            city = int(row[2])
            constellation = row[3]
            constellation_number = constellation_mapping.get(constellation, -1)
            if user_id in user_active_days:
                writer.writerow([user_id, user_active_days[user_id], sex, city, constellation_number])