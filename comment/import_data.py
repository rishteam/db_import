import json, pymysql, os


file = './data.json'
json_data = open(file, encoding='utf-8').read()
json_obj = json.loads(json_data)


# connect to mysql
def connect_to_sql():
	config = dict(host='vm.rish.com.tw', user='root', password='',
                cursorclass=pymysql.cursors.DictCursor
                )

	conn = pymysql.Connect(**config)
	conn.autocommit(1)
	cursor = conn.cursor()
	return conn, cursor


def parse(conn, cousor):
	comment_data = json_obj['Data']

	now = 0
	for i, item in enumerate(comment_data):
		s = []
		className = item.get("className", None)
		s.append(className)
		classOpen = item.get("classOpen", None)
		s.append(classOpen)
		# 2014/1/11 上午 12:11:32
		createDate = item.get("createDate", None)
		idx = 0
		time_str = ''
		tmp_str = ''
		flag = 0
		for i in range(len(createDate)):
			if createDate[i] == ' ':
				if len(tmp_str) == 1:
					tmp_str = '0' + tmp_str
				time_str += tmp_str
				idx = i
				break
			if createDate[i] == '/':
				if len(tmp_str) == 1:
					tmp_str = '0' + tmp_str
				time_str += tmp_str + '-'
				tmp_str = ''
			else:
				tmp_str += createDate[i]

		# print(tmp_str)


		add_12 = False
		time_str += ' '
		tmp_num = ''
		flag = 0
		for i in range(idx,len(createDate)):
			if createDate[i] == '下':
				add_12 = True
			elif createDate[i].isdigit() == True:
				tmp_num += createDate[i]
			elif createDate[i] == ':' and flag == 0:
				if add_12 == True:
					tmp_num = str(int(tmp_num) + 12)
					if tmp_num == '24':
						tmp_num = '00'
				elif add_12 == False:
					if int(tmp_num) < 10:
						tmp_num = '0' + tmp_num
				flag = 1
				time_str += tmp_num
				time_str += ':'
				tmp_num = ''
			elif createDate[i] == ':' and flag == 1:
				time_str += tmp_num
				time_str += ':'
				tmp_num = ''

		time_str += tmp_num


		s.append(time_str)
		ifFinalExam = item.get("ifFinalExam", None)
		s.append(ifFinalExam)
		ifGroupReport = item.get("ifGroupReport", None)
		s.append(ifGroupReport)
		ifMidExam = item.get("ifMidExam", None)
		s.append(ifMidExam)
		ifOtherExam = item.get("ifOtherExam", None)
		s.append(ifOtherExam)
		ifOtherWork = item.get("ifOtherWork", None)
		s.append(ifOtherWork)
		ifPersonalReport = item.get("ifPersonalReport", None)
		s.append(ifPersonalReport)
		ifSmallExam = item.get("ifSmallExam", None)
		s.append(ifSmallExam)
		lvExamAmount = item.get("lvExamAmount", None)
		s.append(lvExamAmount)
		lvFun = item.get("lvFun", None)
		s.append(lvFun)
		lvLearned = item.get("lvLearned", None)
		s.append(lvLearned)
		lvRequest = item.get("lvRequest", None)
		s.append(lvRequest)
		lvTeachlear = item.get("lvTeachlear", None)
		s.append(lvTeachlear)
		lvWork = item.get("lvWork", None)
		s.append(lvWork)
		lv_recommend = item.get("lv_recommend", None)
		s.append(lv_recommend)
		message = item.get("message", None)
		s.append(message)
		teaher = item.get("teaher", None)
		s.append(teaher)


		str_for_exec = ''
		cnt = 1
		for i in s:
			# print(type(i))
			# if cnt == 18 or cnt == 3:
			# 	str_for_exec += '"' + str(i) + '"' + ','
			# else:
			str_for_exec += '"' + str(i) + '"' + ','
			cnt += 1


		# print(s)[]
		str_for_exec = str_for_exec[:-1]
		# print(str_for_exec)
		conn.select_db('Course')
		cursor.execute("INSERT INTO comment VALUES ({})".format(str_for_exec))
		print('Sending:' + str(now) )
		now += 1

if __name__ == "__main__":
	conn, cursor = connect_to_sql()
	parse(conn, cursor)
