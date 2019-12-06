import json
import pymysql
import os


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

	file = []
	file.append('./fju.json')
	file.append('./inist.json') 
	file.append('./night.json')

	now = 0
	for j in file:
		print('import the ' + j )
		json_data = open(j, encoding='utf-8').read()
		fju_course_data = json_obj = json.loads(json_data)

		for i, item in enumerate(fju_course_data):
			s = []
			course_code = item.get("課程碼", None)
			s.append(course_code)
			name = item.get("科目名稱", None).get("中文", None)
			s.append(name)
			teacher = item.get("授課教師/專長", None).get("教師", None)
			s.append(teacher)
			department = item.get("開課單位",None)
			s.append(department)
			day = item.get("星期", None)
			day = day[2:5]
			s.append(day)
			week = item.get("週別", None)
			s.append(week)
			period = item.get("節次", None)
			s.append(period)
			classroom = item.get("教室", None)
			s.append(classroom)

			week2 = item.get("週別2", None)
			s.append(week2)
			period2 = item.get("節次2", None)
			s.append(period2)
			classroom2 = item.get("教室2", None)
			s.append(classroom2)

			week3 = item.get("週別3", None)
			s.append(week3)
			period3 = item.get("節次3", None)
			s.append(period3)
			classroom3 = item.get("教室3", None)
			s.append(classroom3)


			course_selection = item.get("是否開放選課條", None)
			if course_selection == '否':
				s.append(0)
			elif course_selection == '是':
				s.append(1)
	
			str_for_exec = ''
			cnt = 1
			for i in s:
				str_for_exec += '"' + str(i) + '"' + ','
				cnt += 1

			str_for_exec = str_for_exec[:-1]
			print(str_for_exec)

			conn.select_db('Course')
			cursor.execute("INSERT INTO fju_course VALUES ({})".format(str_for_exec))
			print('Sending:' + str(now))
			now += 1


if __name__ == "__main__":
	conn, cursor = connect_to_sql()
	parse(conn, cursor)
