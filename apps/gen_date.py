import random

import pendulum
import csv

dt1=pendulum.datetime(2022, 1, 1, 0, 0, 0, tz='Asia/Ho_Chi_Minh')
dt2=pendulum.datetime(2022, 1, 1, 23, 59, 59, tz='Asia/Ho_Chi_Minh')

period = pendulum.period(dt1, dt2)
period_1 = pendulum.period(
    pendulum.datetime(2022, 1, 1, 0, 0, 0, tz='Asia/Ho_Chi_Minh'),
    pendulum.datetime(2022, 1, 1, 6, 59, 59, tz='Asia/Ho_Chi_Minh')
)
list_freq_1 = [1,2,3]
period_2 = pendulum.period(
    pendulum.datetime(2022, 1, 1, 7, 0, 0, tz='Asia/Ho_Chi_Minh'),
    pendulum.datetime(2022, 1, 1, 10, 59, 59, tz='Asia/Ho_Chi_Minh')
)
list_freq_2 = [2,3,4]
period_3 = pendulum.period(
    pendulum.datetime(2022, 1, 1, 11, 0, 0, tz='Asia/Ho_Chi_Minh'),
    pendulum.datetime(2022, 1, 1, 13, 59, 59, tz='Asia/Ho_Chi_Minh')
)

list_freq_3 = [3,4,5]


period_4 = pendulum.period(
    pendulum.datetime(2022, 1, 1, 14, 0, 0, tz='Asia/Ho_Chi_Minh'),
    pendulum.datetime(2022, 1, 1, 17, 59, 59, tz='Asia/Ho_Chi_Minh')
)
list_freq_4 = [2,3,4]



period_5 = pendulum.period(
    pendulum.datetime(2022, 1, 1, 18, 0, 0, tz='Asia/Ho_Chi_Minh'),
    pendulum.datetime(2022, 1, 1, 21, 59, 59, tz='Asia/Ho_Chi_Minh')
)
list_freq_5 = [4,5,6]



period_6 = pendulum.period(
    pendulum.datetime(2022, 1, 1, 22, 0, 0, tz='Asia/Ho_Chi_Minh'),
    pendulum.datetime(2022, 1, 1, 23, 59, 59, tz='Asia/Ho_Chi_Minh')
)
list_freq_6 = [2,3,4]

list_timestamp = []
k = 0
for i in period.range("seconds"):
    k = k+1
    if i in period_1:
        freq=random.choice(list_freq_1)
    elif i in period_2:
        freq=random.choice(list_freq_2)
    elif i in period_3:
        freq=random.choice(list_freq_3)
    elif i in period_4:
        freq=random.choice(list_freq_4)
    elif i in period_5:
        freq=random.choice(list_freq_5)
    elif i in period_6:
        freq=random.choice(list_freq_6)
    for j in range(freq):
        list_timestamp.append([i.format('DD/MMM/YYYY:HH:mm:ss ZZ')])


def writeCsvFile(fname, data, *args, **kwargs):
    """
    @param fname: string, name of file to write
    @param data: list of list of items

    Write data to file
    """

    with open(fname, 'w') as file:
        writer = csv.writer(file)
        for line in data:
            writer.writerow(line)


writeCsvFile(r'kafka_code/listtimestamp.csv', list_timestamp)
print(k)
print(len(list_timestamp))