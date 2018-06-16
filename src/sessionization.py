#By: Mykel Johnson mykeldj@gmail.com
#Date: 15 June 2018
#Response to coding challenge for Insight Date Engineering
#Python2.7

import datetime

import csv

import collections

import itertools

L = open('input/log.csv')

log = list(csv.reader(L))


def line_reader(log_file):
    
    for line in log_file:
        
        yield line



def skipFirst(it):
    
    it.next()
    
    for x in it:
        
        yield x



log_lines = iter(line_reader(log))



I = open('input/inactivity_period.txt')

inactive = float(I.read())



#output file

output_file = open('output/sessionization.txt', 'w')



timer = []

time_book = collections.defaultdict(list)

ip_book = {}

web_counter = 0



for rows in skipFirst(log_lines) :
    
    
    
    ips = next(itertools.islice(rows, 0, None))
    
    dates = datetime.datetime.strptime(next(itertools.islice(rows,1, None)), '%m/%d/%Y')
    
    times = datetime.datetime.strptime((next(itertools.islice(rows,2, None))), '%H:%M:%S').time()
    
    timer.append(times)
    
    current_time = datetime.datetime.combine(date=dates, time=times)
    
    r_time = datetime.datetime.strftime(current_time, "%m/%d/%Y %H:%M:%S")
    
    time_book[r_time].append(ips)
    
    if ips not in ip_book:
        
        ip_book[ips] = [r_time, current_time]

    if r_time not in time_book:
        
        timer.append(r_time)



web_counter = {x: collections.Counter(y) for x, y in time_book.items()}
    
    
    
    diff = times.second - timer[0].second
    
    
    
    if diff > inactive:
        
        time0 = timer[0]
        
        previous_time = time_book[time0]
        
        try:
            
            for n in range(int(inactive+1))[1:]:
                
                later_time = time_book[timer[n]]
                
                later_time += later_time
    
        except: later_time = time_book[timer[0]]
        
        
        
        earlier_ip = set(previous_time)
        
        later_ip = set(later_time)
        
        only_early = [i for i in earlier_ip if i not in later_ip]
        
        
        
        for ip in only_early:
            
            elapsed_time = (r_time - ip_book[ip][0]).total_seconds()
            
            start_time = ip_book[ip][1]
            
            tim = ip_book[ip][0]
            
            while tim < r_time:
                
                web_counter += web_counter[tim][ip]
                
                tim += datetime.timedelta(seconds=1)
            
            output_file.write([ip, start_time, current_time, elapsed_time, web_counter])
            
    output_file.write("\n")
        
        del timer[0]

else:
    
    pass

#close output_file

output_file.close()

