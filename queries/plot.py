import matplotlib.pyplot as plt
import numpy as np


times = [[], [], []]
with open("times.txt") as f:
    lines = f.readlines()
    for i in range(len(lines)):
        times[i % 3].append(float(lines[i]))

labels = ['Query 1', 'Query 2', 'Query 3', 'Query 4', 'Query 5']
rdd = times[0]
csv = times[1]
par = times[2]

x = np.arange(len(labels))  # the label locations
width = 0.2  # the width of the bars

fig, ax = plt.subplots()
rects1 = ax.bar(x - 3 * width/2, rdd, width, label='RDD')
rects2 = ax.bar(x - width/2, csv, width, label='SQL CSV')
rects3 = ax.bar(x + width/2, par, width, label='SQL PARQUET')

ax.set_ylabel('Time(s)')
ax.set_xticks(x, labels)
ax.legend()

fig.tight_layout()

plt.savefig('queries.png')
