import sys, os, math
from scipy.stats import norm
import matplotlib.pyplot as plt
import numpy as np
from scipy.stats import binned_statistic
from numpy import array
import matplotlib

def calculate_ecdf(data, min, max):
    total_points = len(data)
    ecdf = dict()
    for point in data:
        if point not in ecdf:
            ecdf[point] = 0
        ecdf[point] += 1

    for i in xrange(min, max+1):
        if i not in ecdf:
            ecdf[i] = 0
        if i == 1:
            ecdf[i] = float(ecdf[i]) / float(total_points)
        else:
            ecdf[i] = ecdf[i-1] + (float(ecdf[i]) / float(total_points))
    return ecdf


directory = "."
latencies = dict()
min = sys.maxint
max = -1
for filename in os.listdir(directory):
    filepath = os.path.join(directory, filename)
    with open(filename, "r") as fin:
        if not filename.endswith(".txt"): continue
        ixp_name = filename.split(".")[1]
        print ixp_name
        latencies[ixp_name] = list()
        for line in fin:
            lf = line.strip().split("|")
            latency = math.ceil(float(lf[2]))
            if len(lf) > 0 and int(lf[1]) > 30:
                if latency > max:
                    max = latency
                if latency < min:
                    min = latency
                latencies[ixp_name].append(latency)

binned_latencies = dict()
for ixp in latencies:
    binned_latencies[ixp] = dict()
    for rtt in latencies[ixp]:
        bin = math.ceil(rtt/10)
        if bin not in binned_latencies[ixp]:
            binned_latencies[ixp][bin] = 0
        binned_latencies[ixp][bin] += 1

for ixp in binned_latencies:
    plt.plot(binned_latencies[ixp].keys(), binned_latencies[ixp].values())

plt.savefig("test.png")