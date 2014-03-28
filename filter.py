f = open('def', 'rw')
sum=0.0
for row in f:
    start=row.find('[')
    end=row.find(']')
    sum=sum+float(row[start+1:end])
print sum

