from pyspark import SparkConf, SparkContext


def parse_line(line):
    fields = line.split(',')
    customer_id = int(fields[0])
    amount_spent = float(fields[2])
    return customer_id, amount_spent


conf = SparkConf().setMaster("local").setAppName("SpendingsCount")
sc = SparkContext(conf=conf)

lines = sc.textFile("file:///Users/sgardziewicz/Documents/SparkCourse/customer-orders.csv")
parsed_lines = lines.map(parse_line)
summed_amounts = parsed_lines.reduceByKey(lambda x, y: x + y)
swapped = summed_amounts.map(lambda xy: (xy[1], xy[0]))
sorted_amounts = swapped.sortByKey()

print(sorted_amounts.collect())
