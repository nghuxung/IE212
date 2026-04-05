from pyspark import SparkContext
from datetime import datetime

sc = SparkContext("local[*]", "Bai6")

# ĐỌC FILE
base = "file:///Users/nghuxung/Downloads/IE212/Lab3/"

ratings1 = sc.textFile(base + "input/ratings_1.txt")
ratings2 = sc.textFile(base + "input/ratings_2.txt")

ratings = ratings1.union(ratings2)

# =========================
# HÀM LẤY NĂM TỪ TIMESTAMP
# =========================
def get_year(ts):
    try:
        return datetime.fromtimestamp(int(ts)).year
    except:
        return None

ratings_rdd = ratings.map(lambda x: x.split(","))

# MAP → (Year, (rating,1))
mapped = ratings_rdd.map(lambda x: (
    get_year(x[3]), (float(x[2]), 1)
)).filter(lambda x: x[0] is not None)

# REDUCE
reduced = mapped.reduceByKey(
    lambda a, b: (a[0] + b[0], a[1] + b[1])
)

# AVG
avg = reduced.mapValues(
    lambda x: (round(x[0] / x[1], 2), x[1])
)

# SORT
sorted_result = avg.sortByKey()

data = sorted_result.collect()

# OUTPUT
print("\nPHÂN TÍCH ĐÁNH GIÁ THEO NĂM")
print("-" * 40)
print(f"{'Year':<10} | {'Avg':<12} | {'Count':<10}")
print("-" * 40)

for x in data:
    year = x[0]
    avg = x[1][0]
    count = x[1][1]

    print(f"{year:<10} | {avg:<12} | {count:<10}")

sc.stop()