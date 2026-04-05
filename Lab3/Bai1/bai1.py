from pyspark import SparkContext

sc = SparkContext("local[*]", "Bai1")

# ĐỌC FILE
base = "file:///Users/nghuxung/Downloads/IE212/Lab3/"

movies = sc.textFile(base + "input/movies.txt")
ratings1 = sc.textFile(base + "input/ratings_1.txt")
ratings2 = sc.textFile(base + "input/ratings_2.txt")

ratings = ratings1.union(ratings2)

# XỬ LÝ DỮ LIỆU
movies_rdd = movies.map(lambda x: x.split(","))
ratings_rdd = ratings.map(lambda x: x.split(","))

# MAP
movie_map = movies_rdd.map(
    lambda x: (x[0].strip(), x[1].strip())
)

rating_map = ratings_rdd.map(
    lambda x: (x[1].strip(), (float(x[2]), 1))
)

# TÍNH
rating_reduce = rating_map.reduceByKey(
    lambda a, b: (a[0] + b[0], a[1] + b[1])
)

avg_rating = rating_reduce.mapValues(
    lambda x: (round(x[0] / x[1], 2), x[1])
)

# JOIN
result = avg_rating.join(movie_map)

# SORT
sorted_result = result.sortBy(
    lambda x: x[1][0][0], ascending=False
)

data = sorted_result.collect()

# CHECK DATA
if len(data) == 0:
    print("Không có dữ liệu")
    sc.stop()
    exit()

best = data[0]

print("\nBỘ PHIM CÓ ĐIỂM TRUNG BÌNH CAO NHẤT (CÓ ÍT NHẤT 5 LƯỢT ĐÁNH GIÁ)")
print("-" * 40)
print("Title : {}".format(best[1][1]))
print("ID    : {}".format(best[0]))
print("Avg   : {:.2f}".format(best[1][0][0]))
print("Count : {}".format(best[1][0][1]))

print("\nDANH SÁCH TOÀN BỘ PHIM")
print("-" * 90)

print("{:<6} | {:<60} | {:<11} | {:<10}".format(
    "ID", "Title", "Avg", "Count"
))
print("-" * 90)

for x in data:
    movie_id = x[0]
    title = x[1][1]
    avg = x[1][0][0]
    count = x[1][0][1]

    print("{:<6} | {:<60} | {:<11.2f} | {:<10}".format(
        movie_id, title, avg, count
    ))

sc.stop()