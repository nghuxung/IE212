from pyspark import SparkContext

sc = SparkContext("local[*]", "Bai2")
sc.setLogLevel("ERROR")

# ĐỌC FILE
base = "file:///Users/nghuxung/Downloads/IE212/Lab3/"

movies = sc.textFile(base + "input/movies.txt")
ratings1 = sc.textFile(base + "input/ratings_1.txt")
ratings2 = sc.textFile(base + "input/ratings_2.txt")

ratings = ratings1.union(ratings2)

movies_rdd = movies.map(lambda x: x.split(","))
ratings_rdd = ratings.map(lambda x: x.split(","))

movie_genres = movies_rdd.map(
    lambda x: (x[0].strip(), x[2].strip().split("|"))
)

rating_map = ratings_rdd.map(
    lambda x: (x[1].strip(), float(x[2]))
)

# JOIN → (MovieID, (rating, [genres]))
joined = rating_map.join(movie_genres)

# FLATMAP → (genre, rating)
genre_rating = joined.flatMap(
    lambda x: [(genre, (x[1][0], 1)) for genre in x[1][1]]
)

# TÍNH AVG
genre_reduce = genre_rating.reduceByKey(
    lambda a, b: (a[0] + b[0], a[1] + b[1])
)

genre_avg = genre_reduce.mapValues(
    lambda x: (round(x[0] / x[1], 2), x[1])
)

# SORT
sorted_genre = genre_avg.sortBy(
    lambda x: x[1][0], ascending=False
)

data = sorted_genre.collect()

# OUTPUT
if len(data) == 0:
    print("Không có dữ liệu")
    sc.stop()
    exit()

print("\nDANH SÁCH THỂ LOẠI")
print("-" * 30)

print("{:<15} | {:<10}".format(
    "Thể loại", "Điểm TB"
))
print("-" * 30)

for x in data:
    genre = x[0]
    avg = x[1][0]
    count = x[1][1]

    print("{:<15} | {:<10.2f}".format(
        genre, avg
    ))

sc.stop()