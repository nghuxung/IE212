from pyspark import SparkContext

sc = SparkContext("local[*]", "Bai3")
sc.setLogLevel("ERROR")

# ĐỌC FILE
base = "file:///Users/nghuxung/Downloads/IE212/Lab3/"

users = sc.textFile(base + "input/users.txt")
movies = sc.textFile(base + "input/movies.txt")
ratings1 = sc.textFile(base + "input/ratings_1.txt")
ratings2 = sc.textFile(base + "input/ratings_2.txt")

ratings = ratings1.union(ratings2)

users_rdd = users.map(lambda x: x.split(","))
movies_rdd = movies.map(lambda x: x.split(","))
ratings_rdd = ratings.map(lambda x: x.split(","))

# UserID → Gender
user_gender = users_rdd.map(
    lambda x: (x[0].strip(), x[1].strip())   # (UserID, Gender)
)

# MovieID → Title
movie_map = movies_rdd.map(
    lambda x: (x[0].strip(), x[1].strip())
)

# Rating + Gender
rating_map = ratings_rdd.map(
    lambda x: (x[0].strip(), (x[1].strip(), float(x[2])))
)
# (UserID → (MovieID, Rating))

# join → (UserID → ((MovieID, Rating), Gender))
joined = rating_map.join(user_gender)

# =========================
# ((MovieID, Gender) → (rating,1))
# =========================
movie_gender_rating = joined.map(
    lambda x: ((x[1][0][0], x[1][1]), (x[1][0][1], 1))
)

# TÍNH AVG
reduce_data = movie_gender_rating.reduceByKey(
    lambda a, b: (a[0] + b[0], a[1] + b[1])
)

avg_data = reduce_data.mapValues(
    lambda x: (round(x[0] / x[1], 2), x[1])
)

formatted = avg_data.map(
    lambda x: (x[0][0], (x[0][1], x[1][0], x[1][1]))
)

result = formatted.join(movie_map)

# SORT
sorted_result = result.sortBy(
    lambda x: (x[0], x[1][1])  # (MovieID, Gender)
)

data = sorted_result.collect()

# OUTPUT
if len(data) == 0:
    print("Không có dữ liệu")
    sc.stop()
    exit()

print("\nĐIỂM TRUNG BÌNH THEO PHIM & GIỚI TÍNH")
print("-" * 110)

print("{:<6} | {:<60} | {:<8} | {:<10} | {:<10}".format(
    "ID", "Title", "Gender", "Avg", "Count"
))
print("-" * 110)

for x in data:
    movie_id = x[0]
    gender = x[1][0][0]
    avg = x[1][0][1]
    count = x[1][0][2]
    title = x[1][1]

    print("{:<6} | {:<60} | {:<8} | {:<10.2f} | {:<10}".format(
        movie_id, title, gender, avg, count
    ))

sc.stop()