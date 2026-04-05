from pyspark import SparkContext

sc = SparkContext("local[*]", "Bai4")

# ĐỌC FILE
base = "file:///Users/nghuxung/Downloads/IE212/Lab3/"

movies = sc.textFile(base + "input/movies.txt")
ratings1 = sc.textFile(base + "input/ratings_1.txt")
ratings2 = sc.textFile(base + "input/ratings_2.txt")
users = sc.textFile(base + "input/users.txt")

ratings = ratings1.union(ratings2)

# HÀM PHÂN NHÓM TUỔI
def age_group(age):
    age = int(age)
    if age < 25:
        return "18-24"
    elif age <= 34:
        return "25-34"
    elif age <= 44:
        return "35-44"
    elif age <= 54:
        return "45-54"
    else:
        return "55+"

movies_rdd = movies.map(lambda x: x.split(","))
ratings_rdd = ratings.map(lambda x: x.split(","))
users_rdd = users.map(lambda x: x.split(","))

# MAP
movie_map = movies_rdd.map(
    lambda x: (x[0].strip(), x[1].strip())
)

user_map = users_rdd.map(
    lambda x: (x[0].strip(), age_group(x[2]))
)

rating_map = ratings_rdd.map(
    lambda x: (x[0].strip(), (x[1].strip(), float(x[2])))
)
# (UserID, (MovieID, Rating))

# JOIN USER → RATING
join1 = rating_map.join(user_map)
# (UserID, ((MovieID, Rating), AgeGroup))

# MAP → (MovieID, AgeGroup)
mapped = join1.map(
    lambda x: (
        (x[1][0][0], x[1][1]),  # (MovieID, AgeGroup)
        (x[1][0][1], 1)         # (Rating, 1)
    )
)

# REDUCE
reduced = mapped.reduceByKey(
    lambda a, b: (a[0] + b[0], a[1] + b[1])
)

avg = reduced.mapValues(
    lambda x: (round(x[0] / x[1], 2), x[1])
)

# JOIN MOVIE TITLE
result = avg.map(
    lambda x: (x[0][0], (x[0][1], x[1][0], x[1][1]))
)
# (MovieID, (AgeGroup, Avg, Count))

final = result.join(movie_map)
# (MovieID, ((AgeGroup, Avg, Count), Title))

# SORT 
sorted_result = final.sortBy(
    lambda x: (x[1][1], x[1][0][0])
)

data = sorted_result.collect()

# OUTPUT
print("\nĐIỂM TRUNG BÌNH THEO PHIM & NHÓM TUỔI")
print("-" * 110)
print(f"{'ID':<6} | {'Title':<60} | {'Age Group':<10} | {'Avg':<10} | {'Count':<10}")
print("-" * 110)

for x in data:
    movie_id = x[0]
    age_group = x[1][0][0]
    avg_score = x[1][0][1]
    count = x[1][0][2]
    title = x[1][1]

    print(f"{movie_id:<6} | {title:<60} | {age_group:<10} | {avg_score:<10} | {count:<10}")

sc.stop()