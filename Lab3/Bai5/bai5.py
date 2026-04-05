from pyspark import SparkContext

sc = SparkContext("local[*]", "Bai5")

# ĐỌC FILE
base = "file:///Users/nghuxung/Downloads/IE212/Lab3/"

ratings1 = sc.textFile(base + "input/ratings_1.txt")
ratings2 = sc.textFile(base + "input/ratings_2.txt")
users = sc.textFile(base + "input/users.txt")
occupations = sc.textFile(base + "input/OCCUPATION.txt")

ratings = ratings1.union(ratings2)

# PARSE
ratings_rdd = ratings.map(lambda x: x.split(","))
users_rdd = users.map(lambda x: x.split(","))
occ_rdd = occupations.map(lambda x: x.split(","))

# MAP USER → OCCUPATION ID
user_map = users_rdd.map(
    lambda x: (x[0].strip(), x[3].strip())  # (UserID, OccID)
)

# MAP OCC ID → NAME
occ_map = occ_rdd.map(
    lambda x: (x[0].strip(), x[1].strip())  # (OccID, Name)
)

# MAP RATING
rating_map = ratings_rdd.map(
    lambda x: (x[0].strip(), float(x[2]))  # (UserID, Rating)
)

# JOIN USER + RATING
join1 = rating_map.join(user_map)
# (UserID, (Rating, OccID))

# MAP → (OccID, (rating,1))
mapped = join1.map(
    lambda x: (x[1][1], (x[1][0], 1))
)

# REDUCE
reduced = mapped.reduceByKey(
    lambda a, b: (a[0] + b[0], a[1] + b[1])
)

# AVG
avg = reduced.mapValues(
    lambda x: (round(x[0] / x[1], 2), x[1])
)

# JOIN OCC NAME
final = avg.join(occ_map)
# (OccID, ((Avg, Count), Name))

# SORT
sorted_result = final.sortBy(
    lambda x: x[1][0][0], ascending=False
)

data = sorted_result.collect()

# OUTPUT
print("\nĐÁNH GIÁ THEO NGHỀ NGHIỆP")
print("-" * 80)
print(f"{'ID':<5} | {'Occupation':<15} | {'Avg':<12} | {'Count':<10}")
print("-" * 80)

for x in data:
    occ_id = x[0]
    avg_score = x[1][0][0]
    count = x[1][0][1]
    occ_name = x[1][1]

    print(f"{occ_id:<5} | {occ_name:<15} | {avg_score:<12} | {count:<10}")

sc.stop()