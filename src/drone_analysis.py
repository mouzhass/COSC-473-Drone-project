from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, variance, min as spark_min, max as spark_max, col


# Spark + S3A (uses your AWS env vars or ~/.aws/credentials)
spark = (
    SparkSession.builder
        .appName("Drone Flight Analysis")
        # .master("local[*]")  # uncomment when running locally
        .config("spark.jars.packages",
                "org.apache.hadoop:hadoop-aws:3.3.4,"
                "com.amazonaws:aws-java-sdk-bundle:1.12.772")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "com.amazonaws.auth.DefaultAWSCredentialsProviderChain")
        .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com")
        .getOrCreate()
)

s3_path = "s3a://drone-flight-data-hassan-oliver/drone_flight_log_manualFlight_3.csv"
df = spark.read.csv(s3_path, header=True, inferSchema=True)

spark.conf.set("spark.sql.debug.maxToStringFields", "100")
# Data quality analysis
print("\n" + "=" * 70)
print("DATA QUALITY ANALYSIS")
print("=" * 70)

total_records = df.count()
print(f"\nTotal Records: {total_records}")

# Filter valid flight data
flight_df = df.filter(
    (col("height_cm") != -100.0) &
    (col("height_cm") != 999.9) &
    (col("height_cm") > 0)
)

valid_records = flight_df.count()
invalid_records = total_records - valid_records

print(f"Valid Flight Records: {valid_records}")
print(f"Invalid/Ground Records: {invalid_records}")
print(f"Data Quality: {(valid_records / total_records) * 100:.1f}%")

# Calculate flight time
if valid_records > 0:
    flight_time_df = flight_df.agg(
        spark_min("t_s").alias("start_time"),
        spark_max("t_s").alias("end_time")
    ).collect()[0]

    flight_duration = flight_time_df['end_time'] - flight_time_df['start_time']
    print(f"\nFlight Duration: {flight_duration:.2f} seconds")
else:
    flight_duration = 0.0

# Calculate comprehensive flight statistics
print("\n" + "=" * 70)
print("DRONE FLIGHT STATISTICS")
print("=" * 70)

# Calculate statistics on valid flight data (no std dev anywhere)
stats = flight_df.select(
    # Altitude statistics
    avg("height_cm").alias("avg_altitude_cm"),
    spark_min("height_cm").alias("min_altitude_cm"),
    spark_max("height_cm").alias("max_altitude_cm"),

    # Speed statistics
    avg("speed_cm_s").alias("avg_speed_cm_s"),
    spark_min("speed_cm_s").alias("min_speed_cm_s"),
    spark_max("speed_cm_s").alias("max_speed_cm_s"),

    # Vertical velocity
    avg("vz_cm_s").alias("avg_vz_cm_s"),
    spark_max("vz_cm_s").alias("max_climb_rate"),
    spark_min("vz_cm_s").alias("max_descent_rate"),

    # Orientation (roll, pitch, yaw)
    avg("roll_deg").alias("avg_roll_deg"),
    avg("pitch_deg").alias("avg_pitch_deg"),
    avg("yaw_deg").alias("avg_yaw_deg"),
    variance("yaw_deg").alias("yaw_variance"),

    # Acceleration
    avg("accel_x").alias("avg_accel_x"),
    spark_min("accel_x").alias("min_accel_x"),
    spark_max("accel_x").alias("max_accel_x"),

    # Battery
    avg("batt_%").alias("avg_battery_pct"),
    spark_min("batt_%").alias("min_battery_pct"),
    spark_max("batt_%").alias("max_battery_pct"),

    # Temperature
    avg("temp_c").alias("avg_temp_c"),
    spark_min("temp_c").alias("min_temp_c"),
    spark_max("temp_c").alias("max_temp_c")
).collect()[0]

# Display Altitude Statistics
print(f"\nALTITUDE STATISTICS:")
print(f"  Average Altitude:    {stats['avg_altitude_cm']:.2f} cm ({stats['avg_altitude_cm'] / 100:.2f} m)")
print(f"  Minimum Altitude:    {stats['min_altitude_cm']:.2f} cm")
print(f"  Maximum Altitude:    {stats['max_altitude_cm']:.2f} cm ({stats['max_altitude_cm'] / 100:.2f} m)")
print(f"  Altitude Range:      {stats['max_altitude_cm'] - stats['min_altitude_cm']:.2f} cm")

# Display Speed Statistics
print(f"\nSPEED STATISTICS:")
print(f"  Average Speed:       {stats['avg_speed_cm_s']:.2f} cm/s ({stats['avg_speed_cm_s'] / 100:.2f} m/s)")
print(f"  Minimum Speed:       {stats['min_speed_cm_s']:.2f} cm/s")
print(f"  Maximum Speed:       {stats['max_speed_cm_s']:.2f} cm/s ({stats['max_speed_cm_s'] / 100:.2f} m/s)")
print(f"  Speed Range:         {stats['max_speed_cm_s'] - stats['min_speed_cm_s']:.2f} cm/s")

# Display Vertical Velocity
print(f"\nVERTICAL VELOCITY:")
print(f"  Average V-Speed:     {stats['avg_vz_cm_s']:.2f} cm/s")
print(f"  Max Climb Rate:      {stats['max_climb_rate']:.2f} cm/s")
print(f"  Max Descent Rate:    {stats['max_descent_rate']:.2f} cm/s")

# Display Orientation Statistics (no std dev)
print(f"\nORIENTATION STATISTICS:")
print(f"  Roll - Avg:          {stats['avg_roll_deg']:.2f}°")
print(f"  Pitch - Avg:         {stats['avg_pitch_deg']:.2f}°")
print(f"  Yaw - Avg:           {stats['avg_yaw_deg']:.2f}°")

# Display Acceleration Statistics (no std dev)
print(f"\nACCELERATION STATISTICS:")
print(f"  Average Accel-X:     {stats['avg_accel_x']:.2f}")
print(f"  Minimum Accel-X:     {stats['min_accel_x']:.2f}")
print(f"  Maximum Accel-X:     {stats['max_accel_x']:.2f}")

# Display Battery Statistics
print(f"\nBATTERY STATISTICS:")
print(f"  Average Battery:     {stats['avg_battery_pct']:.2f}%")
print(f"  Starting Battery:    {stats['max_battery_pct']:.2f}%")
print(f"  Ending Battery:      {stats['min_battery_pct']:.2f}%")
print(f"  Battery Consumed:    {stats['max_battery_pct'] - stats['min_battery_pct']:.2f}%")
if flight_duration > 0:
    battery_drain_rate = (stats['max_battery_pct'] - stats['min_battery_pct']) / flight_duration
    print(f"  Drain Rate:          {battery_drain_rate:.2f}%/second")
    estimated_flight_time = stats['min_battery_pct'] / battery_drain_rate if battery_drain_rate > 0 else 0
    print(f"  Est. Remaining Time: {estimated_flight_time:.2f} seconds")

# Display Temperature Statistics
print(f"\nTEMPERATURE STATISTICS:")
print(f"  Average Temp:        {stats['avg_temp_c']:.2f}°C")
print(f"  Minimum Temp:        {stats['min_temp_c']:.2f}°C")
print(f"  Maximum Temp:        {stats['max_temp_c']:.2f}°C")
print(f"  Temp Range:          {stats['max_temp_c'] - stats['min_temp_c']:.2f}°C")

# Flight Stability Analysis (based on yaw variance only)
print(f"\nFLIGHT STABILITY ANALYSIS:")
print(f"  Yaw Variance:        {stats['yaw_variance']:.2f} deg²")

if stats['yaw_variance'] < 10:
    stability = "EXCELLENT - Very stable flight"
elif stats['yaw_variance'] < 25:
    stability = "GOOD - Moderately stable flight"
elif stats['yaw_variance'] < 50:
    stability = "FAIR - Some instability detected"
else:
    stability = "POOR - Significant instability"

print(f"  Flight Stability:    {stability}")

# Stop Spark session
spark.stop()
