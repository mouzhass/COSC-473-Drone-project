import time, csv, math, json
from codrone_edu.drone import *
from kafka import KafkaProducer

LOG_FILE = "drone_flight_log_milestone2_flight_1.csv"  # Output CSV path
SAMPLE_RATE_HZ = 10                               # Target log frequency (samples/sec)
FLIGHT_DURATION_S = 60                            # Target flight/log duration in seconds (~1 minute)
KAFKA_TOPIC = "drone_telemetry"                  # Kafka topic name
KAFKA_BOOTSTRAP = "localhost:9092"               # Kafka bootstrap server

drone = Drone()   # Create drone
drone.pair()      # Pair/connect to the physical drone

# Create Kafka producer for streaming telemetry
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

print("Logging started. Fly manually. Press Ctrl+C to stop.")

# Open CSV file and write header row
with open(LOG_FILE, "w", newline="") as f:
   writer = csv.writer(f)
   writer.writerow([
       "t_s","height_cm","roll_deg","pitch_deg","yaw_deg","batt_%","accel_x","temp_c",
       "speed_cm_s","vz_cm_s"
   ])


   start = time.time()             # start timestamp
   prev_h = drone.get_height("cm") # Last-sample height (for vertical speed)
   prev_t = start                  # Last-sample timestamp

   # Main logging loop
   try:
       while True:
           now = time.time()                     # Current timestamp
           t   = round(now - start, 2)           # Elapsed time since start

           # Stop automatically after the configured flight duration
           if t >= FLIGHT_DURATION_S:
               break

           dt  = max(now - prev_t, 1e-6)         # time since last loop (avoid divide-by-zero)

           # Read sensors
           h     = drone.get_height("cm")        # Altitude (cm)
           roll  = drone.get_angle_x()           # Roll angle  (x-axis)
           pitch = drone.get_angle_y()           # Pitch angle (y-axis)
           yaw   = drone.get_angle_z()           # Yaw angle   (z-axis)
           batt  = drone.get_battery()           # Battery %
           acc   = drone.get_accel_x()           # X-axis acceleration
           temp  = drone.get_drone_temperature() # Temperature

           # Horizontal speed
           vx = drone.get_flow_velocity_x()      # +X forward/back velocity
           vy = drone.get_flow_velocity_y()      # +Y left/right velocity
           # Since we're only finding the horizontal speed we are using Pythagorean theorem to calculate speed
           speed_xy = math.hypot(vx, vy)

           # Vertical speed from height derivative
           vz = (h - prev_h) / dt

           # --- Send required telemetry to Kafka ---
           kafka_msg = {
               "timestamp": now,              # epoch seconds
               "height": float(h),           # cm
               "pitch": float(pitch),
               "roll": float(roll),
               "yaw": float(yaw),
               "battery": float(batt),
           }
           producer.send(KAFKA_TOPIC, kafka_msg)

           # --- Write full metrics row to CSV ---
           writer.writerow([t, h, roll, pitch, yaw, batt, acc, temp, speed_xy, vz])
           f.flush()  # force write to disk

           # Print output to console for monitoring
           print(
               f"t={t:5.2f}s  h={h:6.1f}cm  roll={roll:6.1f}  pitch={pitch:6.1f}  "
               f"yaw={yaw:6.1f}  batt={batt:3d}%  ax={acc:6.2f}  temp={temp}  "
               f"vxy={speed_xy:6.1f}  vz={vz:6.1f}"
           )

           # Update for next loop
           prev_h = h
           prev_t = now

           # Keep SAMPLE_RATE_HZ timing (this controls readings per second)
           time.sleep(1 / SAMPLE_RATE_HZ)

   except KeyboardInterrupt:
       #stop when user presses Ctrl+C
       print("\nStop requested.")
   finally:
       # Flush Kafka and land/close connection safely
       try:
           producer.flush()
       except Exception:
           # Ignore producer shutdown errors
           pass

       try:
           drone.land()
           drone.close()
       except Exception:
           # Ignore any shutdown errors to avoid masking the save message
           pass

       print(f"Saved: {LOG_FILE}")
