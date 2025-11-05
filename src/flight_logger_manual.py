import time, csv, math
from codrone_edu.drone import *

LOG_FILE = "drone_flight_log_manualFlight_3.csv"  # Output CSV path
SAMPLE_RATE_HZ = 10                               # Target log frequency (samples/sec)

drone = Drone()   # Create drone
drone.pair()      # Pair/connect to the physical drone

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
           dt  = max(now - prev_t, 1e-6)         # time since last loop (avoid divide-by-zero)
           # Read sensors
           h     = drone.get_height("cm")        # Altitude
           roll  = drone.get_angle_x()           # Roll angle
           pitch = drone.get_angle_y()           # Pitch angle
           yaw   = drone.get_angle_z()           # Yaw angl
           batt  = drone.get_battery()           # Battery %
           acc   = drone.get_accel_x()           # X-axis acceleration
           temp  = drone.get_drone_temperature() # Temperature

           # Horizontal speed
           vx = drone.get_flow_velocity_x()      # +X forward/back velocity
           vy = drone.get_flow_velocity_y()      # +Y left/right velocity
           speed_xy = math.hypot(vx, vy)      # since where only finding the horizontal speed we are using Pythagorean theorem to calculate speed
           # Vertical speed from height derivative
           vz = (h - prev_h) / dt

           # write a row and save it
           writer.writerow([t, h, roll, pitch, yaw, batt, acc, temp, speed_xy, vz])
           f.flush()  # force write to disk

           # print output
           print(
               f"t={t:5.2f}s  h={h:6.1f}cm  roll={roll:6.1f}  pitch={pitch:6.1f}  "
               f"yaw={yaw:6.1f}  batt={batt:3d}%  ax={acc:6.2f}  temp={temp}  "
               f"vxy={speed_xy:6.1f}  vz={vz:6.1f}"
           )

           # Update for next loop
           prev_h = h
           prev_t = now

           # Keep SAMPLE_RATE_HZ timing
           time.sleep(1 / SAMPLE_RATE_HZ)

   except KeyboardInterrupt:
       #stop when user presses Ctrl+C
       print("\nStop requested.")
   finally:
       #land and close connection safely
       try:
           drone.land()
           drone.close()
       except Exception:
           # Ignore any shutdown errors to avoid masking the save message
           pass
       print(f"Saved: {LOG_FILE}")
