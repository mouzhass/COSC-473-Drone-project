import time, csv, math
from codrone_edu.drone import *


LOG_FILE = "drone_flight_log_manualFlight_3.csv"
SAMPLE_RATE_HZ = 10


drone = Drone()
drone.pair()


print("Logging started. Fly manually. Press Ctrl+C to stop.")
with open(LOG_FILE, "w", newline="") as f:
   writer = csv.writer(f)
   writer.writerow([
       "t_s","height_cm","roll_deg","pitch_deg","yaw_deg","batt_%","accel_x","temp_c",
       "speed_cm_s","vz_cm_s"
   ])


   start = time.time()
   prev_h = drone.get_height("cm")
   prev_t = start


   try:
       while True:
           now = time.time()
           t   = round(now - start, 2)
           dt  = max(now - prev_t, 1e-6)  # avoid divide-by-zero


           # Sensors
           h     = drone.get_height("cm")
           roll  = drone.get_angle_x()
           pitch = drone.get_angle_y()
           yaw   = drone.get_angle_z()
           batt  = drone.get_battery()
           acc   = drone.get_accel_x()
           temp  = drone.get_drone_temperature()


           vx = drone.get_flow_velocity_x() # +X forward/back
           vy = drone.get_flow_velocity_y() # +Y left/right
           speed_xy = math.hypot(vx, vy)
           vz = (h - prev_h) / dt # vertical speed (cm/s)


           writer.writerow([t, h, roll, pitch, yaw, batt, acc, temp, speed_xy, vz])
           f.flush()


           print(
               f"t={t:5.2f}s  h={h:6.1f}cm  roll={roll:6.1f}  pitch={pitch:6.1f}  "
               f"yaw={yaw:6.1f}  batt={batt:3d}%  ax={acc:6.2f}  temp={temp}  "
               f"vxy={speed_xy:6.1f}  vz={vz:6.1f}"
           )


           prev_h = h
           prev_t = now
           time.sleep(1 / SAMPLE_RATE_HZ)


   except KeyboardInterrupt:
       print("\nStop requested.")
   finally:
       try:
           drone.land()
           drone.close()
       except Exception:
           pass
       print(f"Saved: {LOG_FILE}")