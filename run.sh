#!/bin/bash

LOG_FILE="/var/log/udpserver.log"

# Function to get current date and time
current_date() {
    echo $(date '+%Y-%m-%d %H:%M:%S')
}

# Enable core dumps
ulimit -c unlimited
while true; do
    echo "[$(current_date)] Starting udpserver..." >> "$LOG_FILE"
    # Start the process and redirect its stdout and stderr to the log file
    ./UdpServer -p 5555 -b 0 >> "$LOG_FILE" 2>&1
    # If the process exits, log the crash time
    echo "[$(current_date)] udpserver crashed." >> "$LOG_FILE"
    # Optional: wait before restarting, e.g., 1 second
    sleep 1
done
