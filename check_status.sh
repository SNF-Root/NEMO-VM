#!/bin/bash

echo "=== Nemo Automation Status Check ==="
echo ""

# Check if cron job is set up
echo "Cron jobs:"
crontab -l | grep nemo || echo "No nemo cron job found"

echo ""
echo "Recent log entries (last 20 lines):"
if [ -f ~/nemo_automation/nemo_log.txt ]; then
    tail -20 ~/nemo_automation/nemo_log.txt
else
    echo "No log file found"
fi

echo ""
echo "Last run time:"
if [ -f ~/nemo_automation/nemo_log.txt ]; then
    grep "Process completed successfully" ~/nemo_automation/nemo_log.txt | tail -1
else
    echo "No successful runs found"
fi

echo ""
echo "Disk usage:"
du -sh ~/nemo_automation/

echo ""
echo "Python process status:"
ps aux | grep python | grep nemo || echo "No nemo Python processes running" 