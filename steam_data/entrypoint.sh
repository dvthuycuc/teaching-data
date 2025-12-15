#!/bin/bash
while true; do
  now=$(date +%H:%M)
  if [ "$now" = "17:00" ]; then
    Rscript /app/update_steam_num_players.R
    sleep 61
  fi
  sleep 20
done