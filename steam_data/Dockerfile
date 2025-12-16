FROM rocker/r-base:4.4.1

RUN apt-get update && apt-get install -y --no-install-recommends \
    cron \
    tzdata \
    libcurl4-openssl-dev \
    libssl-dev \
    libxml2-dev \
    && rm -rf /var/lib/apt/lists/*

ENV TZ=Europe/Berlin

RUN R -e "install.packages(c('jsonlite','dplyr','DBI','duckdb','lubridate','httr'), repos='https://cloud.r-project.org')"

WORKDIR /app
COPY update_steam_num_players.R /app/update_steam_num_players.R

COPY crontab /etc/cron.d/steam-cron
RUN chmod 0644 /etc/cron.d/steam-cron
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

CMD ["/entrypoint.sh"]
