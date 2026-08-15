FROM bde2020/hadoop-base:2.0.0-hadoop3.2.1-java8

# bde2020/hadoop-base ships the entrypoint.sh that turns HDFS_CONF_* / CORE_CONF_*
# env vars (from hadoop.env) into core-site.xml / hdfs-site.xml.
# There is no dedicated bde2020/hadoop-journalnode image, so we reuse hadoop-base
# and just point the command at the journalnode daemon.

HEALTHCHECK CMD echo "healthy" || exit 1

CMD ["hdfs", "journalnode"]
