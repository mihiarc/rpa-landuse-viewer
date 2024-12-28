# Use the official MySQL image
FROM mysql:8.0

# Set environment variables for MySQL configuration.  
# These are crucial and should be set appropriately.
ENV MYSQL_ROOT_PASSWORD=tArpar-gowme1-nysjaj
ENV MYSQL_DATABASE=rpa_mysql_db
ENV MYSQL_USER=mihiarc
ENV MYSQL_PASSWORD=survista683

# Expose the MySQL port
EXPOSE 3306

# Set the default timezone (optional but recommended)
ENV TZ=UTC

# Copy the initialization script (explained below)
COPY init.sql /docker-entrypoint-initdb.d/

# Set the command to start MySQL (using the correct command)
CMD ["mysqld"]
