# MySQL Database Container Guide

This guide explains how to manage the MySQL database container for the RPA Land Use Change Application.

## Container Details

- **Container Name:** `rpa-mysql-container`
- **Image Name:** `rpa-mysql`
- **Port:** 3306 (exposed to host)
- **Data Directory:** `./mysql_data/` (persistent volume)

## Database Connection Details

- **Host:** localhost (or 127.0.0.1)
- **Port:** 3306
- **Database:** rpa_mysql_db
- **Username:** mihiarc
- **Password:** survista683

## Managing the Container

### Starting the Container

```bash
# Start a new container
docker run -d \
  --name rpa-mysql-container \
  -p 3306:3306 \
  -v "$(pwd)/mysql_data:/var/lib/mysql" \
  rpa-mysql

# Or start an existing stopped container
docker start rpa-mysql-container
```

### Stopping the Container

```bash
docker stop rpa-mysql-container
```

### Checking Container Status

```bash
# View running containers
docker ps

# View all containers (including stopped ones)
docker ps -a

# View container logs
docker logs rpa-mysql-container

# Follow container logs
docker logs -f rpa-mysql-container
```

### Accessing MySQL

```bash
# Connect to MySQL from inside the container
docker exec -it rpa-mysql-container mysql -u mihiarc -p

# Execute SQL commands directly
docker exec -it rpa-mysql-container mysql -u mihiarc -p rpa_mysql_db -e "YOUR_SQL_COMMAND"
```

### Database Maintenance

```bash
# Create a backup
docker exec rpa-mysql-container mysqldump -u mihiarc -p rpa_mysql_db > backup.sql

# Restore from backup
cat backup.sql | docker exec -i rpa-mysql-container mysql -u mihiarc -p rpa_mysql_db
```

## Database Schema

The database includes the following tables:

1. `scenarios`
   - Stores scenario information (GCM, RCP, SSP)
   - Primary key: scenario_id

2. `time_steps`
   - Tracks time periods
   - Primary key: time_step_id
   - Unique constraint on (start_year, end_year)

3. `counties`
   - Stores FIPS codes and county names
   - Primary key: fips_code

4. `land_use_transitions`
   - Main table storing land use changes
   - Primary key: transition_id
   - Foreign keys to scenarios, time_steps, and counties
   - Includes from_land_use and to_land_use as ENUM types
   - Indexed for efficient querying

## Troubleshooting

1. **Container won't start:**
   - Check if port 3306 is already in use
   - Verify the data directory permissions
   - Check container logs for errors

2. **Can't connect to database:**
   - Verify the container is running
   - Check connection credentials
   - Ensure port 3306 is not blocked by firewall

3. **Data persistence issues:**
   - Verify the volume mount path
   - Check permissions on mysql_data directory
   - Ensure sufficient disk space

## Rebuilding the Container

If you need to rebuild the container:

1. Stop and remove the existing container:
```bash
docker stop rpa-mysql-container
docker rm rpa-mysql-container
```

2. Rebuild the image:
```bash
docker build -t rpa-mysql -f Dockerfile.dockerfile .
```

3. Start a new container:
```bash
docker run -d \
  --name rpa-mysql-container \
  -p 3306:3306 \
  -v "$(pwd)/mysql_data:/var/lib/mysql" \
  rpa-mysql
```

Note: The database data will persist in the mysql_data directory even when rebuilding the container. 