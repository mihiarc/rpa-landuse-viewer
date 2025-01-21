CREATE DATABASE IF NOT EXISTS rpa_mysql_db;
CREATE USER IF NOT EXISTS 'mihiarc'@'%' IDENTIFIED BY 'survista683';
GRANT ALL PRIVILEGES ON rpa_mysql_db.* TO 'mihiarc'@'%';
FLUSH PRIVILEGES;

USE rpa_mysql_db;

CREATE TABLE scenarios (
  scenario_id INT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
  scenario_name VARCHAR(255) UNIQUE NOT NULL,
  gcm VARCHAR(255) NOT NULL,
  rcp VARCHAR(255) NOT NULL,
  ssp VARCHAR(255) NOT NULL
);

CREATE TABLE time_steps (
  time_step_id INT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
  start_year YEAR NOT NULL,
  end_year YEAR NOT NULL,
  UNIQUE KEY (start_year, end_year)
);

CREATE TABLE counties (
  fips_code VARCHAR(5) PRIMARY KEY,
  county_name VARCHAR(255)
);

CREATE TABLE land_use_transitions (
  transition_id INT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
  scenario_id INT UNSIGNED NOT NULL,
  time_step_id INT UNSIGNED NOT NULL,
  fips_code VARCHAR(5) NOT NULL,
  from_land_use ENUM('Crop', 'Pasture', 'Range', 'Forest', 'Urban') NOT NULL,
  to_land_use ENUM('Crop', 'Pasture', 'Range', 'Forest', 'Urban') NOT NULL,
  acres DOUBLE PRECISION NOT NULL,
  FOREIGN KEY (scenario_id) REFERENCES scenarios(scenario_id),
  FOREIGN KEY (time_step_id) REFERENCES time_steps(time_step_id),
  FOREIGN KEY (fips_code) REFERENCES counties(fips_code),
  INDEX idx_land_use_transitions (scenario_id, time_step_id, fips_code)
);
