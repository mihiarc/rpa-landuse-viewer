CREATE TABLE scenarios(
  scenario_id INTEGER PRIMARY KEY AUTOINCREMENT,
  scenario_name TEXT UNIQUE NOT NULL,
  gcm TEXT NOT NULL,
  rcp TEXT NOT NULL,
  ssp TEXT NOT NULL
);
CREATE TABLE sqlite_sequence(name,seq);
CREATE TABLE time_steps(
  time_step_id INTEGER PRIMARY KEY AUTOINCREMENT,
  start_year INTEGER NOT NULL,
  end_year INTEGER NOT NULL,
  UNIQUE(start_year, end_year)
);
CREATE TABLE counties(fips_code TEXT PRIMARY KEY,
county_name TEXT);
CREATE TABLE land_use_transitions(
  transition_id INTEGER PRIMARY KEY AUTOINCREMENT,
  scenario_id INTEGER NOT NULL,
  time_step_id INTEGER NOT NULL,
  fips_code TEXT NOT NULL,
  from_land_use TEXT NOT NULL,
  to_land_use TEXT NOT NULL,
  acres REAL NOT NULL,
  FOREIGN KEY(scenario_id) REFERENCES scenarios(scenario_id),
  FOREIGN KEY(time_step_id) REFERENCES time_steps(time_step_id),
  FOREIGN KEY(fips_code) REFERENCES counties(fips_code)
);
CREATE INDEX idx_land_use_transitions ON land_use_transitions(
  scenario_id,
  time_step_id,
  fips_code
);
