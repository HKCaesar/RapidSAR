CREATE TABLE files (
       id TEXT PRIMARY KEY, -- Measurement file name
       directory TEXT, -- .SAFE directory location
       track INTEGER, -- Integer relative track number
       orbit_direction TEXT, -- pass direction, (A) ascending or (D) descending
       swath INTEGER, -- Swath of measurement file
       pol TEXT, -- Polarisation of file
       date INTEGER    -- Acquisition date
);

CREATE TABLE bursts (
       id TEXT PRIMARY KEY, -- Unique burst id, made up of track, swath and time since ascending node
       track INTEGER, -- Integer relative track number
       orbit_direction TEXT, -- Pass direction
       swath INTEGER, -- Swath number
       burstid INTEGER, -- Time since ascending node in deciseconds
       center_lat REAL, -- Center coordinate latitude
       center_lon REAL, -- Center coordinate longitude
       corner1_lat REAL, -- Corner latitude
       corner1_lon REAL, -- Corner longitude
       corner2_lat REAL,
       corner2_lon REAL,
       corner3_lat REAL,
       corner3_lon REAL,
       corner4_lat REAL,
       corner4_lon REAL
);

CREATE TABLE files_bursts (
       file_id TEXT, -- Relational database between files and bursts
       burst_id TEXT,
       burst_no INTEGER -- Burst number in file, needed for Gamma
);

CREATE TABLE tracks_procdirs (
       track INTEGER,
       proc_dir TEXT
);

INSERT INTO tracks_procdirs VALUES (111,"/nfs/a1/insar/sentinel1/Iceland/VatnaNVZDesc/");
