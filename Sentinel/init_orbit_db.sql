CREATE TABLE porbits (
       id TEXT PRIMARY KEY, -- EOF Orbit file name
       directory TEXT, -- Directory containing file
       begintime TIME, -- Validity begin time
       endtime TIME -- Validity end time
);
CREATE TABLE rorbits (
       id TEXT PRIMARY KEY, -- EOF Orbit file name
       directory TEXT, -- Directory containing file
       begintime TIME, -- Validity begin time
       endtime TIME -- Validity end time
);
