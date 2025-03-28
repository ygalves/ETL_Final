CREATE TABLE IF NOT EXISTS EU
(
  EU_ID        varchar(20) UNIQUE,
  EU_name      varchar(400),
  Description  varchar(400),
  Type         varchar(400),
  LogID        bigserial    NOT NULL,
  Phases_LogID bigserial    NOT NULL,
  PRIMARY KEY (LogID)
);

COMMENT ON COLUMN EU.EU_ID IS 'Engineering Units ID';
COMMENT ON COLUMN EU.EU_name IS 'Engineering Units label';
COMMENT ON COLUMN EU.Description IS 'Engineering Units desc';
COMMENT ON COLUMN EU.Type IS 'phase type';
COMMENT ON COLUMN EU.LogID IS 'table auto inc id';
COMMENT ON COLUMN EU.Phases_LogID IS 'foreign key phases table';

CREATE TABLE IF NOT EXISTS Events
(
  Datetime            timestamptz NOT NULL UNIQUE,
  Year                smallint    NOT NULL,
  Quarter             smallint    NOT NULL,
  Month               smallint    NOT NULL,
  MonthName           text        NOT NULL,
  Week                smallint    NOT NULL,
  WeekDay             smallint    NOT NULL,
  DayOfYear           smallint    NOT NULL,
  Day                 smallint    NOT NULL,
  DayName             text        NOT NULL,
  Hour                smallint    NOT NULL,
  Minute              smallint    NOT NULL,
  Second              smallint    NOT NULL,
  shift_8H            smallint    NOT NULL,
  Formatted_Timestamp text        NOT NULL,
  LogID               bigserial   NOT NULL,
  Phases_LogID        bigserial   NOT NULL,
  PRIMARY KEY (LogID)
);

COMMENT ON TABLE Events IS 'Event Log';
COMMENT ON COLUMN Events.Datetime IS 'UTC Tiezone-aware';
COMMENT ON COLUMN Events.Year IS 'year number';
COMMENT ON COLUMN Events.Quarter IS 'quarte number';
COMMENT ON COLUMN Events.Month IS 'month number';
COMMENT ON COLUMN Events.MonthName IS 'month name';
COMMENT ON COLUMN Events.Week IS 'week number';
COMMENT ON COLUMN Events.WeekDay IS 'day of a week';
COMMENT ON COLUMN Events.DayOfYear IS 'day of a year (0-365)';
COMMENT ON COLUMN Events.Day IS 'day of a month';
COMMENT ON COLUMN Events.DayName IS 'day name';
COMMENT ON COLUMN Events.shift_8H IS 'shifts of 8 hours';
COMMENT ON COLUMN Events.Formatted_Timestamp IS 'timestamp serialized';
COMMENT ON COLUMN Events.LogID IS 'table auto inc id';
COMMENT ON COLUMN Events.Phases_LogID IS 'foreign key phases table';

CREATE TABLE IF NOT EXISTS Lots
(
  Lot_ID              VARCHAR(20) NOT NULL UNIQUE,
  Type                VARCHAR(2),
  Train_ID            VARCHAR(2),
  Train               VARCHAR(400),
  Prod_ID             VARCHAR(20),
  Prod_Name           VARCHAR(400),
  First_Date          timestamptz,
  Last_Date           timestamptz,
  Duration            interval,
  Duration_sec        int,
  Duration_min        float8,
  Formatted_Timestamp text,
  LogID               bigserial    NOT NULL,
  Phases_LogID        bigserial    NOT NULL,
  PRIMARY KEY (LogID)
);

COMMENT ON TABLE Lots IS 'Produced Lots';
COMMENT ON COLUMN Lots.Lot_ID IS 'Lot unique id';
COMMENT ON COLUMN Lots.Type IS 'type of product';
COMMENT ON COLUMN Lots.Prod_Name IS 'product label';
COMMENT ON COLUMN Lots.First_Date IS 'first register datetime';
COMMENT ON COLUMN Lots.Last_Date IS 'last register datetime';
COMMENT ON COLUMN Lots.Duration IS 'difference between first and last datetime';
COMMENT ON COLUMN Lots.Formatted_Timestamp IS 'timestamp serialized';
COMMENT ON COLUMN Lots.LogID IS 'table auto inc id';
COMMENT ON COLUMN Lots.Phases_LogID IS 'foreign key phases table';

CREATE TABLE IF NOT EXISTS Phases
(
  DateTime            timestamptz,
  Unit                varchar(400),
  Phase_ID            varchar(20) NOT NULL UNIQUE,
  Value               float8,
  EU                  varchar(20),
  ID                  VARCHAR(20),
  Formatted_Timestamp text,
  LogID               bigserial   NOT NULL,
  Events_LogID        bigserial   NOT NULL,
  Lots_LogID          bigserial   NOT NULL,
  EU_LogID            bigserial   NOT NULL,
  PRIMARY KEY (LogID)
);

COMMENT ON COLUMN Phases.Formatted_Timestamp IS 'timestamp serialized';
COMMENT ON COLUMN Phases.LogID IS 'table auto inc id';
COMMENT ON COLUMN Phases.Events_LogID IS 'foreign key Events table';
COMMENT ON COLUMN Phases.Lots_LogID IS 'foreign key Lots table';
COMMENT ON COLUMN Phases.EU_LogID IS 'foreign key EU table';
