# bh3-automation

Automation of the Extent of Physical Damage marine biodiversity indicator

# Importing Data

## Marine Recorder species datapoints

The Marine recorder extract is a somewhat complicated data extraction process owing to some complexity of the source data. Currently split into two seperate Access databases at the time of writing.

The tool does not need most of the data from the source databases, only the following 5 tables are actually required here;

 - sample
 - sample_biotope_all
 - sample_species
 - survey
 - survey_event

```
  +---------------------------------------------------------+
  | sample                                                  |
  +-------------------------+-------------------------------+
  | objectid                | integer                       |
  | sample_reference        | string                        | *
  | survey_event_key        | string                        | *
  | sample_key              | string                        |
  | user_sample_ref         | string                        |
  | sample_date             | timestamp                     | * 
  | surveyors               | string                        |
  | derived_from            | string                        |
  | coordinate_system       | string                        |
  | latitude                | double                        |
  | longitude               | double                        |
  | latitude_wgs84          | double                        | x
  | longitude_wgs84         | double                        | x
  | duration                | string                        |
  | sample_time             | timestamp                     |
  | habitat                 | string                        |
  | description             | string                        |
  | image_file              | string                        |
  | classification_standard | boolean                       |
  | coord_type              | string                        |
  | start_latitude          | double                        |
  | start_longitude         | double                        |
  | start_orig              | string                        |
  | start_latitude_wgs84    | double                        |
  | start_longitude_wgs84   | double                        | 
  | end_latitude            | double                        |
  | end_longitude           | double                        |
  | end_orig                | string                        |
  | end_latitude_wgs84      | double                        |
  | end_longitudewgs84      | double                        |
  | vague_date_start        | timestamp                     |
  | vague_date_end          | timestamp                     |
  | vague_date_type         | string                        |
  | sample_comment          | string                        |
  | the_geom                | geometry(Point, 4326)         | *
  +-------------------------+-------------------------------+

  +---------------------------------------------------------+
  | sample_biotope_all                                      |
  +-----------------------------+---------------------------+
  | objectid                    | integer                   |
  | sample_key                  | string                    |
  | sample_reference            | string                    | *
  | biotope_code                | string                    | *
  | biotope_desc                | string                    | *
  | qualifier                   | string                    | 
  | determination_date          | timestamp                 |
  | sample_biotope_all_comment  | string                    |
  | assessed_by                 | string                    | 
  | determiner_status           | string                    | 
  | biotope_code_version        | string                    | 
  | biotope_occurrence_key      | string                    |
  | biotope_determination_key   | string                    |
  | vague_date_start            | timestamp                 |
  | vague_date_end              | timestamp                 | 
  | vague_date_type             | string                    |
  | preferred                   | boolean                   |
  | biotope_list_item_key       | string                    |
  +-----------------------------+---------------------------+

  +---------------------------------------------------------+
  | sample_species                                          |
  +-----------------------------+---------------------------+
  | objectid                    | integer                   |
  | taxon_occurrence_key        | string                    |
  | sample_reference            | string                    | *
  | sample_key                  | string                    |
  | species_name                | string                    | *
  | sp_qual                     | string                    |
  | taxonomic_order             | string                    | 
  | characterising              | boolean                   |
  | uncertain                   | boolean                   |
  | rep_id                      | string                    |
  | sacforn                     | string                    |
  | sp_percent                  | real                      |
  | sp_count                    | integer                   |
  | score                       | integer                   |
  | pa                          | string                    | 
  | num_per_sqm                 | real                      |
  | determined_by               | string                    | 
  | note                        | string                    |
  | taxon_determination_key     | string                    |
  | vague_date_start            | timestamp                 |
  | vague_date_end              | timestamp                 |
  | vague_date_type             | string                    | 
  | sample_species_comment      | string                    |
  | confidential                | boolean                   |
  | aphia_id                    | integer                   |
  | is_current                  | string                    | 
  | is_dead                     | boolean                   |
  | preferred_species_name      | string                    |
  | preferred_alpha_id          | integer                   |
  +-----------------------------+---------------------------+

  +---------------------------------------------------------+
  | survey                                                  |
  +-----------------------------+---------------------------+
  | objectid                    | integer                   |
  | survey_key                  | string                    | *
  | survey_name                 | string                    |
  | date_from                   | timestamp                 |
  | date_to                     | timestamp                 |
  | sw_lat                      | double                    |
  | sw_long                     | double                    |
  | sw_latitutde_wgs84          | double                    |
  | sw_longitude_wgs84          | double                    |
  | ne_latitude                 | double                    |
  | ne_longitude                | double                    |
  | ne_latitude_wgs84           | double                    |
  | ne_longitude_wgs84          | double                    |
  | coordinate_system           | string                    |
  | ne_derived_from             | string                    |
  | sw_derived_from             | string                    |
  | copyright                   | string                    |
  | data_access                 | string                    |
  | entered_date                | timestamp                 |
  | entered_by                  | string                    |
  | edited_date                 | timestamp                 |
  | edited_by                   | string                    |
  | checked_date                | timestamp                 |
  | checked_by                  | string                    |
  | project_contract            | string                    |
  | description                 | string                    |
  | references_lit              | string                    |
  | mdb_name                    | string                    |
  | from_vague_date_start       | timestamp                 |
  | from_vague_date_end         | timestamp                 |
  | from_vague_date_type        | string                    |
  | to_vague_date_start         | timestamp                 |
  | to_vague_date_end           | timestamp                 |
  | to_vague_date_type          | string                    |
  | sw_orig_coord               | string                    |
  | ne_orig_coord               | string                    |
  | survey_quality              | string                    |
  | metadata                    | string                    |
  +-----------------------------+---------------------------+

  +---------------------------------------------------------+
  | survey_event                                            |
  +-----------------------------+---------------------------+
  | objectid                    | integer                   |
  | survey_event_key            | string                    | *
  | survey_key                  | string                    | *
  | location_key                | string                    |
  | event_name                  | string                    | 
  | event_reference             | string                    |
  | event_date                  | timestamp                 |
  | derived_from                | string                    |
  | coordinate_system           | string                    |
  | latitude                    | double                    |
  | longitude                   | double                    |
  | latitude_wgs84              | double                    |
  | longitude_wgs84             | double                    |
  | spatial_type                | string                    |
  | start_latitude              | double                    |
  | start_longitude             | double                    |
  | start_orig                  | string                    |
  | start_latitude_wgs84        | double                    |
  | start_longitude_wgs84       | double                    |
  | end_latitude                | double                    |
  | end_longitude               | double                    |
  | end_orig                    | string                    |
  | end_latitude_wgs84          | double                    |
  | end_longitude_wgs84         | double                    |
  | surveyors                   | string                    |
  | depth_lower_cd              | real                      |
  | depth_upper_cd              | real                      |
  | depth_lower_sl              | real                      |
  | depth_upper_sl              | real                      |
  | tidal_currents              | real                      |
  | temp_surface                | real                      |
  | temp_bottom                 | real                      |
  | salinity                    | real                      |
  | under_water_visibility      | real                      |
  | vague_date_start            | timestamp with time zone  |
  | vague_date_end              | timestamp with time zone  |
  | vague_date_type             | string                    |
  | file_code                   | string                    |
  +-----------------------------+---------------------------+
```

Rows marked with `*` are the rows that the tool currently requires to run, rows marked with `x` are needed to generate the `the_geom` points in the `sample` table. This is done by running an update script over the imported tables i.e. 

```sql
UPDATE marinerec.sample SET the_geom = ST_SetSRID(ST_MakePoint(longitude_wgs84, latitude_wgs84), 4326);
```

Where `marinerec` is the schema data is currently sitting in.

### Database setup

To upload a new snapshot, you would need to first create a new schema (do not write to an existing schema), in general this should be `marinerec_update`, this can be done via pgadmin or your interface of choice. Once created you will need to create the tables that are needed using the following;

```sql
-- Table: marinerec_update.sample
CREATE TABLE IF NOT EXISTS marinerec_update.sample
(
    objectid integer NOT NULL,
    sample_reference character varying(20) COLLATE pg_catalog."default" NOT NULL,
    survey_event_key character varying(16) COLLATE pg_catalog."default",
    sample_key character varying(16) COLLATE pg_catalog."default",
    user_sample_ref character varying(20) COLLATE pg_catalog."default",
    sample_date timestamp with time zone,
    surveyors character varying(255) COLLATE pg_catalog."default",
    derived_from character varying(40) COLLATE pg_catalog."default",
    coordinate_system character varying(20) COLLATE pg_catalog."default",
    latitude double precision,
    longitude double precision,
    latitude_wgs84 double precision,
    longitude_wgs84 double precision,
    duration character varying(20) COLLATE pg_catalog."default",
    sample_time timestamp with time zone,
    habitat character varying(255) COLLATE pg_catalog."default",
    description text COLLATE pg_catalog."default",
    image_file text COLLATE pg_catalog."default",
    classification_standard boolean,
    coord_type character varying(50) COLLATE pg_catalog."default",
    start_latitude double precision,
    start_longitude double precision,
    start_orig character varying(50) COLLATE pg_catalog."default",
    start_latitude_wgs84 double precision,
    start_longitude_wgs84 double precision,
    end_latitude double precision,
    end_longitude double precision,
    end_orig character varying(50) COLLATE pg_catalog."default",
    end_latitude_wgs84 double precision,
    end_longitudewgs84 double precision,
    vague_date_start timestamp with time zone,
    vague_date_end timestamp with time zone,
    vague_date_type character varying(2) COLLATE pg_catalog."default",
    sample_comment text COLLATE pg_catalog."default",
    the_geom geometry(Point,4326),
    CONSTRAINT sample_pkey PRIMARY KEY (sample_reference)
)
WITH (
    OIDS = FALSE
)
TABLESPACE pg_default;

ALTER TABLE IF EXISTS marinerec_update.sample OWNER to bh3;

GRANT ALL ON TABLE marinerec_update.sample TO bh3;

CREATE INDEX IF NOT EXISTS fki_fk_sample_survey_event_survey_event_key
    ON marinerec_update.sample USING btree
    (survey_event_key COLLATE pg_catalog."default" ASC NULLS LAST)
    TABLESPACE pg_default;

CREATE UNIQUE INDEX IF NOT EXISTS ix_sample_objectid_unique
    ON marinerec_update.sample USING btree
    (objectid ASC NULLS LAST)
    TABLESPACE pg_default;

ALTER TABLE IF EXISTS marinerec_update.sample
    CLUSTER ON ix_sample_objectid_unique;

CREATE INDEX IF NOT EXISTS ix_sample_the_geom
    ON marinerec_update.sample USING gist
    (the_geom)
    TABLESPACE pg_default;

-- Table: marinerec_update.sample_biotope_all

CREATE TABLE IF NOT EXISTS marinerec_update.sample_biotope_all
(
    objectid integer NOT NULL,
    sample_key character varying(16) COLLATE pg_catalog."default",
    sample_reference character varying(20) COLLATE pg_catalog."default",
    biotope_code character varying(30) COLLATE pg_catalog."default",
    biotope_desc text COLLATE pg_catalog."default",
    qualifier character varying(50) COLLATE pg_catalog."default",
    determination_date timestamp with time zone,
    sample_biotope_all_comment text COLLATE pg_catalog."default",
    assessed_by character varying(50) COLLATE pg_catalog."default",
    determiner_status character varying(50) COLLATE pg_catalog."default",
    biotope_code_version character varying(10) COLLATE pg_catalog."default",
    biotope_occurrence_key character varying(16) COLLATE pg_catalog."default" NOT NULL,
    biotope_determination_key character varying(16) COLLATE pg_catalog."default",
    vague_date_start timestamp with time zone,
    vague_date_end timestamp with time zone,
    vague_date_type character varying(2) COLLATE pg_catalog."default",
    preferred boolean,
    biotope_list_item_key character varying(16) COLLATE pg_catalog."default",
    CONSTRAINT sample_biotope_all_pkey PRIMARY KEY (biotope_occurrence_key)
)
WITH (
    OIDS = FALSE
)
TABLESPACE pg_default;

ALTER TABLE IF EXISTS marinerec_update.sample_biotope_all
    OWNER to bh3;

GRANT ALL ON TABLE marinerec_update.sample_biotope_all TO bh3;

-- Table: marinerec_update.sample_species

CREATE TABLE IF NOT EXISTS marinerec_update.sample_species
(
    objectid integer NOT NULL,
    taxon_occurrence_key character varying(16) COLLATE pg_catalog."default" NOT NULL,
    sample_reference character varying(255) COLLATE pg_catalog."default",
    sample_key character varying(255) COLLATE pg_catalog."default",
    species_name character varying(255) COLLATE pg_catalog."default",
    sp_qual character varying(25) COLLATE pg_catalog."default",
    taxonomic_order character varying(255) COLLATE pg_catalog."default",
    characterising boolean,
    uncertain boolean,
    rep_id character varying(20) COLLATE pg_catalog."default",
    sacforn character varying(2) COLLATE pg_catalog."default",
    sp_percent real,
    sp_count integer,
    score integer,
    pa character varying(2) COLLATE pg_catalog."default",
    num_per_sqm real,
    determined_by character varying(50) COLLATE pg_catalog."default",
    note text COLLATE pg_catalog."default",
    taxon_determination_key character varying(16) COLLATE pg_catalog."default",
    vague_date_start timestamp with time zone,
    vague_date_end timestamp with time zone,
    vague_date_type character varying(2) COLLATE pg_catalog."default",
    sample_species_comment text COLLATE pg_catalog."default",
    confidential boolean,
    aphia_id integer,
    is_current character varying(10) COLLATE pg_catalog."default",
    is_dead boolean,
    preferred_species_name character varying(50) COLLATE pg_catalog."default",
    preferred_alpha_id integer,
    CONSTRAINT sample_species_pkey PRIMARY KEY (taxon_occurrence_key)
)
WITH (
    OIDS = FALSE
)
TABLESPACE pg_default;

ALTER TABLE IF EXISTS marinerec_update.sample_species OWNER to bh3;

GRANT ALL ON TABLE marinerec_update.sample_species TO bh3;

CREATE INDEX IF NOT EXISTS fki_fk_sample_species_sample_replicate_sample_key
    ON marinerec_update.sample_species USING btree
    (sample_key COLLATE pg_catalog."default" ASC NULLS LAST)
    TABLESPACE pg_default;

CREATE UNIQUE INDEX IF NOT EXISTS ix_sample_species_objectid_unique
    ON marinerec_update.sample_species USING btree
    (objectid ASC NULLS LAST)
    TABLESPACE pg_default;

ALTER TABLE IF EXISTS marinerec_update.sample_species
    CLUSTER ON ix_sample_species_objectid_unique;

-- Table: marinerec_update.survey

CREATE TABLE IF NOT EXISTS marinerec_update.survey
(
    objectid integer NOT NULL,
    survey_key character varying(16) COLLATE pg_catalog."default" NOT NULL,
    survey_name character varying(100) COLLATE pg_catalog."default",
    date_from timestamp with time zone,
    date_to timestamp with time zone,
    sw_lat double precision,
    sw_long double precision,
    sw_latitutde_wgs84 double precision,
    sw_longitude_wgs84 double precision,
    ne_latitude double precision,
    ne_longitude double precision,
    ne_latitude_wgs84 double precision,
    ne_longitude_wgs84 double precision,
    coordinate_system character varying(20) COLLATE pg_catalog."default",
    ne_derived_from character varying(40) COLLATE pg_catalog."default",
    sw_derived_from character varying(40) COLLATE pg_catalog."default",
    copyright character varying(255) COLLATE pg_catalog."default",
    data_access character varying(255) COLLATE pg_catalog."default",
    entered_date timestamp with time zone,
    entered_by character varying(50) COLLATE pg_catalog."default",
    edited_date timestamp with time zone,
    edited_by character varying(50) COLLATE pg_catalog."default",
    checked_date timestamp with time zone,
    checked_by character varying(255) COLLATE pg_catalog."default",
    project_contract character varying(50) COLLATE pg_catalog."default",
    description text COLLATE pg_catalog."default",
    references_lit text COLLATE pg_catalog."default",
    mdb_name character varying(255) COLLATE pg_catalog."default",
    from_vague_date_start timestamp with time zone,
    from_vague_date_end timestamp with time zone,
    from_vague_date_type character varying(2) COLLATE pg_catalog."default",
    to_vague_date_start timestamp with time zone,
    to_vague_date_end timestamp with time zone,
    to_vague_date_type character varying(2) COLLATE pg_catalog."default",
    sw_orig_coord character varying(30) COLLATE pg_catalog."default",
    ne_orig_coord character varying(30) COLLATE pg_catalog."default",
    survey_quality character varying(50) COLLATE pg_catalog."default",
    metadata text COLLATE pg_catalog."default",
    CONSTRAINT survey_pkey PRIMARY KEY (survey_key)
)
WITH (
    OIDS = FALSE
)
TABLESPACE pg_default;

ALTER TABLE IF EXISTS marinerec_update.survey OWNER to admin;

GRANT ALL ON TABLE marinerec_update.survey TO admin;

GRANT ALL ON TABLE marinerec_update.survey TO bh3;

CREATE UNIQUE INDEX IF NOT EXISTS ix_survey_objectid_unique
    ON marinerec_update.survey USING btree
    (objectid ASC NULLS LAST)
    TABLESPACE pg_default;

ALTER TABLE IF EXISTS marinerec_update.survey
    CLUSTER ON ix_survey_objectid_unique;

-- Table: marinerec_update.survey_event

CREATE TABLE IF NOT EXISTS marinerec_update.survey_event
(
    objectid integer NOT NULL,
    survey_event_key character varying(255) COLLATE pg_catalog."default" NOT NULL,
    survey_key character varying(50) COLLATE pg_catalog."default",
    location_key character varying(16) COLLATE pg_catalog."default",
    event_name character varying(255) COLLATE pg_catalog."default",
    event_reference character varying(50) COLLATE pg_catalog."default",
    event_date timestamp with time zone,
    derived_from character varying(50) COLLATE pg_catalog."default",
    coordinate_system character varying(20) COLLATE pg_catalog."default",
    latitude double precision,
    longitude double precision,
    latitude_wgs84 double precision,
    longitude_wgs84 double precision,
    spatial_type character varying(50) COLLATE pg_catalog."default",
    start_latitude double precision,
    start_longitude double precision,
    start_orig character varying(50) COLLATE pg_catalog."default",
    start_latitude_wgs84 double precision,
    start_longitude_wgs84 double precision,
    end_latitude double precision,
    end_longitude double precision,
    end_orig character varying(50) COLLATE pg_catalog."default",
    end_latitude_wgs84 double precision,
    end_longitude_wgs84 double precision,
    surveyors character varying(255) COLLATE pg_catalog."default",
    depth_lower_cd real,
    depth_upper_cd real,
    depth_lower_sl real,
    depth_upper_sl real,
    tidal_currents real,
    temp_surface real,
    temp_bottom real,
    salinity real,
    under_water_visibility real,
    vague_date_start timestamp with time zone,
    vague_date_end timestamp with time zone,
    vague_date_type character varying(2) COLLATE pg_catalog."default",
    file_code character varying(255) COLLATE pg_catalog."default",
    CONSTRAINT survey_event_pkey PRIMARY KEY (survey_event_key)
)
WITH (
    OIDS = FALSE
)
TABLESPACE pg_default;

ALTER TABLE IF EXISTS marinerec_update.survey_event OWNER to bh3;

GRANT ALL ON TABLE marinerec_update.survey_event TO bh3;

CREATE INDEX IF NOT EXISTS fki_fk_survey_event_location_location_key
    ON marinerec_update.survey_event USING btree
    (location_key COLLATE pg_catalog."default" ASC NULLS LAST)
    TABLESPACE pg_default;

CREATE INDEX IF NOT EXISTS fki_fk_survey_event_survey_survey_key
    ON marinerec_update.survey_event USING btree
    (survey_key COLLATE pg_catalog."default" ASC NULLS LAST)
    TABLESPACE pg_default;

CREATE UNIQUE INDEX IF NOT EXISTS ix_survey_event_objectid_unique
    ON marinerec_update.survey_event USING btree
    (objectid ASC NULLS LAST)
    TABLESPACE pg_default;

ALTER TABLE IF EXISTS marinerec_update.survey_event
    CLUSTER ON ix_survey_event_objectid_unique;
```

### Data extraction from Access DB

There are multiple paths to extract the data from the Access Databases, you can attempt to set up a link between Access and the Postgres database but this has been problematic. You can also extract data via a tool like FME or a simple script i.e.;

```python
import pyodbc
import csv
import argparse

parser = argparse.ArgumentParser(description='Extract named table from Access.')
parser.add_argument('-i', '--input', help='Input file path', required=True)
parser.add_argument('-t', '--table', help='Table name', required=True)

args = parser.parse_args()

# MS ACCESS DB CONNECTION
pyodbc.lowercase = False
conn = pyodbc.connect(
    r"Driver={Microsoft Access Driver (*.mdb, *.accdb)};" +
    r"Dbq=" + args.input + ";")

table = args.table
# OPEN CURSOR AND EXECUTE SQL
cur = conn.cursor()
#cur.execute("SELECT * FROM Survey")
cur.execute("SELECT * FROM " + table)

# OPEN CSV AND ITERATE THROUGH RESULTS
with open(table.lower() + '.csv', 'w', newline='\n', encoding='utf-8') as f:
    writer = csv.writer(f, lineterminator='\n')
    for row in cur.fetchall() :
        writer.writerow(row)

cur.close()
conn.close()
```

This will produce an output `.csv` for the specified table, supplied as a command line argument (`-t tablename`) output into the current working folder. You will need to point it at the current access database with te `-i path` argument. This script should be run in a virtualenv with pyodbc installed.

These CSV's need to be appended to a header line which matches the table headers the headers are as follows, you will need to create csvt file to help with types on import;

```
sample >
objectid,sample_reference,survey_event_key,sample_key,user_sample_ref,sample_date,surveyors,derived_from,coordinate_system,latitude,longitude,latitude_wgs84,longitude_wgs84,duration,sample_time,habitat,description,image_file,classification_standard,coord_type,start_latitude,start_longitude,start_orig,start_latitude_wgs84,start_longitude_wgs84,end_latitude,end_longitude,end_orig,end_latitude_wgs84,end_longitudewgs84,vague_date_start,vague_date_end,vague_date_type,sample_comment

sample.csvt >
"Integer","String","String","String","String","DateTime","String","String","String","Real","Real","Real","Real","String","DateTime","String","String","String","OFSTBoolean ","String","Real","Real","String","Real","Real","Real","Real","String","Real","Real","DateTime","DateTime","String","String"

sample_biotope_all >
objectid,sample_key,sample_reference,biotope_code,biotope_desc,qualifier,determination_date,sample_biotope_all_comment,assessed_by,determiner_status,biotope_code_version,biotope_occurrence_key,biotope_determination_key,vague_date_start,vague_date_end,vague_date_type,preferred,biotope_list_item_key

sample_biotope_all.csvt >
"Integer","String","String","String","String","String","DateTime","String","String","String","String","String","String","DateTime","DateTime","String","Boolean","String"

sample_species >
objectid,taxon_occurrence_key,sample_reference,sample_key,species_name,sp_qual,taxonomic_order,characterising,uncertain,rep_id,sacforn,sp_percent,sp_count,score,pa,num_per_sqm,determined_by,note,taxon_determination_key,vague_date_start,vague_date_end,vague_date_type,sample_species_comment,confidential,aphia_id,is_current,is_dead

sample_species.csvt >
"Integer","String","String","String","String","String","String","Integer(Boolean)","Integer(Boolean)","String","String","Real","Integer","Integer","String","Real","String","String","String","DateTime","DateTime","String","String","Integer(Boolean)","Integer","String","Integer(Boolean)"

survey >

survey.csvt >

survey_event > 
objectid,survey_event_key,survey_key,location_key,event_name,event_reference,event_date,derived_from,coordinate_system,latitude,longitude,latitude_wgs84,longitude_wgs84,spatial_type,start_latitude,start_longitude,start_orig,start_latitude_wgs84,start_longitude_wgs84,end_latitude,end_longitude,end_orig,end_latitude_wgs84,end_longitude_wgs84,surveyors,depth_lower_cd,depth_upper_cd,depth_lower_sl,depth_upper_sl,tidal_currents,temp_surface,temp_bottom,salinity,under_water_visibility,vague_date_start,vague_date_end,vague_date_type,file_code

survey_event.csvt >
"Integer","String","String","String","String","String","DateTime","String","String","Real","Real","Real","Real","String","Real","Real","String","Real","Real","Real","Real","String","Real","Real","String","Real","Real","Real","Real","Real","Real","Real","Real","Real","DateTime","DateTime","String","String"
```

Once you have a `.csv` with headers and its associated `.csvt` you can use `ogr2ogr` to import into the database using the following;

```bash
ogr2ogr -f PostgreSQL PG:"host=$DB_HOSTNAME dbname=$DB_DATABASE user=$DB_USERNAME ACTIVE_SCHEMA=$DB_SCHEMA" $CSV_FILE -nln $CSV_TABLE --config PG_USE_COPY YES

ogr2ogr -f PostgreSQL PG:"host=$DB_HOSTNAME dbname=$DB_DATABASE user=$DB_USERNAME ACTIVE_SCHEMA=static" $HABITAT_MAP_FILE -nln static.$HABITAT_MAP_TABLE -lco GEOMETRY_NAME=the_geom -nlt MULTIPOLYGON --config PG_USE_COPY YES

$DB_HOSTNAME            =>  The hostname or IP of the database server
$DB_DATABASE            =>  The name of the database you are importing into i.e. bh3
$DB_USERNAME            =>  The username of your database user
$DB_SCHEMA              =>  The target schema to import into
$CSV_FILE               =>  The filename / path of the CSV file
$CSV_TABLE              =>  The name of the table
```

### Finalise schema

Once you have the data uploaded and you are ready to use the new snapshot, simply rename the old `marinerec` snaphost to an appropriate name (i.e. `marinerec_old_YYYYMMDD`) and then rename `marinerec_update` to `marinerec` to complete the update (`marinerec` is hard coded into the tool at present).

## Habitat Maps

Habitat maps are imported as a single table they should be formatted to match the existing schema;

| Attribute Name | Data Type | Notes |
| - | - | - |
| gid | integer | Primary Key field, rolling integer |
| eunis_l3 | string(32) | Not actually L3 codes, should be limited to L3 but includes L2 |
| hab_type | string(50) | The full habitat code |
| the_geom | geometry(MultiPolygon, 4326) | MultiPolygon in 4326 only, Ensure you strip 3D elements if running through ESRI |

Data should be imported through ogr2ogr into the database i.e.;

```bash
ogr2ogr -f PostgreSQL PG:"host=$DB_HOSTNAME dbname=$DB_DATABASE user=$DB_USERNAME ACTIVE_SCHEMA=static" $HABITAT_MAP_FILE -nln static.$HABITAT_MAP_TABLE -lco GEOMETRY_NAME=the_geom -nlt MULTIPOLYGON --config PG_USE_COPY YES

$DB_HOSTNAME            =>  The hostname or IP of the database server
$DB_DATABASE            =>  The name of the database you are importing into i.e. bh3
$DB_USERNAME            =>  The username of your database user
$HABITAT_MAP_FILE       =>  The filename / path of the habitat map (prefer GeoPackage)
$HABITAT_MAP_TABLE      =>  The name of the table (tends to be $HABITAT_MAP_FILE minus extension)
```

You should always create a new table for each habitat map, do not ammend or add to existing tables, if a habitat map is no longer required drop that table do not modify.

This snippet assumes you have setup the connection already via a `.pgpass` file in your home directory.

To use the new habitat map you will need to update the procedure call to include it i.e. update `habitat_schema` and `habitat_table` variables.

## Sensitivity Sources (Scores)

Sensitivity sources / scores are contained within the `lut` schema, each type is stored in its own table, current sources are and are defined in the `sensitivity_source` type;

 - broadscale_habitats
 - eco_groups
 - rock
 - eco_groups_rock
 - rock_eco_groups

### Broadscale habitats

Broadscale habitat updates are always used in the tool, they are stored as a single table in the following schema;

| Name | Type | Notes | 
| - | - | - |
| gid | integer | Primary Key field, rolling integer |
| eunis_l3_code | string | Not actually L3 codes, should be limited to L3 but includes L2 |
| sensitivity_ab_su | string | Not used |
| sensitivity_ab_ss | string | Not used | 
| sensitivity_ab_su_num_max | integer | 
| sensitivity_ab_ss_num_max | integer | 
| confidence_ab_su | string | Not used | 
| confidence_ab_ss | string | Not used |
| confidence_ab_su_num | integer | | 
| confidence_ab_ss_num | integer | |

Data can be imported via ogr2ogr as a CSV file.

```bash
ogr2ogr -f PostgreSQL PG:"host=$DB_HOSTNAME dbname=$DB_DATABASE user=$DB_USERNAME ACTIVE_SCHEMA=lut" $CSV_DATA_FILE -nln $CSV_DATA_TABLE --config PG_USE_COPY YES

$DB_HOSTNAME            =>  The hostname or IP of the database server
$DB_DATABASE            =>  The name of the database you are importing into i.e. bh3
$DB_USERNAME            =>  The username of your database user
$CSV_DATA_FILE          =>  The filename / path of the CSV file
$CSV_DATA_TABLE         =>  The name of the table (tends to be $CSV_DATA_FILE minus extension)
```

You should always create a new table for each update, do not ammend or add to existing tables, if a sensitivity source / score table is no longer required drop that table do not modify.

To run with the updated score for broadscale habitats you will need to update the function run with the name (and schema if it is different) i.e. update `habitat_sensitivity_lookup_schema` and `habitat_sensitivity_lookup_table` variables.

