CREATE_IMMIGRATION_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS public.immigration (
    cicid int4,
  	i94res int4,
  	i94cit int4,
  	arrdate varchar,
	i94addr varchar,
  	i94yr int4,
    i94mon int4,
  	i94port varchar,
  	i94mode int4,
  	i94visa int4,
  	visatype varchar,
  	depdate varchar,
    dtaddto varchar,
    entdepd varchar,
  	i94bir int4,
    biryear int4,
  	gender varchar,
  	airline varchar,
  	stay int4,
	CONSTRAINT immigration_pkey PRIMARY KEY ("cicid")
);
"""

CREATE_COUNTRY_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS public.country (
	Code int4,
	CountryName varchar,
	Temperature float,
	CONSTRAINT country_pkey PRIMARY KEY ("Code")
);
"""

CREATE_STATE_TABLE_SQL = """
CREATE TABLE public.state (
	State varchar,
    StateCode varchar,
	TotalPopulation int8,
	MalePopulation int8,
	FemalePopulation int8,
	ForeignBorn int8,
	NumberOfVeterans int8,
	AmericanIndianAndAlaskaNative int8,
	Asian int8,
	BlackOrAfricanAmerican int8,
	HispanicOrLatino int8,
	White int8,
	CONSTRAINT state_pkey PRIMARY KEY ("StateCode")
);
"""

CREATE_DATE_TABLE_SQL = """
CREATE TABLE public."date" (
	"date" varchar NOT NULL,
	"year" int4,
	"month" int4,
	"day" int4,
	weekofyear int4,
	dayofweek int4,
	CONSTRAINT date_pkey PRIMARY KEY ("date")
);
"""
