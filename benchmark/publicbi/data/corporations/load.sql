CREATE TABLE "Corporations_1"(
  "Id1" integer NOT NULL,
  "Number of Records" smallint NOT NULL,
  "angelco_account" varchar(95),
  "business_model" varchar(15),
  "city" varchar(99),
  "continent" varchar(26),
  "country" varchar(32),
  "crunchbase_account" varchar(137),
  "facebook_account" varchar(776),
  "financing_stage" varchar(16),
  "founding_date" varchar(10),
  "id" integer NOT NULL,
  "industries" varchar(187),
  "keywords" varchar(255),
  "last_funding_date" varchar(10),
  "linkedin_account" varchar(324),
  "location" varchar(100),
  "long_description" varchar(562),
  "name" varchar(338),
  "num_employees" integer NOT NULL,
  "region" varchar(33),
  "score" integer NOT NULL,
  "short_descriptiion" varchar(259),
  "stage" varchar(18),
  "total_funding" integer NOT NULL,
  "twitter_account" varchar(194),
  "website" varchar(255)
);


COPY Corporations_1 FROM 'benchmark/publicbi/Corporations_1.csv.gz' ( DELIMITER '|', NULL 'null', QUOTE '', ESCAPE '\\n' );