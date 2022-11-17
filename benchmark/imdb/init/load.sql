CREATE TABLE aka_name (
                          id integer NOT NULL,
                          person_id integer NOT NULL,
                          name character varying(218) NOT NULL,
                          imdb_index character varying(12),
                          name_pcode_cf character varying(5),
                          name_pcode_nf character varying(5),
                          surname_pcode character varying(5),
                          md5sum character varying(32)
);

CREATE TABLE aka_title (
                           id integer NOT NULL,
                           movie_id integer NOT NULL,
                           title character varying(553) NOT NULL,
                           imdb_index character varying(12),
                           kind_id integer NOT NULL,
                           production_year integer,
                           phonetic_code character varying(5),
                           episode_of_id integer,
                           season_nr integer,
                           episode_nr integer,
                           note character varying(72),
                           md5sum character varying(32)
);

CREATE TABLE cast_info (
                           id integer NOT NULL,
                           person_id integer NOT NULL,
                           movie_id integer NOT NULL,
                           person_role_id integer,
                           note character varying(992),
                           nr_order integer,
                           role_id integer NOT NULL
);

CREATE TABLE char_name (
                           id integer NOT NULL,
                           name character varying(478) NOT NULL,
                           imdb_index character varying(12),
                           imdb_id integer,
                           name_pcode_nf character varying(5),
                           surname_pcode character varying(5),
                           md5sum character varying(32)
);

CREATE TABLE comp_cast_type (
                                id integer NOT NULL,
                                kind character varying(32) NOT NULL
);

CREATE TABLE company_name (
                              id integer NOT NULL,
                              name character varying(200) NOT NULL,
                              country_code character varying(255),
                              imdb_id integer,
                              name_pcode_nf character varying(5),
                              name_pcode_sf character varying(5),
                              md5sum character varying(32)
);

CREATE TABLE company_type (
                              id integer NOT NULL,
                              kind character varying(32) NOT NULL
);

CREATE TABLE complete_cast (
                               id integer NOT NULL,
                               movie_id integer,
                               subject_id integer NOT NULL,
                               status_id integer NOT NULL
);

CREATE TABLE info_type (
                           id integer NOT NULL,
                           info character varying(32) NOT NULL
);

CREATE TABLE keyword (
                         id integer NOT NULL,
                         keyword character varying(74) NOT NULL,
                         phonetic_code character varying(5)
);

CREATE TABLE kind_type (
                           id integer NOT NULL,
                           kind character varying(15) NOT NULL
);

CREATE TABLE link_type (
                           id integer NOT NULL,
                           link character varying(32) NOT NULL
);

CREATE TABLE movie_companies (
                                 id integer NOT NULL,
                                 movie_id integer NOT NULL,
                                 company_id integer NOT NULL,
                                 company_type_id integer NOT NULL,
                                 note character varying(208)
);

CREATE TABLE movie_info (
                            id integer NOT NULL,
                            movie_id integer NOT NULL,
                            info_type_id integer NOT NULL,
                            info character varying(8000) NOT NULL,
                            note character varying(387)
);

CREATE TABLE movie_info_idx (
                                id integer NOT NULL,
                                movie_id integer NOT NULL,
                                info_type_id integer NOT NULL,
                                info character varying(10) NOT NULL,
                                note character varying(1)
);

CREATE TABLE movie_keyword (
                               id integer NOT NULL,
                               movie_id integer NOT NULL,
                               keyword_id integer NOT NULL
);

CREATE TABLE movie_link (
                            id integer NOT NULL,
                            movie_id integer NOT NULL,
                            linked_movie_id integer NOT NULL,
                            link_type_id integer NOT NULL
);

CREATE TABLE name (
                      id integer NOT NULL,
                      name character varying(106) NOT NULL,
                      imdb_index character varying(12),
                      imdb_id integer,
                      gender character varying(1),
                      name_pcode_cf character varying(5),
                      name_pcode_nf character varying(5),
                      surname_pcode character varying(5),
                      md5sum character varying(32)
);

CREATE TABLE person_info (
                             id integer NOT NULL,
                             person_id integer NOT NULL,
                             info_type_id integer NOT NULL,
                             info text NOT NULL,
                             note character varying(430)
);

CREATE TABLE role_type (
                           id integer NOT NULL,
                           role character varying(32) NOT NULL
);

CREATE TABLE title (
   id integer NOT NULL,
   title character varying(334) NOT NULL,
   imdb_index character varying(12),
   kind_id integer NOT NULL,
   production_year integer,
   imdb_id integer,
   phonetic_code character varying(5),
   episode_of_id integer,
   season_nr integer,
   episode_nr integer,
   series_years character varying(49),
   md5sum character varying(32)
);

INSERT INTO aka_name SELECT * FROM read_csv_auto('/Users/tomebergen/Documents/Datasets/imdb_clean/aka_name.csv');
INSERT INTO aka_title SELECT * FROM read_csv_auto('/Users/tomebergen/Documents/Datasets/imdb_clean/aka_title.csv');
INSERT INTO cast_info SELECT * FROM read_csv_auto('/Users/tomebergen/Documents/Datasets/imdb_clean/cast_info.csv');
INSERT INTO char_name SELECT * FROM read_csv_auto('/Users/tomebergen/Documents/Datasets/imdb_clean/char_name.csv');
INSERT INTO comp_cast_type SELECT * FROM read_csv_auto('/Users/tomebergen/Documents/Datasets/imdb_clean/comp_cast_type.csv');
INSERT INTO company_name SELECT * FROM read_csv_auto('/Users/tomebergen/Documents/Datasets/imdb_clean/company_name.csv');
INSERT INTO company_type SELECT * FROM read_csv_auto('/Users/tomebergen/Documents/Datasets/imdb_clean/company_type.csv');
INSERT INTO complete_cast SELECT * FROM read_csv_auto('/Users/tomebergen/Documents/Datasets/imdb_clean/complete_cast.csv');
INSERT INTO info_type SELECT * FROM read_csv_auto('/Users/tomebergen/Documents/Datasets/imdb_clean/info_type.csv');
INSERT INTO keyword SELECT * FROM read_csv_auto('/Users/tomebergen/Documents/Datasets/imdb_clean/keyword.csv');
INSERT INTO kind_type SELECT * FROM read_csv_auto('/Users/tomebergen/Documents/Datasets/imdb_clean/kind_type.csv');
INSERT INTO link_type SELECT * FROM read_csv_auto('/Users/tomebergen/Documents/Datasets/imdb_clean/link_type.csv');
INSERT INTO movie_companies SELECT * FROM read_csv_auto('/Users/tomebergen/Documents/Datasets/imdb_clean/movie_companies.csv');
INSERT INTO movie_info SELECT * FROM read_csv_auto('/Users/tomebergen/Documents/Datasets/imdb_clean/movie_info.csv');
INSERT INTO movie_info_idx SELECT * FROM read_csv_auto('/Users/tomebergen/Documents/Datasets/imdb_clean/movie_info_idx.csv');
INSERT INTO movie_keyword SELECT * FROM read_csv_auto('/Users/tomebergen/Documents/Datasets/imdb_clean/movie_keyword.csv');
INSERT INTO movie_link SELECT * FROM read_csv_auto('/Users/tomebergen/Documents/Datasets/imdb_clean/movie_link.csv');
INSERT INTO name SELECT * FROM read_csv_auto('/Users/tomebergen/Documents/Datasets/imdb_clean/name.csv');
INSERT INTO person_info SELECT * FROM read_csv_auto('/Users/tomebergen/Documents/Datasets/imdb_clean/person_info.csv');
INSERT INTO role_type SELECT * FROM read_csv_auto('/Users/tomebergen/Documents/Datasets/imdb_clean/role_type.csv');
INSERT INTO title SELECT * FROM read_csv_auto('/Users/tomebergen/Documents/Datasets/imdb_clean/title.csv');
