CREATE TABLE public.staging_movies (
	movieid int IDENTITY(1, 1),
	title varchar,
	rating float,
	year int,
    duration int,
    director varchar,
    gross int,
	genres varchar,
    votes int,
    content varchar,
    budget int,
	PRIMARY KEY (movieid)
);


CREATE TABLE public.staging_director (
	directorid int IDENTITY(1, 1),
	director_name varchar,
    gross int,
  	genres varchar,
    movie_title varchar,
    content_rating varchar,
    budget int,
	rating float,
	PRIMARY KEY (directorid)
);


CREATE TABLE public.movies_decade (
	movieid int IDENTITY(1, 1),
	title varchar,
	rating float,
	year int,
    duration int,
    director varchar,
	genres varchar,
	PRIMARY KEY (movieid)
);



CREATE TABLE public.movies_decade (
	movieid int IDENTITY(1, 1),
	title varchar,
	rating float,
	year varchar,
    duration int,
    director varchar,
	genres varchar,
	PRIMARY KEY (movieid)
);


CREATE TABLE public.top_ten_movies (
	movieid int IDENTITY(1, 1),
	year int,
	title varchar,
	rating float,
	PRIMARY KEY (movieid)
)

CREATE TABLE public.movies_list (
	title varchar,
	rating float,
	"year" int,
    duration int,
    director varchar,
    gross int,
	genres varchar,
    votes int,
    content varchar,
    budget int,
	PRIMARY KEY (title)
);



CREATE TABLE public.director_table (
	directorid int IDENTITY(1, 1),
	director_name varchar,
    gross int,
  	genres varchar,
    movie_title varchar,
    content_rating varchar,
    budget int,
	rating float,
	PRIMARY KEY (directorid)
);

