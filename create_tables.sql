CREATE TABLE public.staging_movies (
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
	PRIMARY KEY (title)
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
	title varchar,
	rating float,
	year int,
    duration int,
    director varchar,
	genres varchar,
	PRIMARY KEY (title)
);


CREATE TABLE public.top_ten_movies (
	year int,
	title varchar,
	rating float,
	PRIMARY KEY (title)
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

