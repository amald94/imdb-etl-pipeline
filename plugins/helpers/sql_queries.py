class SqlQueries:
    
    ### Movie 
    movie_table_insert = ("""
    INSERT INTO public.movies_list (
       title, 
       rating, 
       year, 
       duration, 
       director, 
       genres)
    SELECT  title, rating, 
       year, duration, 
       director,genres
    FROM staging_movies;
    """)

    top_ten_movies = ("""
       INSERT INTO public.top_ten_movies (
       year, 
       title, 
       rating)
        SELECT year, title, rating
        FROM (
                SELECT year, title, rating,
                row_number() over(partition by year order by rating desc) as rn
                FROM public.movies_list
                WHERE year IS NOT NULL
                )
        WHERE rn <= 10
        ORDER BY year DESC;
    """)

    top_movies_decade = ("""
       INSERT INTO public.top_movies_decade (
        title, 
        rating, 
        year, 
        duration, 
        director,
        genres)
    SELECT  title, 
        rating, 
        year, 
        duration, 
        director,
        genres
    FROM public.movies_list
        WHERE year >= 2010
        ORDER BY rating desc;

    """)


###### Director
    director_table_insert = ("""
    INSERT INTO public.director_table (
       director_name, 
       gross, 
       genres, 
       movie_title, 
       content_rating, 
       budget,
       rating)
    SELECT  director_name, 
       gross, genres, 
       movie_title,content_rating, 
       budget,rating
    FROM public.staging_director;
    """)

    top_gross_films_by_category = ("""
    INSERT INTO public.top_gross_films (
       director_name,
       profit, 
       content_rating)
    SELECT director_name, sum(gross-budget) as profit, content_rating
        FROM public.staging_director
        WHERE gross IS NOT NULL
        GROUP BY director_name, content_rating
        ORDER BY profit DESC;
    """)