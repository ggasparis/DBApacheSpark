map(key, value):
    key1 <- Set the ID from movies
    value1 <- Set pairs of the movie summary and date
    IF date.movies >=2000
        emit(key1, value1)

map(key, value):
    key2 <- Set the ID from movies
    value2 <- Set the movie genres
    D <- 'Drama' from genres
    IF genre == D
        emit(key2, value2)

map(key, value):
    key3 <- Set the movie date
    value3 <- Set the length of the movie summary
    IF summary != " "
        emit(key3, value3)

reduce(key, value):
    key4 <- Set the year of the movie
    value4 <- Set the average length of the summaries
    for each pair
        count length
    emit(key4, value4)

map(key, value):
    key5 <- Set the year of the movie
    value5 <- Set the average length of the summaries
    if dates == valid
        emit(key5, value5)
