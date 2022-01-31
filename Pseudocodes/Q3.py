map(key, value):
    key1 <- Set the movie id
    value1 <- pair rating as R
    emit(key1, value1)

reduce(key, value):
    key2 <- Set the movie id
    value <- Set the pair of ratings
    for each value
        value2 <- Calculate the average rating
    emit(key2,value2)

map(key, value0):
    key3 <- Set id from  the mapped ratings
    value3 <- Set  the pair of the average rating and the movie ID
    emit(key3,  value3)
