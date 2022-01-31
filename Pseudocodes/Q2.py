map(key,value):
    key1  <- rating of UserID as ID
    value1 <- rating
    emit(ID, (1,value1))

reduce(key, value):
    key2 <- ID
    value2 <- the list of pairs of ratings
    for each pair in value2
        v1 <- Calculate the amount of total ratings
        v2 <- Calculate the sum of the ratings above 3
        emit(key2, v2/v1)
