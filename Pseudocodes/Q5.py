map(key, value):
    key1 <- Set the movie id
    value1 <- Set the pairs of ratings and user ID
    emit(key1, value1)

map(key, value):
    key2 <- Set the pair of the genre and user id
    value2 <- Set as value1 join with genre with counter 
    emit(key2, value2)

reduce(key, value):
    key3 <- Set the pair of the genre and use id
    value3 <- Calculate the sum of the users
    emit(key3, value3)

map(key, value):
    key4 <- Set the genre
    value4 <- Set the pair of user and the sum  of ratings
    emit(key4, value4)

reduce(key, value):
    key5 <- Set the genre
    value5 <- Set the max value
    for each pair (userID, Ratings)
        find max <- max(pair)
    emit(key5, value5)

map(key, value):
    key6 <- Set the pair of genre and sum of ratings from  pair of genre and userID
    value6 <- Set the user id from the sum of ratings
    emit(key6, value6)

map(key, value):
    key7 <- Set the pair of genre and sum of ratings from genre
    value7 <- Set the user id from pair of user and ratings
    emit(key7, value7)

map(key, value):
    key8 <- Set the genre
    value8 <- set the pair of sum of ratings and userID
    emit(key8, value8)

map(key, value):
    key9 <- Set the movie id
    value9 <- Set the genre
    emit(key9, value9)

map(key, value):
    key10 <- Set the movie id after join
    value10 <- Set the genre with movie id and user id
    emit(key10, value10)

map(key, value):
    key11 <- Set the pair of userID and movieID after join movies
    value11 <- Set  the values of value10 with movie title and times that appears
    emit(key11, value11)

map(key, value):
    key12 <- Set the genre after join ratings
    value12 <- Set the value11 with the ratings
    emit(key12, value12)

map(key, value):
    key13 <- Set the key12 with the pair of ratings and sum  of ratings
    value13 <- Set the userID with the pair of the title and the tmes that appears
    emit(key13, value13)

map(key, value):
    key14 <- Set the pair of genre and the sum of ratings
    value14 <- Set the value13
    emit(key14, value14)

reduce(key, value):
    key15 <- Set the key14
    value15 <- Set the pair of value14 with the maximum rating as MaxR
    for each pair
        find the MaxR <- max(pair through rating)
    emit(key15, value15)

map(key, value):
    key16 <- Set the key15 with the MaxR
    value16 <- Set the MaxR
    emit(key16, value16)

map(key, value):
    key17 <- set the key17 after join
    value17 <- Set the pair of value16 and value13
    emit(key17, value17)

reduce(key, value):
    key18 <- Set the pair of genre and userID
    value18 <- Set the values of the maximum rating and times that appears
    for each pair
        find max(most frequently appears)
    emit(key18, value18)
