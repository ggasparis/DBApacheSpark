map(key, value):
    key1 = Date
    value[i] <- Put in  value Cost, Title and Revenue where i=1,2,3
    IF (value[i]!= 0)
        emit(key, value[i])

reduce(key, value):
    key2  = Date
    Revenus as R, Cost as  C, Title as T
    values[i] <- Put in R, C and T
    fun <-  {(R-C)/C}*100
    pair <- (T, fun)
    for each Date >= 2000
        for each i in values
            max_pair <- find the biggest movie profit
    emit(key2, max_pair)
