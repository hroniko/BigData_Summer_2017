val list = List(1,2,3,4,5,6,7,8,9,10)
val res =  list.filter(x => x % 3 == 0)
  .filter(x => x > 5)

val summa = res.reduce((elemOne, elemTwo) => elemOne + elemTwo)
val pro = res.reduce((elemOne, elemTwo) => elemOne * elemTwo)

val summMap = res.map(x => x+1)
  .reduce((elemOne, elemTwo) => elemOne + elemTwo)

val hostPort = ("localhost")