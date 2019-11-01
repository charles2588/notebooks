import numpy as np
import pandas as pd
import random
import string
def randomcolumnnamegenerator(numberofcolumns,columnLength):
    listofcolumns = []

    while(len(listofcolumns) < numberofcolumns):
        letters = random.choice(string.ascii_letters)
        randomcolname = ''.join(random.choice(letters) for i in range(columnLength))
        if randomcolname not in listofcolumns:
            listofcolumns.append(randomcolname)

    return listofcolumns

def randomcolumnnamegeneratorwithprefix(prefix,numberofcolumns):
    listofcolumns = []

    for i in range(1,numberofcolumns+1):
        listofcolumns.append(prefix+str(i))

    return listofcolumns

def randomdataframegenerator(listofcolumns,numberofrows):
    df = pd.DataFrame(np.random.randint(0, 100, size=(numberofrows, len(listofcolumns))), columns=listofcolumns)
    return df

listofcolumns = randomcolumnnamegeneratorwithprefix(prefix="col",numberofcolumns=1000)
print(listofcolumns)
randomdf = randomdataframegenerator(listofcolumns=listofcolumns,numberofrows=100)

randomdf.head()

nameofcsv = "1000cols.csv"
randomdf.to_csv(nameofcsv,index=False)