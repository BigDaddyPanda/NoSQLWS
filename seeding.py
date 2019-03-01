'''
{
    "book_id",
    "book_name",
    "book_author",
    "book_tags",
    "rental_date"

}
{
    'id',
    'full_name',
    'email',
}
'''
from uuid import uuid4
import datetime
from pprint import pprint
from models import Books,Tenancy
from random import choice,randint as ri
books=["The Silent Wolf","The Storm","Les Miserables","Belle et la Bete","Superman"]
authors=["Michel Austin","Shakespeare","Victor Hugo"]
tags=["Romance","Action","Thriller","Mystery"]
names=["Samir","Mounir","Mariem","Mouna"]
f_names=["ElAbdi","Rouni","Abboud","Lahnin"]
def _date():
    return datetime.datetime(ri(2017,2018),ri(1,12),ri(1,28))
try:    
    Books.delete_many()
    Tenancy.delete_many()
except Exception as e:
    print("No Previous Collections")

for i in books:
    Books.insert(
        {
            "isbn":str(uuid4()),
            "book_name":i,
            "book_author":choice(authors),
            "book_tags":choice(tags)
        }
    )

t=list(Books.find({},{"_id":0,"isbn":1}))
print(len(t))
for i in range(10):
    Tenancy.insert({
        'isbn':choice(t)["isbn"],
        'full_name':choice(names)+" "+choice(f_names),
        "date":_date(),
    })

pprint(len(list(Tenancy.find({}))))
# pprint(list(Tenancy.find({})))