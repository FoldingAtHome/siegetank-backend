import uuid

from server.SQLTypes import User
from server.SQLTypes import initialize

Session = initialize()

print Session()

s1= Session()

user1 = User("diwakar", "pass2", "dshukla@gmail.com")

s1.add(user1)
s1.commit()