import uuid

from server.types import Project
from server.types import Stream
from server.types import User
from server.types import initialize

from sqlalchemy.orm import relationship, backref
from sqlalchemy.ext.declarative import declarative_base

engine = initialize()

from sqlalchemy.orm import sessionmaker
Session = sessionmaker(bind=engine)
session = Session()

with open('system.xml', 'rb') as f:
	system = f.read()

with open('integrator.xml', 'rb') as f:
	integrator = f.read()

with open('state.xml', 'rb') as f:
	state = f.read()

proj1 = Project(str(uuid.uuid4()), "kinase", system, integrator)
proj1.streams = [
				Stream(str(uuid.uuid4()), state)
				]	

user1 = User("proteneer", "Hw0K3psmzp5", "proteneer@gmail.com")
user1.projects = [proj1]

session.add(user1)
session.commit()