from sqlalchemy.orm import relationship, backref, sessionmaker
from sqlalchemy import Column, Integer, String, DateTime, ForeignKey, create_engine
from sqlalchemy.types import LargeBinary
from sqlalchemy.ext.declarative import declarative_base
import uuid
import datetime

Base = declarative_base()

class Project(Base):
    __tablename__ = 'projects'
    # required
    uuid = Column(String, nullable=False, primary_key=True)
    description = Column(String, nullable=False)
    system = Column(LargeBinary, nullable=False)
    integrator = Column(LargeBinary, nullable=False)
    # optional
    steps_per_frame = Column(Integer, nullable=False)
    frame_format = Column(String, nullable=False)
    precision = Column(Integer, nullable=False)

    date = Column(DateTime, nullable = False)

    # relationships
    owner = Column(String, ForeignKey('users.username'), nullable=False)
    user = relationship("User", backref=backref('projects', order_by=uuid))

    def __init__(self,
                 uuid,
                 description, 
                 system, 
                 integrator, 
                 steps_per_frame,
                 frame_format, 
                 precision):

        self.uuid = uuid
        self.description = description
        self.system = system
        self.integrator = integrator
        self.steps_per_frame = steps_per_frame
        self.frame_format = frame_format
        self.precision = precision
        self.date = datetime.datetime.now()

class Stream(Base):
    __tablename__ = 'streams'
    uuid = Column(String, nullable=False, primary_key=True)
    state = Column(LargeBinary, nullable=False)
    frames = Column(Integer)

    # todo add IP of work server?

    # relationships
    project_uuid = Column(String, ForeignKey('projects.uuid'), nullable=False)
    project = relationship("Project",backref=backref('streams', order_by=uuid))
    
    def __init__(self, uuid, state):
        self.uuid = uuid
        self.state = state
        self.frames = 0

class User(Base):
    __tablename__ = 'users'
    username = Column(String, nullable=False, primary_key = True)
    password = Column(String, nullable=False)
    email = Column(String, nullable=False)
    token = Column(String, nullable=False)

    def __init__(self, username, password, email):
        self.username = username
        self.password = password
        self.email = email
        self.token = str(uuid.uuid4())