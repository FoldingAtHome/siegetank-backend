from sqlalchemy.orm import relationship, backref
from sqlalchemy import Column, Integer, String, ForeignKey
from sqlalchemy.types import LargeBinary
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine
Base = declarative_base()

class Project(Base):
    __tablename__ = 'projects'
    # required
    uuid = Column(String, nullable=False, primary_key=True)
    owner = Column(String, nullable=False)
    description = Column(String, nullable=False)
    system = Column(LargeBinary, nullable=False)
    integrator = Column(LargeBinary, nullable=False)
    # optional
    steps_per_frame = Column(Integer, nullable=False)
    frame_format = Column(String, nullable=False)
    precision = Column(Integer, nullable=False)

    def __init__(self, uuid, owner, description, system, integrator, 
                steps_per_frame=50000,
                frame_format='xtc', 
                precision=3):

        self.uuid = uuid
        self.owner = owner
        self.description = description
        self.system = system
        self.integrator = integrator
        self.steps_per_frame = steps_per_frame
        self.frame_format = frame_format
        self.precision = precision

class Stream(Base):
    __tablename__ = 'streams'
    uuid = Column(String, nullable=False, primary_key=True)
    state = Column(LargeBinary, nullable=False)

    frames = Column(Integer)
    project_uuid = Column(String, ForeignKey('projects.uuid'), nullable=False)
    project = relationship("Project",backref=backref('streams', order_by=uuid))
    
    def __init__(self, uuid, state):
        self.uuid = uuid
        self.state = state
        self.frames = 0

def initialize():
    engine = create_engine('postgresql://localhost/sandbox2', echo=True)
    Base.metadata.create_all(engine)
    return engine