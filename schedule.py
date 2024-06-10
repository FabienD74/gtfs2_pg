
from __future__ import (division, absolute_import, print_function,
                        unicode_literals)
from datetime import date

import sqlalchemy
import sqlalchemy.orm
from sqlalchemy import and_
from sqlalchemy.sql.expression import select, join

from .gtfs_entities import *
from .feed import *
import sys

import six





import logging
_LOGGER = logging.getLogger(__name__)



##########################################################################
##########################################################################
##########################################################################
class Schedule:
    """Represents the full database.

    The schedule is the most important object in pygtfs. It represents the
    entire dataset. Most of the properties come straight from the gtfs
    reference. Two of them were renamed: calendar is called `services`, and
    calendar_dates `service_exceptions`. One addition is the `feeds` table,
    which is here to support more than one feed in a database.

    Each of the properties is a list created upon access by sqlalchemy. Then,
    each element of the list as attributes following the gtfs reference. In
    addition, if they are related to another table, this can also be accessed
    by attribute.

    :param db_conection: Either a sqlalchemy database url or a filename to be used with sqlite.

    """
    ####################################
    def __init__(self, db_connection):
        _LOGGER.debug(f"Schedule.__init__ BEGIN" ) 

        self.init_entities()     

        self.db_connection = db_connection
        self.db_filename = None
        if '://' not in db_connection:
            self.db_connection = 'sqlite:///%s' % self.db_connection
        if self.db_connection.startswith('sqlite'):
            self.db_filename = self.db_connection
        self.engine = sqlalchemy.create_engine(self.db_connection)
        Session = sqlalchemy.orm.sessionmaker(bind=self.engine)
        self.session = Session()
        Base.metadata.create_all(self.engine)
        _LOGGER.debug(f"Schedule.__init__ END" ) 

    ####################################
    def init_entities(self):
        _LOGGER.debug(f"Schedule.init_entities BEGIN" ) 

        for entity in (gtfs_all + [Feed]):
            _LOGGER.debug(f"Schedule.init_entities entity ={entity}" ) 

            entity_doc = "A list of :py:class:`pygtfs.gtfs_entities.{0}` objects".format(entity.__name__)
            entity_raw_doc = ("A :py:class:`sqlalchemy.orm.Query` object to fetch "
                            ":py:class:`pygtfs.gtfs_entities.{0}` objects"
                            .format(entity.__name__))
            entity_by_id_doc = "A list of :py:class:`pygtfs.gtfs_entities.{0}` objects with matching id".format(entity.__name__)
            setattr(Schedule, entity._plural_name_, _meta_query_all(entity, entity_doc))
            setattr(Schedule, entity._plural_name_ + "_query",
                    _meta_query_raw(entity, entity_raw_doc))
            if hasattr(entity, 'id'):
                setattr(Schedule, entity._plural_name_ + "_by_id", _meta_query_by_id(entity, entity_by_id_doc))    
        _LOGGER.debug(f"Schedule.init_entities END " ) 

    ####################################
    def drop_feed(self, feed_id):
        _LOGGER.debug(f"Schedule.drop_feed BEGIN feed_id={feed_id}" ) 
        """ Delete a feed from a database by feed id"""
        # the following does not cascade unfortunatly.
        # self.session.query(Feed).filter(Feed.feed_id == feed_id).delete()
        feed = self.session.query(Feed).get(feed_id)
        _LOGGER.debug(f"Schedule.drop_feed debug 1 feed= {feed}" ) 
        self.session.delete(feed)
        self.session.commit()
        _LOGGER.debug(f"Schedule.drop_feed END" ) 

    ####################################
    def list_feeds(self):
        _LOGGER.debug(f"Schedule.list_feeds BEGIN" ) 
        for (i, a_feed) in enumerate(self.feeds):
#            print("{0}. id {1.feed_id}, name {1.feed_name}, "
#                "loaded on {1.feed_append_date}".format(i, a_feed))
            _LOGGER.debug(f"Schedule.list_feeds feed= {a_feed}" ) 
        _LOGGER.debug(f"Schedule.list_feeds END" ) 

    def delete_feed_id(self, feed_id ):
        _LOGGER.debug(f"Schedule.delete_feed_id BEGIN" ) 
        self.drop_feed(feed_id)
        _LOGGER.debug(f"Schedule.delete_feed_id END" ) 

    ####################################
    def delete_feed(self, feed_filename, interactive=False):
        _LOGGER.debug(f"Schedule.delete_feed BEGIN" ) 



        feed_name = feed.derive_feed_name(feed_filename)
        feeds_with_name = self.session.query(Feed).filter(Feed.feed_name == feed_name).all()
        delete_all = not interactive
        for matching_feed in feeds_with_name:
            if not delete_all:
                print("Found feed ({0.feed_id}) named {0.feed_name} loaded on {0.feed_append_date}".format(matching_feed))
                ans = ""
                while ans not in ("K", "O", "A"):
                    ans = six.moves.input("(K)eep / (O)verwrite / overwrite (A)ll ? ").upper()
                if ans == "K":
                    continue
                if ans == "A":
                    delete_all = True
            # you get here if ans is A or O, and if delete_all
            self.drop_feed(matching_feed.feed_id)
        _LOGGER.debug(f"Schedule.delete_feed END" ) 

    ####################################
    def overwrite_feed(self, feed_filename, *args, **kwargs):
        interactive = kwargs.pop('interactive', False)
        self.delete_feed( feed_filename, interactive=interactive)
        self.append_feed( feed_filename, *args, **kwargs)

    ####################################
    def append_feed(self, feed_filename, strip_fields=True,
                    chunk_size=5000, agency_id_override=None, partial_commit=True):

        fd = Feed_Reader(feed_filename, strip_fields)

        # create new feed
        feed_entry = Feed(feed_name=fd.feed_name, feed_append_date=date.today())
        self.session.add(feed_entry)
        self.session.flush()
        if partial_commit:
            _LOGGER.debug(f"debug: partial commit 1" ) 
            self.session.commit()

        feed_id = feed_entry.feed_id

        for gtfs_class in gtfs_all:
            gtfs_filename = gtfs_class.__tablename__ + '.txt'
            gtfs_table = []
            try:
                # We ignore the feed supplied feed id, because we create our own
                # later.
                gtfs_table = fd.read_table(
                    gtfs_filename,
                    set(c.name for c in gtfs_class.__table__.columns) - {'feed_id'})

            except (KeyError, IOError):
                if gtfs_class in gtfs_required:
                    _LOGGER.debug(f"Error: could not find {gtfs_filename}" ) 
                    raise IOError('Error: could not find %s' % gtfs_filename)
                else:
                    # make sure table is empty....it should be but..
                    gtfs_table = []

            for i, record in enumerate(gtfs_table):
                if not record:
                    # Empty row.
                    continue

                try:
                    instance = gtfs_class(feed_id=feed_id, **record._asdict())
                except:
                    _LOGGER.debug(f"Failure while writing {record}" ) 
                    print("Failure while writing {0}".format(record))
                    raise
                self.session.add(instance)

                if isinstance(instance, ServiceException):
                    service = self.session.execute(
                        select(Service).where(Service.service_id == instance.service_id).where(Service.feed_id == feed_id)
                    ).one_or_none()
                    if service is None:
                        # ServiceException was added for a day that has no associated regular service
                        # Create a dummy service object for this service exception
                        dummy = Service(
                            feed_id=feed_id, 
                            service_id=instance.service_id,
                            monday='0',
                            tuesday='0',
                            wednesday='0',
                            thursday='0',
                            friday='0',
                            saturday='0',
                            sunday='0',
                            start_date=instance.date.strftime('%Y%m%d'),
                            end_date=instance.date.strftime('%Y%m%d'))
                        self.session.add(dummy)
                    

                if i % chunk_size == 0 and i > 0:
                    self.session.flush()
                    if partial_commit:
                        _LOGGER.debug(f"debug: partial commit table:  record {i}" ) 
                        self.session.commit()

            self.session.flush()
            if partial_commit:
                _LOGGER.debug(f"debug: partial commit end of table ..." ) 
                self.session.commit()
            gtfs_table = []

        _LOGGER.debug(f"END: final commit  ..." ) 

        self.session.flush()
        self.session.commit()
        
        # load many to many relationships
        if Translation in gtfs_tables:
            print('Mapping translations to stops')
            q = (self.session.query(
                    Stop.feed_id.label('stop_feed_id'),
                    Translation.feed_id.label('translation_feed_id'),
                    Stop.stop_id.label('stop_id'),
                    Translation.trans_id.label('trans_id'),
                    Translation.lang.label('lang'))
                .filter(Stop.feed_id==feed_id)
                .filter(Translation.feed_id==feed_id)
                .filter(Stop.stop_name==Translation.trans_id)
                )
            upd = _stop_translations.insert().from_select(
                    ['stop_feed_id', 'translation_feed_id', 'stop_id', 'trans_id', 'lang'], q)
            self.session.execute(upd)
        if ShapePoint in gtfs_tables:
            print('Mapping shapes to trips')
            q = (self.session.query(
                    Trip.feed_id.label('trip_feed_id'),
                    ShapePoint.feed_id.label('shape_feed_id'),
                    Trip.trip_id.label('trip_id'),
                    ShapePoint.shape_id.label('shape_id'),
                    ShapePoint.shape_pt_sequence.label('shape_pt_sequence'))
                .filter(Trip.feed_id==feed_id)
                .filter(ShapePoint.feed_id==feed_id)
                .filter(ShapePoint.shape_id==Trip.shape_id)
                )
            upd = _trip_shapes.insert().from_select(
                    ['trip_feed_id', 'shape_feed_id', 'trip_id', 'shape_id', 'shape_pt_sequence'], q)
            self.session.execute(upd)
        self.session.commit()

        print('Complete.')
        return schedule



#############################################################
#############################################################
#############################################################

def _meta_query_all(entity, docstring=None):
    def _query_all(instance_self):
        """ A list generated on access """
        return instance_self.session.query(entity).all()

    if docstring is not None:
        _query_all.__doc__ = docstring
    return property(_query_all)


#############################################################
#############################################################
#############################################################

def _meta_query_by_id(entity, docstring=None):
    def _query_by_id(self, id):
        """ A function that returns a list of entries with matching ids """
        return self.session.query(entity).filter(entity.id == id).all()
    if docstring is not None:
        _query_by_id.__doc__ = docstring
    return _query_by_id


#############################################################
#############################################################
#############################################################

def _meta_query_raw(entity, docstring=None):
    def _query_raw(instance_self):
        """
            A raw sqlalchemy query object that the user can then manipulate
            manually
        """
        return instance_self.session.query(entity)

    if docstring is not None:
        _query_raw.__doc__ = docstring
    return property(_query_raw)


