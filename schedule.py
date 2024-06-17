
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

import multiprocessing

import logging
_LOGGER = logging.getLogger(__name__)

import time

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
#        _LOGGER.debug(f"Schedule.__init__ BEGIN" ) 

        self.init_entities()     

        self.db_connection = db_connection
        self.db_filename = None
        if '://' not in db_connection:
            self.db_connection = 'sqlite:///%s' % self.db_connection
        if self.db_connection.startswith('sqlite'):
            self.db_filename = self.db_connection
        l_engine = sqlalchemy.create_engine(self.db_connection)
        l_Session = sqlalchemy.orm.sessionmaker(bind=l_engine)
#        l_session = l_Session()
        Base.metadata.create_all(l_engine)
#        _LOGGER.debug(f"Schedule.__init__ END" ) 

    ####################################
    def init_entities(self):
#        _LOGGER.debug(f"Schedule.init_entities BEGIN" ) 

        for entity in (gtfs_all + [Feed]):
#            _LOGGER.debug(f"Schedule.init_entities entity ={entity}" ) 

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
#        _LOGGER.debug(f"Schedule.init_entities END " ) 

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
    def overwrite_feed(self, feed_filename, *args, **kwargs):
        interactive = kwargs.pop('interactive', False)
        self.delete_feed( feed_filename, interactive=interactive)
        self.append_feed( feed_filename, *args, **kwargs)


    ####################################
    def process_chunk (self, gtfs_class, feed_id, chunk_records):

        l_engine = sqlalchemy.create_engine(self.db_connection)
        l_Session = sqlalchemy.orm.sessionmaker(bind=l_engine)
        l_session = l_Session()


        for i, record in enumerate(chunk_records):
            try:
                instance = gtfs_class(feed_id=feed_id, **record._asdict())
            except:
        #                _LOGGER.debug(f"Failure while writing {record}" ) 
        #                print("Failure while writing {0}".format(record))
                raise
            l_session.add(instance)

#            _LOGGER.debug(f"isinstance({instance}, {CalendarDates})" ) 

            if isinstance(instance, CalendarDates):
                calendar = l_session.execute(
                    select(Calendar).where(Calendar.service_id == instance.service_id).where(Calendar.feed_id == feed_id)
                ).one_or_none()
                if calendar is None:
                    # CalendarDates was added for a day that has no associated regular service
                    # Create a dummy service object for this service exception

                    _LOGGER.debug(f"ADD DUMMY CALENDAR for {instance.service_id}" ) 

                    dummy = Calendar(
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
                    l_session.add(dummy)

        l_session.flush()
        l_session.commit()
        return


    ####################################
    def import_single_gtfs(self, feed_filename, strip_fields, gtfs_class, feed_id, chunk_size, partial_commit):
        gtfs_filename = gtfs_class.__tablename__ + '.txt'
    #    _LOGGER.debug(f"import_single_gtfs: BEGIN {gtfs_filename}" ) 

        feed_reader = Feed_Reader(feed_filename, strip_fields)


        gtfs_table = []
        try:
            # We ignore the feed supplied feed id, because we create our own
            # later.
            gtfs_table = feed_reader.read_table(
                gtfs_filename,
                set(c.name for c in gtfs_class.__table__.columns) - {'feed_id'})

        except (KeyError, IOError):
            if gtfs_class in gtfs_required:
                pass
#                _LOGGER.debug(f"Error: could not find {gtfs_filename}" ) 
#                raise IOError('Error: could not find %s' % gtfs_filename)
            else:
                # make sure table is empty....it should be but..
                gtfs_table = []


        i = 0
        chunk_records = []

        for record in gtfs_table:
            if not record:
                # Empty row.
                continue

            chunk_records.append (record)
            i = i + 1

            if  (i % chunk_size) == 0 :
                self.process_chunk (gtfs_class,feed_id,chunk_records)    
                chunk_records = []

        # process pending lines in chunk_records
        self.process_chunk (gtfs_class,feed_id,chunk_records)    
        chunk_records = []

        return



    ####################################
    def append_feed(self, feed_filename, strip_fields=True,
                    chunk_size=5000, agency_id_override=None, partial_commit=True):

        feed_reader = Feed_Reader(feed_filename, strip_fields)

        # create new feed

        l_engine = sqlalchemy.create_engine(self.db_connection)
        l_Session = sqlalchemy.orm.sessionmaker(bind=l_engine)
        l_session = l_Session()


        feed_entry = Feed(feed_name=feed_reader.feed_name, feed_append_date=date.today())

        _LOGGER.debug(f"Feed(feed_name={feed_reader.feed_name}, feed_append_date={date.today()})" ) 


        l_session.add(feed_entry)
        l_session.flush()
        l_session.commit()

        feed_id = feed_entry.feed_id

#       Process gtfs_all per "group"
#       Group0 has no dependencies ( like _feed)
#       Group1 only depends of group0 like feed_info, agencies ..
#       Group2 only depends of group1, and groups2 on group1 so on...
#       All class in the same group can be executed in //
#       Group can only be started at the end of previous group.
#
#gtfs_all = [Agency, Stops, Transfers, Routes, FareAttributes, FareRules, Shapes,
#            Calendar, CalendarDates, Trips, Frequencies, StopTimes, FeedInfo,
#            Translations]

#        gtfs_group_0 = [Feed];          #  feed (_feed) is already created

#        gtfs_group_1 = [FeedInfo,Agency,Service,Stop,ShapePoint]
        gtfs_group_1  = [FeedInfo,Agency,Calendar,Stops]

        gtfs_group_2  = [Routes]
        gtfs_group_2b = [CalendarDates]
        gtfs_group_2c = [FareAttributes]

        gtfs_group_3  = [Trips,FareRules]
        gtfs_group_4  = [StopTimes,Frequencies]
        gtfs_group_5  = [Transfers,Translations]

        gtfs_all_groups = [ 
            gtfs_group_1, 
            gtfs_group_2,
            gtfs_group_2b,
            gtfs_group_2c,

            gtfs_group_3, 
            gtfs_group_4,
            gtfs_group_5 ]

        _LOGGER.debug(f"debug: Before processing groups" ) 

        for gtfs_group in gtfs_all_groups:
            _LOGGER.debug(f"debug: BEGIN processing group {gtfs_group}" ) 

#            pool = multiprocessing.Pool(processes=8)
            for gtfs in gtfs_group:

                _LOGGER.debug(f" run  {gtfs}" ) 
                job_import_single_gtfs( self.db_connection, feed_filename, strip_fields , gtfs, feed_id, chunk_size, partial_commit)

#                _LOGGER.debug(f"    add  {gtfs} to pool" ) 
#                pool.apply_async (
#                    self.import_single_gtfs, 
#                    args=( self,
#                        self.db_connection,
#                        feed_filename,
#                        strip_fields ,
#                        gtfs, 
#                        feed_id, 
#                        chunk_size, 
#                        partial_commit,))

            # Start all process in the pool
#            _LOGGER.debug(f"start all processes of pool {gtfs}" ) 
#            pool.close()
                
            # wait all processes in the pool ti finish
#            pool.join()
#            _LOGGER.debug(f"all processes of pool {gtfs} finished " ) 

            # end of group
        _LOGGER.debug(f"debug: ALL GROUPS HAVE BEEN PROCESSED!!" ) 


        return



        # load many to many relationships
        if Translation in gtfs_tables:
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

        return


#############################################################
#############################################################
#############################################################

def job_import_single_gtfs(db_connection, feed_filename, strip_fields, gtfs_class, feed_id, chunk_size, partial_commit):
    local_schedule = Schedule (db_connection)
    local_schedule.import_single_gtfs(feed_filename, strip_fields, gtfs_class, feed_id, chunk_size=5000, partial_commit=True)

    return


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








#############################################################
#############################################################
#############################################################

def upload_zip_to_db (db_conn_str , filename  ):

    if 1 == 2 :
            # in foregroud to have debug... but ill fail with time-out :-(
            sched = Schedule ( db_conn_str)
            sched.append_feed (
                feed_filename = filename, 
                strip_fields=False,
                chunk_size=1000,
                agency_id_override=None,
                partial_commit=True)
    else:
        if os.fork() != 0:    
            return
        else:
            sched = Schedule ( db_conn_str)
            sched.append_feed (
                feed_filename = filename, 
                strip_fields=False,
                chunk_size=10000,
                agency_id_override=None,
                partial_commit=True)


    return
