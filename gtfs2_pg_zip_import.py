
import zipfile
import csv


########################################################
class gtfs2_pg_zip_import:
########################################################
    
    ####################################
    def __init__( 
        db_engine,
        zip_file_name,
        mode ) -> None:

        self.db_engine = db_engine
        self.zip_file = zip_file
        self.mode = mode
        return

    ####################################
    def create_db_structure( ) -> None:
        return
    ####################################
    def remove_feed_id_from_db( feed_id) -> None:
        return
    ####################################
    def get_zip_content( ) -> None:

        # zip file handler  
        zip = zipfile.ZipFile(zip_file_name)

        # list available files in the container
        namelist = []
        namelist = zip.namelist()
        return namelist



    ####################################
    def split_csv_line(  line) -> None:
        return


    ####################################
    def import_header_line( filename_in_zip, line) -> None:
        return

    ####################################
    def import_line( filename_in_zip, line) -> None:
            


#        file_sequence = ['feed_info.txt',
#            'agency.txt',
#            'calendar.txt',
#            'calendar_dates.txt',
#            'stops.txt',
#            'stop_times.txt',
#            'routes.txt',
#            'shapes.txt',
#            'trips.txt']

        match filename_in_zip:
            case 'feed_info.txt':
                return
            case 'agency.txt':
                return
            case 'calendar.txt':
                action-3
            case _:
                return 
        
        return

    ####################################
    def import_file( filename_in_zip) -> None:
        zip = zipfile.ZipFile(zip_file_name)

        f = zip.open(filename_in_zip)
        csvreader = csv.reader(f,delimiter=' ')
        line = csvreader.readline()
        self.import_header_line (filename_in_zip, line)
 
        line = csvreader.readline()
        while (line):
            self.import_line ( filename_in_zip, line)
            line = csvreader.readline()
        f.close()

        return

    ####################################
    def import_all_files_in_sequence( ) -> None:

        file_sequence = ['feed_info.txt',
            'agency.txt',
            'calendar.txt',
            'calendar_dates.txt',
            'stops.txt',
            'stop_times.txt',
            'routes.txt',
            'shapes.txt',
            'trips.txt']

        for file in file_senquence :
            self.import_file( file )




########################################################
class gtfs2_pg_zip_import_postgres(gtfs2_pg_zip_import):
########################################################

    ####################################
    def __init__( 
        db_engine,
        zip_file_name,
        mode ) -> None:

        super().__init__(db_engine = db_engine, zip_file_name = zip_file_name, mode = mode )
        return
