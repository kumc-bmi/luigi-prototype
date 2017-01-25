
import luigi
import psycopg2
from time import sleep
from random import randint


class TaskTemplate(luigi.Task):
    
    def requires(self):
        '''
        1. Needs the stuff
        '''
    
    def run(self):
        '''
        2. Do the stuff
        '''
    
    def complete(self):
        '''
        3. test for completeness (an alternative to the standard output method)
        '''


class ETLTask(luigi.Task):
    
    db_host = luigi.Parameter()
    db_port = luigi.Parameter()
    db_user = luigi.Parameter()
    db_pass = luigi.Parameter()
    db_name = luigi.Parameter()
    
    def get_conn(self):
        conn_dsn = 'host=%s port=%s user=%s password=%s dbname=%s' % (
            self.db_host, self.db_port, self.db_user, self.db_pass, self.db_name
        )
        return psycopg2.connect(conn_dsn)


class TransformTask(ETLTask):
    '''
    Abstract transform Task
    '''
    
    def run(self):
        conn = self.get_conn()
        cur = conn.cursor()
        
        # Create transform view
        with open(self.transform_file_path, 'r') as transform_sql:
            cur.execute(transform_sql.read())
        
        conn.commit()
        conn.close()
    
    def complete(self):
        conn = self.get_conn()
        cur = conn.cursor()
        
        # Check that view is populated (low-bar test for completeness)
        completed = False
        try:
            cur.execute('SELECT * FROM %s;' % self.transform_view)
            if cur.rowcount > 0:
                completed = True
        except:
            pass  # Don't really do this!
        
        conn.commit()
        conn.close()
        
        return completed


class LoadTask(ETLTask):
    '''
    Abstract load task
    '''
    
    def run(self):
        # Make database connection
        conn = self.get_conn()
        cur = conn.cursor()
                
        # Insert into load table
        with open(self.load_file_path, 'r') as load_sql:
            cur.execute(load_sql.read())
        
        conn.commit()
        conn.close()
    
    def complete(self):
        conn = self.get_conn()
        cur = conn.cursor()
        
        # Check that rows have been inserted (low-bar test for completeness)
        completed = False
        try:
            cur.execute('SELECT * FROM %s;' % self.destination_table)
            if cur.rowcount > 0:
                completed = True
        except:
            pass  # Don't really do this!
        
        conn.commit()
        conn.close()
        
        return completed


class DemographicTransform(TransformTask):
    
    transform_view = 'demographic_transform'
    transform_file_path = '/vagrant/PCORNetLoader/sql/demographic_transform.sql'


class DemographicLoad(LoadTask):
    
    destination_table = 'demographic'
    load_file_path = '/vagrant/PCORNetLoader/sql/demographic_load.sql'
    
    def requires(self):
        return DemographicTransform(
            self.db_host, self.db_port, self.db_user, self.db_pass, self.db_name
        )


class EncounterTransform(TransformTask):
    
    transform_view = 'encounter_transform'
    transform_file_path = '/vagrant/PCORNetLoader/sql/encounter_transform.sql'


class EncounterLoad(LoadTask):
    
    destination_table = 'encounter'
    load_file_path = '/vagrant/PCORNetLoader/sql/encounter_load.sql'
    
    def requires(self):
        params = {
            'db_host': self.db_host,
            'db_port': self.db_port,
            'db_user': self.db_user,
            'db_pass': self.db_pass,
            'db_name': self.db_name
        }
        
        return [
            DemographicLoad(**params),
            EncounterTransform(**params)
        ]


class PCORnetETL(luigi.WrapperTask):
    
    db_host = luigi.Parameter(default='localhost')
    db_port = luigi.Parameter(default='5432')
    db_user = luigi.Parameter()
    db_pass = luigi.Parameter()
    db_name = luigi.Parameter()
    
    def requires(self):
        yield EncounterLoad(
            self.db_host, self.db_port, self.db_user, self.db_pass, self.db_name
        )