
import luigi
from time import sleep
from random import randint
from psycopg2


class ETLTemplate(luigi.Task):

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
        3. test for completeness?
        '''


class TransformTask(luigi.Task):
    '''
    Abstract transform Task
    '''
    
    db_host = luigi.Parameter(default='localhost')
    db_port = luigi.Parameter(default='5432')
    db_user = luigi.Parameter()
    db_pass = luigi.Parameter()
    db_name = luigi.Parameter()
    
    def get_conn():
        conn_dsn = 'host=%s port=%s user=%s password=%s dbname=%s' % (
            self.db_host, self.db_port, self.db_user, self.db_pass, self.db_name
        )
        return conn = psycopg2.connect(conn_dsn)


class LoadTask(luigi.Task):
    '''
    Abstract load task
    '''
    
    db_host = luigi.Parameter(default='localhost')
    db_port = luigi.Parameter(default='5432')
    db_user = luigi.Parameter()
    db_pass = luigi.Parameter()
    db_name = luigi.Parameter()
    
    def get_conn():
        conn_dsn = 'host=%s port=%s user=%s password=%s dbname=%s' % (
            self.db_host, self.db_port, self.db_user, self.db_pass, self.db_name
        )
        return conn = psycopg2.connect(conn_dsn)


class DemographicTransform(TransformTask):
    
    transform_file_path = '/Users/mprittie/Vagrant/luigi/PCORNetLoader/sql/demographic_transform.sql'
    
    def run(self):
        conn = self.get_conn()
        cur = conn.cursor()
        
        # Create transform view
        with open(self.transform_file_path, 'r') as transform_sql:
            cur.execute(transform_sql)
        
    def complete(self):
        conn = self.get_conn()
        cur = conn.cursor()
        
        # Check that view exists


class DemographicLoad(LoadTask):
    
    load_file_path = '/Users/mprittie/Vagrant/luigi/PCORNetLoader/sql/demographic_load.sql'
    
    def requires(self):
        return DemographicTransform(
            self.db_host, self.db_port, self.db_user, self.db_pass, self.db_name
        )
    
    def run(self):
        # Make database connection
        conn = self.get_conn()
        cur = conn.cursor()
                
        # Insert into load table
        with open(load_file_path, 'r') as load_sql:
            cur.execute(load_sql)
    
    def complete(self):
        conn = self.get_conn()
        cur = conn.cursor()
        
        # Check that rows have been inserted




# To be continued ...




class EncounterTransform(TransformTask):
    
    transform_file_path = '/Users/mprittie/Vagrant/luigi/PCORNetLoader/sql/encounter_transform.sql'
    
    def run(self):
        conn = self.get_conn()
        cur = conn.cursor()
        
        # Create transform view
        with open(transform_file_path, 'r') as transform_sql:
            cur.execute(transform_sql)
        
    def complete(self):
        conn = self.get_conn()
        cur = conn.cursor()
        
        # Check that view exists
        


class EncounterLoad(LoadTask):
    
    load_file_path = '/Users/mprittie/Vagrant/luigi/PCORNetLoader/sql/demographic_load.sql'
    
    def requires(self):
        return DemographicLoad(
            self.db_host, self.db_port, self.db_user, self.db_pass, self.db_name
        ),
        EncounterTransform(
            self.db_host, self.db_port, self.db_user, self.db_pass, self.db_name
        )

    def run(self):
        # Make database connection
        conn = self.get_conn()
        cur = conn.cursor()
                
        # Insert into load table
        with open(load_file_path, 'r') as load_sql:
            cur.execute(load_sql)
    
    def complete(self):
        conn = self.get_conn()
        cur = conn.cursor()
        
        # Check that rows have been inserted
        