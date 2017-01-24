
import luigi
import psycopg2
from time import sleep
from random import randint


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
        3. test for completeness (an alternative to standard output method)
        '''


class ETLTask(luigi.Task):
    
    db_host = luigi.Parameter(default='localhost')
    db_port = luigi.Parameter(default='5432')
    db_user = luigi.Parameter()
    db_pass = luigi.Parameter()
    db_name = luigi.Parameter()
    
    def get_conn():
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
            cur.execute(transform_sql)
        
        conn.commit()
        conn.close()


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
            cur.execute(load_sql)
        
        conn.commit()
        conn.close()


class DemographicTransform(TransformTask):
    
    transform_view = 'demographic_transform'
    transform_file_path = '/Users/mprittie/Vagrant/luigi/PCORNetLoader/sql/demographic_transform.sql'
        
    def complete(self):
        conn = self.get_conn()
        cur = conn.cursor()
        
        # Check that view is populated (low-bar test for completeness)
        cur.execute('SELECT * FROM %s;' % self.transform_view)
        if cur.rowcount > 0:
            return True
        else:
            return False
        
        conn.commit()
        conn.close()


class DemographicLoad(LoadTask):
    
    destination_table = 'demographic'
    load_file_path = '/Users/mprittie/Vagrant/luigi/PCORNetLoader/sql/demographic_load.sql'
    
    def requires(self):
        return DemographicTransform(
            self.db_host, self.db_port, self.db_user, self.db_pass, self.db_name
        )
    
    def complete(self):
        conn = self.get_conn()
        cur = conn.cursor()
        
        # Check that rows have been inserted (low-bar test for completeness)
        cur.execute('SELECT * FROM %s;' % self.destination_table)
        if cur.rowcount > 0:
            return True
        else:
            return False
        
        conn.commit()
        conn.close()



# To be continued ...




class EncounterTransform(TransformTask):
    
    transform_view = 'encounter_transform'
    transform_file_path = '/Users/mprittie/Vagrant/luigi/PCORNetLoader/sql/encounter_transform.sql'
    
    def complete(self):
        conn = self.get_conn()
        cur = conn.cursor()
        
        # Check that view exists
        # Check that view is populated (low-bar test for completeness)
        cur.execute('SELECT * FROM %s;' % self.transform_view)
        if cur.rowcount > 0:
            return True
        else:
            return False
        
        conn.commit()
        conn.close()


class EncounterLoad(LoadTask):
    
    destination_table = 'encounter'
    load_file_path = '/Users/mprittie/Vagrant/luigi/PCORNetLoader/sql/encounter_load.sql'
    
    def requires(self):
        return EncounterTransform(
            self.db_host, self.db_port, self.db_user, self.db_pass, self.db_name
        ),
        DemographicLoad(
            self.db_host, self.db_port, self.db_user, self.db_pass, self.db_name
        )

    def complete(self):
        conn = self.get_conn()
        cur = conn.cursor()
        
        # Check that rows have been inserted (low-bar test for completeness)
        cur.execute('SELECT * FROM %s;' % self.destination_table)
        if cur.rowcount > 0:
            return True
        else:
            return False
        
        conn.commit()
        conn.close()
