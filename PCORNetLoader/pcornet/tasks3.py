
import luigi
import psycopg2


class SQLTask(luigi.Task):
    
    _db = luigi.DictParameter()
    _sql = luigi.Parameter(default='')
    
    def get_conn(self):
        conn_dsn = 'host=%s port=%s user=%s password=%s dbname=%s' % (
            self._db['host'],
            self._db['port'],
            self._db['user'],
            self._db['pass'],
            self._db['name']
        )
        return psycopg2.connect(conn_dsn)
    
    def run(self):
        conn = self.get_conn()
        cur = conn.cursor()
        
        cur.execute(self._sql)
        
        conn.commit()
        conn.close()


class StucturedSQLTask(SQLTask):
    
    _path = luigi.Parameter()
    _source = luigi.Parameter()
    _aspect = luigi.Parameter()
    _type = luigi.Parameter()
    
    def run(self):
        sql_file_path = '%s/%s/%s_%s.sql' % (
            self._path,
            self._source,
            self._aspect,
            self._type
        )
        
        with open(sql_file_path, 'r') as sql_file:
            self._sql = sql_file.read()
        
        super(StucturedSQLTask, self).run()


class SQLTransformTask(StucturedSQLTask):
    
    _type = luigi.Parameter(default='transform')
    
    def complete(self):
        conn = self.get_conn()
        cur = conn.cursor()
        
        # Check that view is populated (low-bar test for completeness)
        completed = False
        try:
            cur.execute('SELECT * FROM %s_transform;' % self._aspect)
            if cur.rowcount > 0:
                completed = True
        except:
            pass  # Don't really do this!
        
        conn.commit()
        conn.close()
        
        return completed


class SQLLoadTask(StucturedSQLTask):
    
    _type = luigi.Parameter(default='load')
    
    def complete(self):
        conn = self.get_conn()
        cur = conn.cursor()
        
        # Check that view is populated (low-bar test for completeness)
        completed = False
        try:
            cur.execute('SELECT * FROM %s;' % self._aspect)
            if cur.rowcount > 0:
                completed = True
        except:
            pass  # Don't really do this!
        
        conn.commit()
        conn.close()
        
        return completed


class DemographicTransform(SQLTransformTask):
    pass


class DemographicLoad(SQLLoadTask):
    
    _aspect = luigi.Parameter(default='demographic')
    _source = luigi.Parameter(default='heron')
    
    def requires(self):
        yield DemographicTransform(
            _aspect=self._aspect,
            _source=self._source,
            _db=self._db,
            _path=self._path
        )


class EncounterTransform(SQLTransformTask):
    pass


class EncounterLoad(SQLLoadTask):
    
    _aspect = luigi.Parameter(default='encounter')
    _source = luigi.Parameter(default='heron')
    
    def requires(self):
        yield DemographicLoad(_db=self._db, _path=self._path)
        yield EncounterTransform(
            _aspect=self._aspect,
            _source=self._source,
            _db=self._db,
            _path=self._path
        )


class PCORnetETL(luigi.WrapperTask):
    
    db_host = luigi.Parameter(default='localhost')
    db_port = luigi.Parameter(default='5432')
    db_user = luigi.Parameter()
    db_pass = luigi.Parameter()
    db_name = luigi.Parameter()
    sql_path = luigi.Parameter()
    
    def requires(self):
        db = {
            'host':self.db_host,
            'port':self.db_port,
            'user':self.db_user,
            'pass':self.db_pass,
            'name':self.db_name
        }
        
        yield EncounterLoad(_db=db, _path=self.sql_path)