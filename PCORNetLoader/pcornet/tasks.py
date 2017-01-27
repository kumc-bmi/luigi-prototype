import luigi
from time import sleep
from random import randint

SLEEP_INTERVAL = 5


class Demographic(luigi.Task):
        
    def run(self):
        with self.output().open('w') as outfile:
            outfile.write('')
        sleep(randint(1, 5) * SLEEP_INTERVAL)
    
    def output(self):
        return luigi.LocalTarget('/vagrant/data/demographic.txt')


class Encounter(luigi.Task):
    
    def requires(self):
        return Demographic()
        
    def run(self):
        with self.output().open('w') as outfile:
            outfile.write('')
        sleep(randint(1, 5) * SLEEP_INTERVAL)
    
    def output(self):
        return luigi.LocalTarget('/vagrant/data/encounter.txt')


class Diagnosis(luigi.Task):
    
    def requires(self):
        return Demographic(), Encounter()
        
    def run(self):
        with self.output().open('w') as outfile:
            outfile.write('')
        sleep(randint(1, 5) * SLEEP_INTERVAL)
    
    def output(self):
        return luigi.LocalTarget('/vagrant/data/diagnosis.txt')


class Condition(luigi.Task):
    
    def requires(self):
        return Demographic(), Encounter()
        
    def run(self):
        with self.output().open('w') as outfile:
            outfile.write('')
        sleep(randint(1, 5) * SLEEP_INTERVAL)
    
    def output(self):
        return luigi.LocalTarget('/vagrant/data/condition.txt')


class Procedure(luigi.Task):
    
    def requires(self):
        return Demographic(), Encounter()
        
    def run(self):
        with self.output().open('w') as outfile:
            outfile.write('')
        sleep(randint(1, 5) * SLEEP_INTERVAL)
    
    def output(self):
        return luigi.LocalTarget('/vagrant/data/procedure.txt')


class Vital(luigi.Task):
    
    def requires(self):
        return Demographic(), Encounter()
        
    def run(self):
        with self.output().open('w') as outfile:
            outfile.write('')
        sleep(randint(1, 5) * SLEEP_INTERVAL)
    
    def output(self):
        return luigi.LocalTarget('/vagrant/data/vital.txt')


class Enrollment(luigi.Task):
    
    def requires(self):
        return Demographic()
        
    def run(self):
        with self.output().open('w') as outfile:
            outfile.write('')
        sleep(randint(1, 5) * SLEEP_INTERVAL)
    
    def output(self):
        return luigi.LocalTarget('/vagrant/data/enrollment.txt')


class LabResultCM(luigi.Task):
    
    def requires(self):
        return Demographic(), Encounter()
        
    def run(self):
        with self.output().open('w') as outfile:
            outfile.write('')
        sleep(randint(1, 5) * SLEEP_INTERVAL)
    
    def output(self):
        return luigi.LocalTarget('/vagrant/data/labresultcm.txt')


class Prescribing(luigi.Task):
    
    def requires(self):
        return Demographic(), Encounter()
        
    def run(self):
        with self.output().open('w') as outfile:
            outfile.write('')
        sleep(randint(1, 5) * SLEEP_INTERVAL)
    
    def output(self):
        return luigi.LocalTarget('/vagrant/data/prescribing.txt')


class Dispensing(luigi.Task):
    
    def requires(self):
        return Demographic(), Prescribing()
        
    def run(self):
        with self.output().open('w') as outfile:
            outfile.write('')
        sleep(randint(1, 5) * SLEEP_INTERVAL)
    
    def output(self):
        return luigi.LocalTarget('/vagrant/data/dispensing.txt')


class Death(luigi.Task):
    
    def requires(self):
        return Demographic()
        
    def run(self):
        with self.output().open('w') as outfile:
            outfile.write('')
        sleep(randint(1, 5) * SLEEP_INTERVAL)
    
    def output(self):
        return luigi.LocalTarget('/vagrant/data/death.txt')


class Harvest(luigi.Task):
    
    def requires(self):
        return (
            Demographic(), Encounter(), Diagnosis(), Condition(), Procedure(),
            Vital(), Enrollment(), LabResultCM(), Prescribing(), Dispensing(),
            Death()
        )
        
    def run(self):
        with self.output().open('w') as outfile:
            outfile.write('')
        sleep(randint(1, 5) * SLEEP_INTERVAL)
    
    def output(self):
        return luigi.LocalTarget('/vagrant/data/harvest.txt')


class PCORnetETL(luigi.WrapperTask):
    
    def requires(self):
        # yield Demographic()
        # yield Encounter()
        # yield Diagnosis()
        # yield Condition()
        # yield Procedure()
        # yield Vital()
        # yield Enrollment()
        # yield LabResultCM()
        # yield Prescribing()
        # yield Dispensing()
        # yield Death()
        yield Harvest()
    
