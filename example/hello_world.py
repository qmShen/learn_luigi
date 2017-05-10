"""
You can run this example like this:
    .. code:: console
            $ luigi --module examples.hello_world examples.HelloWorldTask --local-scheduler
If that does not work, see :ref:`CommandLine`.
"""
import luigi
import mongodb
import time
from pymongo import MongoClient
class HelloWorldTask(luigi.Task):
    task_namespace = 'examples'


    def get_target(self):
        client = MongoClient('127.0.0.1', 27017)
        # target = mongodb.MongoTarget(client, 'testWeather', 'testCollection')
        return client['testWeather']['testCollection']

    def run(self):
        handler = self.get_target()
        mins = 0
        while mins != 100:
            print ">>>>>>>>>>>>>>>>>>>>>", mins
            # Sleep for a minute
            time.sleep(2)
            # Increment the minute total
            mins += 1
            print("{task} says: Hello world!".format(task=self.__class__.__name__))

            handler.insert({'time': mins})


        # fc = self.output()[0].open('w')
        # fc.write('helloworld');
        # fc.close();
    #
    # def output(self):
    #     # targets = luigi.LocalTarget('result.txt')
    #     return self.get_target()
    #     return target

if __name__ == '__main__':
    luigi.run(['examples.HelloWorldTask', '--workers', '1'])
    # luigi.run()