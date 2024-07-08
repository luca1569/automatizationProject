import json
import logging
from job import Job

class Scheduler:
    def __init__(self, name):
        self.name = name
        self.jobs = []
        self.logger = logging.getLogger(f"Scheduler_{self.name}")
        self.setup_logging()

    def setup_logging(self):
        handler = logging.FileHandler(f"{self.name}.log")
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        self.logger.addHandler(handler)
        self.logger.setLevel(logging.INFO)

    def add_job(self, job_params):
        job = Job(job_params)
        self.jobs.append(job)
        self.logger.info(f"Added job: {job.NomeJob}")

    def run_jobs(self):
        for job in self.jobs:
            self.logger.info(f"Running job: {job.NomeJob}")
            job.run()
            self.logger.info(f"Completed job: {job.NomeJob}")
