import os
import json
import pandas as pd
from sqlalchemy import create_engine
from resourcesMail2 import create_connectionMail, read_properties
from resourcesDBPanda import create_connection
import logging
from enum import Enum
import importlib.util

class TipoJobs(Enum):
    MAIL_QUERY_ALLEGATO_EXCEL = "Mail query allegato excel"
    MAIL_QUERY_SENZA_ALLEGATO = "Mail query senza allegato"
    MAIL_QUERY_SENZA_ALLEGATO_TIPO_WEB = "Mail query senza allegato tipo web"

class Job:
    def __init__(self, params):
        self.NomeJob = params.get('NomeJob')
        self.tipoJobs = TipoJobs(params.get('tipoJobs'))
        self.query_path = params.get('query_path')
        self.config_path_db = params.get('config_path_db')
        self.config_path_mail = params.get('config_path_mail')
        self.excel_dir = params.get('excel_dir')
        self.log_path = params.get('log_path')
        self.subject = params.get('subject')
        self.body = params.get('body')
        self.to_email = params.get('to_email')
        self.condizione1 = params.get('condizione1')
        self.condizione2 = params.get('condizione2')
        self.calcolo = params.get('calcolo', '')

        self.logger = logging.getLogger(f"Job_{self.NomeJob}")
        self.setup_logging()

    def setup_logging(self):
        handler = logging.FileHandler(self.log_path)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        self.logger.addHandler(handler)
        self.logger.setLevel(logging.INFO)

    def read_query(self, file_path):
        with open(file_path, 'r') as f:
            query = f.read().strip()
        return query

    def execute_calcolo(self, script_name):
        spec = importlib.util.spec_from_file_location("module.name", script_name)
        calcolo_module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(calcolo_module)
        return calcolo_module.run()  # Assumes the calcolo script has a run() function

    def run(self):
        try:
            self.logger.info(f"Running job: {self.NomeJob}")

            if not os.path.exists(self.excel_dir):
                os.makedirs(self.excel_dir)
                self.logger.info(f"Created directory: {self.excel_dir}")

            query = self.read_query(self.query_path)
            self.logger.info("Query file read successfully.")

            connection = create_connection(self.config_path_db)
            self.logger.info("Database connection established successfully.")

            df = pd.read_sql(query, connection)
            connection.close()

            if self.condizione1 == "non_empty" and df.empty:
                self.logger.info("Query returned no rows, skipping file creation and email.")
                return

            attachment_path = None

            if self.tipoJobs == TipoJobs.MAIL_QUERY_ALLEGATO_EXCEL:
                if not df.empty:
                    excel_path = os.path.join(self.excel_dir, 'query_results.xlsx')
                    df.to_excel(excel_path, index=False)
                    attachment_path = excel_path
                    self.logger.info("Query executed and results saved to Excel.")
            elif self.tipoJobs == TipoJobs.MAIL_QUERY_SENZA_ALLEGATO:
                self.body += "\n\n" + df.to_string()
                self.logger.info("Query executed and results added to email body.")
            elif self.tipoJobs == TipoJobs.MAIL_QUERY_SENZA_ALLEGATO_TIPO_WEB:
                self.body += "\n\n" + df.to_html()
                self.logger.info("Query executed and results added to email body as HTML.")

            if self.calcolo:
                calcolo_result = self.execute_calcolo(self.calcolo)
                self.body += f"\n\nCalcolo result: {calcolo_result}"
                self.logger.info("Calcolo script executed successfully.")

            properties_mail = read_properties(self.config_path_mail)
            emailReplay = properties_mail['emailReplay']
            smtpServer = properties_mail['smtpServer']

            create_connectionMail(smtpServer, emailReplay, self.subject, self.body, self.to_email, attachment_path)
            self.logger.info("Email sent successfully.")

        except Exception as e:
            self.logger.error(f"Error during job execution: {e}")
            print(f"Error during job execution: {e}")
