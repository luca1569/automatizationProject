import json
from scheduler import Scheduler

#Automation for sending emails with an attached Excel file containing the output of the query. 
if __name__ == "__main__":
    json_params = json.dumps({
        "NomeJob": "ControlloPersoneFisicheSenzaCodiceFiscale",
        "tipoJobs": "Mail query allegato excel",
        "query_path": r"D:\SPA\Python\EMERALD\Query.sql",
        "config_path_db": r"D:\Profin\webapps\EMRL\applications\resources\it\skill\skf3\batch\resources.properties",#config file with db configuartion
        "config_path_mail": r"D:\Profin\webapps\EMRL\applications\resources\it\skill\skf3\server\resources.properties",#config file with mail configuration
        "excel_dir": r"D:\SPA\Python\EMERALD",
        "log_path": r"D:\SPA\Python\EMERALD\job.log",
        "subject": "Risultati della Query",
        "body": "Test excel",
        "to_email": "U.ITSistemiProvvigionalieAmministrativi@cbuatdom.it",
        "condizione1": "non_empty",
        "condizione2": "another_condition",
        "calcolo": ""
    })

    scheduler = Scheduler("Daily Job Scheduler ControlloPersoneFisicheSenzaCodiceFiscale")
    scheduler.add_job(json.loads(json_params))
    scheduler.run_jobs()
